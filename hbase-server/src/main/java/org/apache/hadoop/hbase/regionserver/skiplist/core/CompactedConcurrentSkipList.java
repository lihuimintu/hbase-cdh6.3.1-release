/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.skiplist.core;

import static org.apache.hadoop.hbase.regionserver.skiplist.core.AbstractCCSMap.EQ;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.AbstractCCSMap.LT;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.INDEX_NOT_INITIAL;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NIL_NODE_ID;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NO_INDEX;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.SKIP_LIST_HEAD_NODE;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.ChunkMissingException;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.HeapFullException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A concurrent skip list implementation with more compacted memory usage
 * It's the underlying implementation for CompactedConcurrentSkipListMap(CCSMap for short).
 * <p>
 * About concurrency：
 * 1. write Data to chunk is concurrently.
 * 2. use CAS to update the Node's meta.
 * <p>
 * About writing duplicate key ：
 * 1. remove first,then put again.
 * <p>
 * About remove:
 * 1. remove will make more fragments，CCSList do not do compaction.
 * 2. User can do compaction if needed, for example copy to new CCSList.
 * <p>
 * when the data size is bigger than chunk's ：
 * 1. create the OnHeap chunk, and not pool it.
 * <p>
 * thread-safe
 */
@InterfaceAudience.Private
final class CompactedConcurrentSkipList<K> implements ICCSList<K> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactedConcurrentSkipList.class);
  private static final Double COMPACTION_REMOVED_RATIO = 0.1;

  //one LevelIndexHeader per CCSL
  private final ILevelIndexHeader levelIndexHeader;

  //levelUpdateInd
  private final AtomicBoolean levelUpdateInd = new AtomicBoolean(false);
  private final INodeComparator<K> comparator;
  private final int useOldChunkThreshold;

  private final AtomicInteger size = new AtomicInteger(0);
  private final AtomicReference<IChunk> curChunk = new AtomicReference<>();

  private final IAllocatorHandler allocatorHandler;
  private final SchemaEnum schema;
  private final int chunkSize;

  //stat
  private final Stat ccslStat = new Stat();

  // use PriorityBlockingQueue to keep the used chunk sorting by the unused the space.
  private final BlockingQueue<IChunk> usedChunkQueue =
      new PriorityBlockingQueue<>(256, new Comparator<IChunk>() {
        @Override
        public int compare(IChunk o1, IChunk o2) {
          return Long
                   .compare((o2.getLimit() - o2.getPosition()), (o1.getLimit() - o1.getPosition()));
        }
      });

  private volatile boolean closed = false;

  private static final Random seedGenerator = new Random();
  private transient int randomSeed = seedGenerator.nextInt() | 0x0100; // ensure nonzero

  //it's not use Iterator, IIterCCSList can help to reduce the autoBoxing
  class Iter implements IIterCCSList {
    /**
     * the last node returned by next()
     */
    long lastReturned = NIL_NODE_ID;
    /**
     * the next node to return from next();
     */
    long next = NIL_NODE_ID;

    /**
     * Initializes ascending iterator for entire range.
     */
    Iter() throws ChunkMissingException {
      next = findFirst();
    }

    @Override
    public final boolean hasNext() {
      return next != NIL_NODE_ID;
    }

    @Override
    public long next() {
      long n = next;
      try {
        advance();
      } catch (ChunkMissingException e) {
        throw new IllegalStateException(e);
      }
      return n;
    }

    /**
     * Advances next to higher entry.
     */
    final void advance() throws ChunkMissingException {
      if (next == NIL_NODE_ID) {
        throw new NoSuchElementException();
      }
      lastReturned = next;
      long nextId = getNextNodeById(next);
      while (nextId != NIL_NODE_ID && NodeUtil
          .isNodeRemoving(CompactedConcurrentSkipList.this, nextId)) {
        nextId = getNextNodeById(nextId);
      }
      next = nextId;
    }

    @Override
    public void remove() {
      long l = lastReturned;
      if (l == NIL_NODE_ID) {
        throw new NoSuchElementException();
      }
      // It would not be worth all of the overhead to directly
      // unlink from here. Using remove is fast enough.

      try {
        CompactedConcurrentSkipList.this.remove(l);
      } catch (CCSMapException e) {
        throw new IllegalStateException(e);
      }
      lastReturned = NIL_NODE_ID;
    }

  }

  protected CompactedConcurrentSkipList(AllocatorHandlerRegister.AllocatorHandlerBuilder builder,
      INodeComparator<K> comparator, SchemaEnum schema) {
    this.allocatorHandler = AllocatorHandlerRegister.getAllocatorHandler();
    this.comparator = comparator;
    this.levelIndexHeader = new OnHeapIndexHeader();
    this.schema = schema;
    this.useOldChunkThreshold = builder.getUseOldChunkThreshold();
    this.chunkSize = builder.getChunkSize();
  }

  @Override
  public long getNextNodeById(long nodeId) throws ChunkMissingException {
    if (nodeId == SKIP_LIST_HEAD_NODE) {
      return this.levelIndexHeader.getLevelNextNodeId(0);
    }
    return NodeUtil.getNextNodeIdForLevel(this, nodeId, 0);
  }

  @Override
  public long getNextNodeByIdAndLevel(long nodeId, int level) throws CCSMapException {
    if (nodeId == SKIP_LIST_HEAD_NODE) {
      return this.levelIndexHeader.getLevelNextNodeId(level);
    }
    long result = NodeUtil.getNextNodeIdForLevel(this, nodeId, level);
    while (result == INDEX_NOT_INITIAL) {
      result = NodeUtil.getNextNodeIdForLevel(this, nodeId, level);
      checkState();
    }
    return result;
  }

  boolean helpCasUpdateNextNode(int level, long b, long expected, long update)
    throws ChunkMissingException {
    if (b == SKIP_LIST_HEAD_NODE) {
      return this.levelIndexHeader.casUpdateLevelNextNodeId(level, expected, update);
    }
    return NodeUtil.casUpdateNextNodeForLevel(this, level, b, expected, update);
  }

  boolean helpCheckNodeRemoving(long nodeId) throws ChunkMissingException {
    if (nodeId == SKIP_LIST_HEAD_NODE) {
      return false;
    }
    return NodeUtil.isNodeRemoving(this, nodeId);
  }

  @Override
  public long put(long newNodeId, int level) throws CCSMapException {
    IChunk chunk = getChunkById(NodeUtil.getChunkIdByNodeId(newNodeId));
    long offset = NodeUtil.getChunkOffsetByNodeId(newNodeId);
    checkState();
    //1. update the link
    long bid, nid, fid;
    int c;
    boolean replaced;
    long result = NIL_NODE_ID;
    for (bid = findPredecessor(chunk, offset, 0), nid = getNextNodeById(bid); ; ) {
      replaced = false;
      if (nid != NIL_NODE_ID) {
        fid = getNextNodeById(nid);
        if (nid != getNextNodeById(bid)) {
          nid = getNextNodeById(bid);
          continue; // inconsistent read
        }
        c = compareNodes(chunk, (int) offset, nid);
        if (c > 0) {
          bid = nid;
          nid = fid;
          continue;
        }
        if (c == 0) {
          if (newNodeId == nid) {
            throw new CCSMapException("can't put existed node");
          }
          //new node is before the old one which was removed
          if (!NodeUtil.isNodeRemoving(this, nid)) {
            replaced = true;
          }
        }
      }
      //  c < 0 || n == null; insert to before n
      ChunkUtil.setNextNodeIdForLevel(chunk, offset, 0, nid);
      if (!helpCasUpdateNextNode(0, bid, nid, newNodeId)) {
        nid = getNextNodeById(bid);
        continue; // restart if lost race to append to b
      }
      if (replaced) {
        if (NodeUtil.casSetNodeStat(this, nid, NodeStat.REMOVE)) {
          this.ccslStat.removedNodeCount.incrementAndGet();
        }
        this.ccslStat.replaceRequestCount.incrementAndGet();
        result = nid;
        //4. reclaim removed node
        //TODO reclaimNode(n);
      } else {
        size.incrementAndGet();
      }

      break;
    }
    //5. update HeadIndex if needed
    for (; ; ) {
      if (level <= levelIndexHeader.getLevel()) {
        break;
      }
      if (!levelUpdateInd.compareAndSet(false, true)) {
        continue;
      }
      try {
        levelIndexHeader.updateLevel(level);
      } finally {
        levelUpdateInd.compareAndSet(true, false);
      }
    }

    //6. update index
    if (level != NO_INDEX) {
      putIndex(level, newNodeId, chunk, offset);
    }

    if (replaced) {
      deleteNode();
    }
    return result;
  }

  void putIndex(int level, long newNodeId, IChunk chunk, long offset) throws CCSMapException {

    int currLevel = level;
    long b = findPredecessor(chunk, offset, level);
    long r = getNextNodeByIdAndLevel(b, level);

    for (; ; ) {
      if (r != NIL_NODE_ID) {
        // compare before deletion check avoids needing recheck
        int c = compareNodes(chunk, (int) offset, r);
        if (c > 0) {
          b = r;
          r = getNextNodeByIdAndLevel(b, currLevel);
          if (b == r) {
            throw new RuntimeException("b=r =" + r);
          }
          continue;
        }
      }

      ChunkUtil.setNextNodeIdForLevel(chunk, offset, currLevel, r);

      if (!helpCasUpdateNextNode(currLevel, b, r, newNodeId)) {
        r = getNextNodeByIdAndLevel(b, currLevel);
        continue;
      }
      currLevel--;
      if (currLevel == 0) {
        break;
      }

      r = getNextNodeByIdAndLevel(b, currLevel);
    }
  }

  @Override
  public long remove(long nodeId) throws CCSMapException {
    IChunk chunk = getChunkById(NodeUtil.getChunkIdByNodeId(nodeId));
    long offset = NodeUtil.getChunkOffsetByNodeId(nodeId);
    int keyOffset = ChunkUtil.getKeyOffset(chunk, offset, this.schema);
    int keyLen = ChunkUtil.getKeyLen(chunk, offset, keyOffset, this.schema);
    return remove(chunk.getByteBuffer(), keyOffset, keyLen);
  }

  public long remove(ByteBuffer keyBB, int keyOffset, int keyLen) throws CCSMapException {
    checkState();
    long b, n, f;
    int c;
    outer:
    for (; ; ) {
      for (b = findPredecessor(keyBB, keyOffset, keyLen, 0), n = getNextNodeById(b); ; ) {
        if (n == NIL_NODE_ID) {
          break outer;
        }
        f = getNextNodeById(n);
        if (n != getNextNodeById(b)) {              // inconsistent read
          break;
        }
        if (helpCheckNodeRemoving(n)) {// n is remove
          b = n;
          n = f;
          continue;
        }

        c = compareNodes(keyBB, keyOffset, keyLen, n);
        if (c < 0) {
          break outer;
        }
        if (c > 0) {
          b = n;
          n = f;
          continue;
        }

        if (!NodeUtil.casSetNodeStat(this, n, NodeStat.REMOVE)) {
          b = n;
          n = f;
          continue;
        }
        this.ccslStat.removedNodeCount.incrementAndGet();
        deleteNode();//b, n, f);
        this.size.decrementAndGet();
        //TODO reclaimNode(n);
        return n;
      }
    }
    return NIL_NODE_ID;
  }

  @Override
  public long remove(K key) throws CCSMapException {
    checkState();
    long b, n, f;
    int c;
    outer:
    for (; ; ) {
      for (b = findPredecessor(key, 0), n = getNextNodeById(b); ; ) {
        if (n == NIL_NODE_ID) {
          break outer;
        }
        f = getNextNodeById(n);
        if (n != getNextNodeById(b)) {              // inconsistent read
          break;
        }
        if (helpCheckNodeRemoving(n)) {// n is remove
          b = n;
          n = f;
          continue;
        }

        c = compareNodes(key, n);
        if (c < 0) {
          break outer;
        }
        if (c > 0) {
          b = n;
          n = f;
          continue;
        }

        if (!NodeUtil.casSetNodeStat(this, n, NodeStat.REMOVE)) {
          b = n;
          n = f;
          continue;
        }
        this.ccslStat.removedNodeCount.incrementAndGet();
        deleteNode();
        this.size.decrementAndGet();
        //TODO reclaimNode(n);
        return n;
      }
    }
    return NIL_NODE_ID;
  }

  @Override
  public long get(K key) throws CCSMapException {
    checkState();
    long b, n, f;
    int c;
    outer:
    for (; ; ) {
      for (b = findPredecessor(key, 0), n = getNextNodeById(b); ; ) {
        if (n == NIL_NODE_ID) {
          break outer;
        }
        f = getNextNodeById(n);

        if (helpCheckNodeRemoving(n)) {// n is remove
          b = n;
          n = f;
          continue;
        }
        if (n != (getNextNodeById(b))) {              // inconsistent read
          break;
        }

        c = compareNodes(key, n);
        if (c == 0) {
          return n;
        }

        if (c < 0) {
          break outer;
        }
        b = n;
        n = f;
      }
    }
    return NIL_NODE_ID;
  }

  private int compareNodes(IChunk leftChunk, int offset, long right)
      throws ChunkMissingException {

    int leftKeyOffset = ChunkUtil.getKeyOffset(leftChunk, offset, this.schema);
    int leftKeyLen = ChunkUtil.getKeyLen(leftChunk, offset, leftKeyOffset, this.schema);
    return compareNodes(leftChunk.getByteBuffer(), leftKeyOffset, leftKeyLen, right);
  }

  @Override
  public int compareNodes(long left, ByteBuffer right, int rightKeyOffset, int rightKeyLen)
      throws ChunkMissingException {

    IChunk leftChunk = this.getChunkById(NodeUtil.getChunkIdByNodeId(left));
    long baseOffset = (int) NodeUtil.getChunkOffsetByNodeId(left);
    int leftKeyOffset = ChunkUtil.getKeyOffset(leftChunk, baseOffset, this.schema);
    int leftKeyLen = ChunkUtil.getKeyLen(leftChunk, baseOffset, leftKeyOffset, this.schema);
    return compareNodes(leftChunk.getByteBuffer(), leftKeyOffset, leftKeyLen, right,
        rightKeyOffset, rightKeyLen);
  }

  @Override
  public int compareNodes(ByteBuffer left, int leftKeyOffset, int leftKeyLen, long right)
      throws ChunkMissingException {

    IChunk rightChunk = this.getChunkById(NodeUtil.getChunkIdByNodeId(right));
    long baseOffset = (int) NodeUtil.getChunkOffsetByNodeId(right);
    int rightKeyOffset = ChunkUtil.getKeyOffset(rightChunk, baseOffset, this.schema);
    int rightKeyLen = ChunkUtil.getKeyLen(rightChunk, baseOffset, rightKeyOffset, this.schema);
    return compareNodes(left, leftKeyOffset, leftKeyLen, rightChunk.getByteBuffer(),
        rightKeyOffset, rightKeyLen);
  }

  @Override
  public int compareNodes(K key, long right) throws ChunkMissingException {
    IChunk rightChunk = this.getChunkById(NodeUtil.getChunkIdByNodeId(right));
    long baseOffset = (int) NodeUtil.getChunkOffsetByNodeId(right);
    int rightKeyOffset = ChunkUtil.getKeyOffset(rightChunk, baseOffset, this.schema);
    int rightKeyLen = ChunkUtil.getKeyLen(rightChunk, baseOffset, rightKeyOffset, this.schema);
    return compareNodes(key, rightChunk.getByteBuffer(), rightKeyOffset, rightKeyLen);
  }

  @Override
  public int compareNodes(long left, K key) throws ChunkMissingException {
    IChunk leftChunk = this.getChunkById(NodeUtil.getChunkIdByNodeId(left));
    long baseOffset = (int) NodeUtil.getChunkOffsetByNodeId(left);
    int leftKeyOffset = ChunkUtil.getKeyOffset(leftChunk, baseOffset, this.schema);
    int leftKeyLen = ChunkUtil.getKeyLen(leftChunk, baseOffset, leftKeyOffset, this.schema);
    return compareNodes(leftChunk.getByteBuffer(), leftKeyOffset, leftKeyLen, key);
  }

  @Override
  public int compareNodes(ByteBuffer left, int leftKeyOffset, int leftKeyLen, ByteBuffer right,
      int rightKeyOffset, int rightKeyLen) throws ChunkMissingException {
    return this.comparator
        .compareTo(left, leftKeyOffset, leftKeyLen, right, rightKeyOffset, rightKeyLen);
  }

  public int compareNodes(K left, ByteBuffer right, int rightKeyOffset, int rightKeyLen) {
    return this.comparator.compareTo(left, right, rightKeyOffset, rightKeyLen);
  }

  @Override
  public int compareNodes(ByteBuffer left, int leftKeyOffset, int leftKeyLen, K right)
      throws ChunkMissingException {
    return this.comparator.compareTo(left, leftKeyOffset, leftKeyLen, right);
  }

  @Override
  public int compareNodes(K left, K right) throws ChunkMissingException {
    return this.comparator.compareTo(left, right);
  }

  @Override
  public int getCurMaxLevel() {
    return this.levelIndexHeader.getLevel();
  }

  private long findPredecessor(IChunk chunk, long offset, int level) throws CCSMapException {
    int leftKeyOffset = ChunkUtil.getKeyOffset(chunk, offset, this.schema);
    int leftKeyLen = ChunkUtil.getKeyLen(chunk, offset, leftKeyOffset, this.schema);
    return findPredecessor(chunk.getByteBuffer(), leftKeyOffset, leftKeyLen, level);
  }

  private long findPredecessor(ByteBuffer chunkBB, int keyOffset, int keyLen, int level)
      throws CCSMapException {
    int curLevel = this.levelIndexHeader.getLevel();
    long bid = SKIP_LIST_HEAD_NODE;
    long rid = this.levelIndexHeader.getLevelNextNodeId(curLevel);
    for (; ; ) {
      if (rid != NIL_NODE_ID) {
        if (compareNodes(chunkBB, keyOffset, keyLen, rid) > 0) {
          bid = rid;
          rid = getNextNodeByIdAndLevel(bid, curLevel);
          continue;
        }
      }
      curLevel--;
      if (curLevel <= level) {
        return bid;
      }
      rid = getNextNodeByIdAndLevel(bid, curLevel);
    }
  }

  private long findPredecessor(K key, int level) throws CCSMapException {
    int curLevel = this.levelIndexHeader.getLevel();
    long bid = SKIP_LIST_HEAD_NODE;
    long rid = this.levelIndexHeader.getLevelNextNodeId(curLevel);
    for (; ; ) {
      if (rid != NIL_NODE_ID) {
        if (compareNodes(key, rid) > 0) {
          bid = rid;
          rid = getNextNodeByIdAndLevel(bid, curLevel);
          continue;
        }
      }
      curLevel--;
      if (curLevel <= level) {
        return bid;
      }
      rid = getNextNodeByIdAndLevel(bid, curLevel);
    }
  }

  @Override
  public final long findNear(K key, int rel) throws CCSMapException {
    int curLevel = this.levelIndexHeader.getLevel();
    long bid = SKIP_LIST_HEAD_NODE;
    long rid = this.levelIndexHeader.getLevelNextNodeId(curLevel);
    long nid, fid;
    int c;
    //1. find findPredecessor but not removed
    for (; ; ) {
      if (rid != NIL_NODE_ID) {
        if (NodeUtil.isNodeRemoving(this, rid)) {
          rid = getNextNodeByIdAndLevel(rid, curLevel);
          continue;
        }
        if (compareNodes(key, rid) > 0) {
          bid = rid;
          rid = getNextNodeByIdAndLevel(bid, curLevel);
          continue;
        }
      }
      curLevel--;
      if (curLevel <= 0) {
        break;
      }
      rid = getNextNodeByIdAndLevel(bid, curLevel);
    }

    //2. find near node

    for (nid = getNextNodeById(bid); ; ) {
      if (nid == NIL_NODE_ID) {
        return (rel & LT) == 0 ? NIL_NODE_ID : (bid == SKIP_LIST_HEAD_NODE ? NIL_NODE_ID : bid);
      }
      if (NodeUtil.isNodeRemoving(this, nid)) {
        nid = getNextNodeById(nid);
        continue;
      }
      fid = getNextNodeById(nid);

      c = compareNodes(key, nid);

      if ((c == 0 && (rel & EQ) != 0) || (c < 0 && (rel & LT) == 0)) {
        return nid;
      }
      if (c <= 0 && (rel & LT) != 0) {
        return (bid == SKIP_LIST_HEAD_NODE ? NIL_NODE_ID : bid);
      }
      bid = nid;
      nid = fid;
    }
  }

  @Override
  public final long findFirst() throws ChunkMissingException {
    long firstNodeId = this.levelIndexHeader.getLevelNextNodeId(0);
    while (firstNodeId != NIL_NODE_ID) {
      if (!NodeUtil.isNodeRemoving(this, firstNodeId)) {
        return firstNodeId;
      }
      firstNodeId = getNextNodeById(firstNodeId);
    }
    return NIL_NODE_ID;
  }

  @Override
  public final long findLast() throws CCSMapException {
    int curLevel = this.levelIndexHeader.getLevel();
    long bid = SKIP_LIST_HEAD_NODE;
    long rid = this.levelIndexHeader.getLevelNextNodeId(curLevel);
    long nid;
    for (; ; ) {
      if (rid != NIL_NODE_ID) {
        if (!helpCheckNodeRemoving(rid)) {
          bid = rid;
        }
        rid = getNextNodeByIdAndLevel(rid, curLevel);
      } else if (curLevel > 1) {
        curLevel--;
        rid = getNextNodeByIdAndLevel(bid, curLevel);
      } else {
        for (nid = getNextNodeById(bid); ; ) {
          if (nid == NIL_NODE_ID) {
            return bid == SKIP_LIST_HEAD_NODE ? NIL_NODE_ID : bid;
          }
          if (NodeUtil.isNodeRemoving(this, nid)) {
            nid = getNextNodeById(nid);
            continue;
          }

          bid = nid;
          nid = getNextNodeById(nid);
        }
      }
    }
  }

  @Override
  public long getNodeAndWriteMeta(int level, int metaLen, int dataLen) throws CCSMapException {
    int nodeLen = metaLen + dataLen;
    checkState();
    if (nodeLen > chunkSize) {
      return doAllocateForHuge(level, dataLen, nodeLen);
    } else {
      return doAllocateForNormal(level, dataLen, nodeLen);
    }
  }

  /**
   * HugeChunk only use once, so no multi thread contention
   */
  private long doAllocateForHuge(int level, int dataLen, int nodeLen) throws HeapFullException {
    IChunk chunk = allocatorHandler.allocate(nodeLen);
    Preconditions.checkArgument(chunk instanceof HugeOnHeapChunk, "It's not a HugeChunk");
    long allocOffset = chunk.allocate(nodeLen);
    Preconditions.checkArgument(allocOffset == 0, "HugeChunk only can be use once");
    this.usedChunkQueue.offer(chunk);
    return ChunkUtil.newNodeOnChunk(chunk, allocOffset, level, dataLen);
  }

  private long doAllocateForNormal(int level, int dataLen, int nodeLen) throws HeapFullException {
    IChunk chunk;
    long allocOffset;
    //useOldChunk control only try old chunk once time.
    boolean useOldChunk = false;
    while (true) {
      chunk = curChunk.get();
      if (chunk == null) {
        if (!useOldChunk && this.ccslStat.chunkCount.get() > this.useOldChunkThreshold) {
          // The chunks in use may still have tail spaces, so we will check the one
          // with most un-used space (best-effort since chunk's tail space will change
          // after new allocation but re-ordering will only happen when new chunk
          // added to queue) and try to use it for best memory utilization.
          // Notes:
          // 1. We will try only one chunk even if it's not the best one with enough space
          // 2. The huge chunks won't be chosen since no more tail space there
          chunk = usedChunkQueue.peek();
          //chunk is not null
          useOldChunk = true;
        } else {
          chunk = allocatorHandler.allocate(nodeLen);
          if (!curChunk.compareAndSet(null, chunk)) {
            allocatorHandler.putbackChunk(chunk);
            continue;
          } else {
            // new chunk get success
            this.ccslStat.chunkCount.incrementAndGet();
          }
        }
      } else {
        useOldChunk = false;
      }
      // Try to allocate from this chunk
      allocOffset = chunk.allocate(nodeLen);
      if (allocOffset != -1) {
        // We succeeded - this is the common case - small alloc
        // from a big buffer
        break;
      }
      // not enough space!
      // if this chunk is not old chunk,try to retire it, and add it to usedChunkQueue
      if (!useOldChunk && curChunk.compareAndSet(chunk, null)) {
        this.usedChunkQueue.offer(chunk);
      }
    }
    return ChunkUtil.newNodeOnChunk(chunk, allocOffset, level, dataLen);
  }

  @Override
  public IChunk getChunkById(long chunkId) {
    return this.allocatorHandler.getChunkById(chunkId);
  }

  private void reclaimNode(INode node) {
    //TODO
    //need implement the reference logic first.
    // only all the reference to this node was removed, this node can be reused. #deleteNode
  }

  /**
   * when removed node access threshold, log it.
   */
  private boolean deleteNode() {
    //currently only log it.
    //if there have more remove, user maybe need do compact himself.
    if (this.ccslStat.removedNodeCount.get() >= this.size.get() * COMPACTION_REMOVED_RATIO) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("WARN! remove Node size={}; totalSize={}", this.ccslStat.removedNodeCount.get(),
          this.size.get());
      }
    }
    return true;
  }

  @Override
  public int generateRandomIndexLevel() {
    int x = randomSeed;
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    randomSeed = x;
    // test highest and lowest bits
    if ((x & 0x8001) != 0) {
      return NO_INDEX;
    }
    int level = 1;
    int curMax = levelIndexHeader.getLevel();
    x >>>= 1;
    while ((x & 1) != 0) {
      ++level;
      x >>>= 1;
      //the level only be increased by step
      if (level > curMax) {
        break;
      }
    }
    return level;
  }

  private void checkState() throws CCSMapException {
    if (this.closed) {
      throw new CCSMapException("CCSL has closed.");
    }
  }

  @Override
  public void close() {
    if (this.closed) {
      return;
    }

    synchronized (this) {
      if (this.closed) {
        return;
      }
      IChunk chunk;
      while ((chunk = usedChunkQueue.poll()) != null) {
        allocatorHandler.putbackChunk(chunk);
      }
      chunk = this.curChunk.get();
      if (chunk != null) {
        allocatorHandler.putbackChunk(chunk);
        this.curChunk.set(null);
      }
      this.closed = true;
    }
  }

  @Override
  public int getSize() {
    return size.get();
  }

  @VisibleForTesting
  ILevelIndexHeader getLevelIndexHeader() {
    return levelIndexHeader;
  }

  @VisibleForTesting
  BlockingQueue<IChunk> getUsedChunkQueue() {
    return usedChunkQueue;
  }

  @VisibleForTesting
  IChunk getCurChunk() {
    return curChunk.get();
  }

  @Override
  public IIterCCSList nodeIdIter() throws ChunkMissingException {
    return new Iter();
  }

  @Override
  public Stat getCcslStat() {
    return ccslStat;
  }

  @Override
  public INodeComparator<K> getComparator() {
    return comparator;
  }

  public static class Stat {
    private final AtomicInteger chunkCount = new AtomicInteger(0);
    private final AtomicLong cprRequestCount = new AtomicLong(0);
    private final AtomicLong putRequestCount = new AtomicLong(0);
    private final AtomicLong replaceRequestCount = new AtomicLong(0);
    private final AtomicLong removedNodeCount = new AtomicLong(0);
    private final AtomicLong removeRequestRaceFailCount = new AtomicLong(0);
    private final AtomicLong debugTimer = new AtomicLong(0);
    private final AtomicLong debugCounter = new AtomicLong(0);

    public AtomicInteger getChunkCount() {
      return this.chunkCount;
    }

    public long getCprRequestCount() {
      return cprRequestCount.get();
    }

    public long getPutRequestCount() {
      return putRequestCount.get();
    }

    public long getRemovedNodeCount() {
      return removedNodeCount.get();
    }

    public long getRemoveRequestRaceFailCount() {
      return removeRequestRaceFailCount.get();
    }

    public long getReplaceRequestCount() {
      return replaceRequestCount.get();
    }

    public long getDebugTimer() {
      return debugTimer.get();
    }

    public long getDebugCounter() {
      return debugCounter.get();
    }

    @Override
    public String toString() {
      return "TODO";
    }
  }
}
