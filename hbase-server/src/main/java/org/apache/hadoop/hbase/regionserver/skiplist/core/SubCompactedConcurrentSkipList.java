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
import static org.apache.hadoop.hbase.regionserver.skiplist.core.AbstractCCSMap.GT;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.AbstractCCSMap.LT;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NIL_NODE_ID;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.ChunkMissingException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Sub view of CompactedConcurrentSkipList
 * not support Descending
 */
@InterfaceAudience.Private
final class SubCompactedConcurrentSkipList<K> implements ICCSList<K> {
  private final ICCSList<K> ccsl;
  private final K lo;
  /**
   * upper bound key, or null if to end
   */
  private final K hi;
  /**
   * inclusion flag for lo
   */
  private final boolean loInclusive;
  /**
   * inclusion flag for hi
   */
  private final boolean hiInclusive;

  class SubIter implements IIterCCSList {
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
    SubIter() throws CCSMapException {
      next = findFirst();
    }

    public final boolean hasNext() {
      return next != NIL_NODE_ID;
    }

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
      long nextId = ccsl.getNextNodeById(next);
      while (nextId != NIL_NODE_ID && NodeUtil
          .isNodeRemoving(SubCompactedConcurrentSkipList.this, nextId)) {
        nextId = getNextNodeById(nextId);
      }
      next = nextId;

      if (tooHigh(next)) {
        next = NIL_NODE_ID;
      }
    }

    public void remove() {
      long l = lastReturned;
      if (l == NIL_NODE_ID) {
        throw new NoSuchElementException();
      }
      // It would not be worth all of the overhead to directly
      // unlink from here. Using remove is fast enough.

      try {
        ccsl.remove(l);
      } catch (CCSMapException e) {
        throw new IllegalStateException(e);
      }
      lastReturned = NIL_NODE_ID;
    }

  }

  public SubCompactedConcurrentSkipList(ICCSList<K> ccsl, K lo, K hi, boolean loInclusive,
      boolean hiInclusive) {
    this.ccsl = ccsl;
    this.lo = lo;
    this.hi = hi;
    this.loInclusive = loInclusive;
    this.hiInclusive = hiInclusive;
  }

  @Override
  public IChunk getChunkById(long chunkId) {
    return this.ccsl.getChunkById(chunkId);
  }

  @Override
  public long findFirst() throws CCSMapException {
    return loNode();
  }

  @Override
  public long findLast() throws CCSMapException {
    return hiNode();
  }

  @Override
  public int getSize() {
    //it's too slow.
    try {
      long count = 0;
      long n = loNode();
      for (; ; ) {
        if (n == NIL_NODE_ID) {
          break;
        }

        if (tooHigh(n)) {
          break;
        }
        if (!NodeUtil.isNodeRemoving(this, n)) {
          ++count;
        }

        n = this.ccsl.getNextNodeById(n);
      }
      return (int) count;
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() {
    this.ccsl.close();
  }

  @Override
  public CompactedConcurrentSkipList.Stat getCcslStat() {
    return this.ccsl.getCcslStat();
  }

  @Override
  public IIterCCSList nodeIdIter() throws CCSMapException {
    return new SubIter();
  }

  @Override
  public long get(K key) throws CCSMapException {
    return inBounds(key) ? this.ccsl.get(key) : NIL_NODE_ID;
  }

  @Override
  public int generateRandomIndexLevel() {
    return this.ccsl.generateRandomIndexLevel();
  }

  @Override
  public long getNodeAndWriteMeta(int level, int metaLen, int dataLen) throws CCSMapException {
    return this.ccsl.getNodeAndWriteMeta(level, metaLen, dataLen);
  }

  @Override
  public long put(long newNodeId, int level) throws CCSMapException {
    checkKeyBounds(newNodeId);
    return this.ccsl.put(newNodeId, level);
  }

  @Override
  public long remove(K key) throws CCSMapException {
    return inBounds(key) ? this.ccsl.remove(key) : NIL_NODE_ID;
  }

  public long remove(long nodeId) throws CCSMapException {
    return inBounds(nodeId) ? this.ccsl.remove(nodeId) : NIL_NODE_ID;
  }

  @Override
  public INodeComparator<K> getComparator() {
    return this.ccsl.getComparator();
  }

  @Override
  public long getNextNodeByIdAndLevel(long next, int level) throws CCSMapException {
    return this.ccsl.getNextNodeByIdAndLevel(next, level);
  }

  @Override
  public long getNextNodeById(long next) throws ChunkMissingException {
    return this.ccsl.getNextNodeById(next);
  }

  @Override
  public long findNear(K key, int rel) throws CCSMapException {

    if (tooLow(key)) {
      if ((rel & LT) == 0) {
        long n = loNode();
        if (isBeforeEnd(n)) {
          return n;
        }
      }
      return NIL_NODE_ID;
    }
    if (tooHigh(key)) {
      if ((rel & LT) != 0) {
        long n = hiNode();
        if (n != NIL_NODE_ID) {
          if (inBounds(n)) {
            return n;
          }
        }
      }
      return NIL_NODE_ID;
    }

    long n = this.ccsl.findNear(key, rel);
    if (n == NIL_NODE_ID || !inBounds(n)) {
      return NIL_NODE_ID;
    }

    return n;
  }

  long hiNode() throws CCSMapException {
    long result;
    if (hi == null) {
      result = this.ccsl.findLast();
    } else if (hiInclusive) {
      result = this.ccsl.findNear(hi, LT | EQ);
    } else {
      result = this.ccsl.findNear(hi, LT);
    }

    if (result == NIL_NODE_ID) {
      return result;
    }

    if (tooLow(result)) {
      return NIL_NODE_ID;
    }
    return result;
  }

  long loNode() throws CCSMapException {
    long result;
    if (lo == null) {
      result = this.ccsl.findFirst();
    } else if (loInclusive) {
      result = this.ccsl.findNear(lo, GT | EQ);
    } else {
      result = this.ccsl.findNear(lo, GT);
    }

    if (result == NIL_NODE_ID) {
      return result;
    }

    if (tooHigh(result)) {
      return NIL_NODE_ID;
    }
    return result;
  }

  private boolean isBeforeEnd(long nodeId) throws ChunkMissingException {
    if (nodeId == NIL_NODE_ID) {
      return false;
    }
    if (hi == null) {
      return true;
    }
    int c = compareNodes(nodeId, hi);
    return !(c > 0 || (c == 0 && !hiInclusive));
  }

  @Override
  public int compareNodes(long leftNodeId, ByteBuffer right, int rightKeyOffset, int rightKeyLen)
      throws ChunkMissingException {
    return this.ccsl.compareNodes(leftNodeId, right, rightKeyOffset, rightKeyLen);
  }

  @Override
  public int compareNodes(ByteBuffer left, int leftKeyOffset, int leftKeyLen, long rightNodeId)
      throws ChunkMissingException {
    return this.ccsl.compareNodes(left, leftKeyOffset, leftKeyLen, rightNodeId);
  }

  @Override
  public int compareNodes(ByteBuffer left, int leftKeyOffset, int leftKeyLen, ByteBuffer right,
      int rightKeyOffset, int rightKeyLen) throws ChunkMissingException {
    return this.ccsl
        .compareNodes(left, leftKeyOffset, leftKeyLen, right, rightKeyOffset, rightKeyLen);
  }

  @Override
  public int compareNodes(ByteBuffer left, int leftKeyOffset, int leftKeyLen, K right)
      throws ChunkMissingException {
    return this.ccsl.compareNodes(left, leftKeyOffset, leftKeyLen, right);
  }

  @Override
  public int compareNodes(long leftNodeId, K right) throws ChunkMissingException {
    return this.ccsl.compareNodes(leftNodeId, right);
  }

  @Override
  public int compareNodes(K left, long rightNodeId) throws ChunkMissingException {
    return this.ccsl.compareNodes(left, rightNodeId);
  }

  @Override
  public int compareNodes(K left, K right) throws ChunkMissingException {
    return this.ccsl.compareNodes(left, right);
  }

  @Override
  public int getCurMaxLevel() {
    return this.ccsl.getCurMaxLevel();
  }

  private boolean inBounds(long nodeId) throws ChunkMissingException {
    return !tooLow(nodeId) && !tooHigh(nodeId);
  }

  private boolean inBounds(K key) throws ChunkMissingException {
    return !tooLow(key) && !tooHigh(key);
  }

  private void checkKeyBounds(long nodeId) throws ChunkMissingException {
    if (!inBounds(nodeId)) {
      throw new IllegalArgumentException("key out of range");
    }
  }

  private boolean tooHigh(K key) throws ChunkMissingException {
    if (hi == null) {
      return false;
    }
    int c = ccsl.compareNodes(key, hi);
    return c > 0 || (c == 0 && !hiInclusive);
  }

  private boolean tooHigh(long nodeId) throws ChunkMissingException {
    if (hi == null) {
      return false;
    }
    int c = ccsl.compareNodes(nodeId, hi);
    return c > 0 || (c == 0 && !hiInclusive);
  }

  private boolean tooLow(K key) throws ChunkMissingException {
    if (lo == null) {
      return false;
    }
    int c = ccsl.compareNodes(key, lo);
    return c < 0 || (c == 0 && !loInclusive);
  }

  private boolean tooLow(long nodeId) throws ChunkMissingException {
    if (lo == null) {
      return false;
    }
    int c = ccsl.compareNodes(nodeId, lo);
    return c < 0 || (c == 0 && !loInclusive);
  }

}
