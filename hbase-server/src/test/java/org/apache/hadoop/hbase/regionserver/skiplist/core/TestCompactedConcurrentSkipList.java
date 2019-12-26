/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NO_INDEX;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.SKIP_LIST_HEAD_NODE;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.MockBaseCCSMap.CCSMAP_TEST_BYTE_COMPARTOR;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@SuppressWarnings("PointlessBitwiseExpression")
@Category(SmallTests.class)
public class TestCompactedConcurrentSkipList {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompactedConcurrentSkipList.class);

  @Test
  public void testClose() throws Exception {
    CompactedConcurrentSkipList ccsl = newCCSList(HeapMode.offHeap, 128 * 1024, 100);

    ccsl.getNodeAndWriteMeta(0, NodeUtil.getNodeMetaLenByLevel(0), NodeUtil.getDataLen(10, 40));
    //curChunk has value
    Assert.assertEquals(1, ccsl.getCurChunk().getChunkId());

    Thread[] ts = new Thread[10];

    AtomicBoolean hasCloseException = new AtomicBoolean(false);

    for (int i = 0; i < ts.length; i++) {
      ts[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            ccsl.close();
          } catch (Exception e) {
            hasCloseException.set(true);
          }
        }
      });
    }

    for (Thread t : ts) {
      t.start();
    }

    for (Thread t : ts) {
      t.join();
    }

    Assert.assertEquals(null, ccsl.getCurChunk());
    Assert.assertFalse(hasCloseException.get());
  }

  @Test
  public void testCCSLAllocate() throws Exception {

    CompactedConcurrentSkipList<byte[]> ccsl = newCCSList(HeapMode.offHeap, 128 * 1024, 100);
    BlockingQueue<IChunk> usedChunk = ccsl.getUsedChunkQueue();

    //Huge Chunk
    long node0 =
      ccsl.getNodeAndWriteMeta(0, NodeUtil.getNodeMetaLenByLevel(0), NodeUtil.getDataLen(13, 100));
    Assert.assertEquals(1, usedChunk.size());
    Assert.assertEquals(null, ccsl.getCurChunk());

    long node1 =
      ccsl.getNodeAndWriteMeta(0, NodeUtil.getNodeMetaLenByLevel(0), NodeUtil.getDataLen(10, 40));
    Assert.assertEquals(1, usedChunk.size());
    Assert.assertEquals(1, ccsl.getCcslStat().getChunkCount().get());
    Assert.assertEquals(1, ccsl.getCurChunk().getChunkId());

    long node2 =
      ccsl.getNodeAndWriteMeta(0, NodeUtil.getNodeMetaLenByLevel(0), NodeUtil.getDataLen(11, 28));
    Assert.assertEquals(2, usedChunk.size());
    Assert.assertEquals(1, usedChunk.peek().getChunkId());
    Assert.assertEquals(2, ccsl.getCcslStat().getChunkCount().get());
    Assert.assertEquals(2, ccsl.getCurChunk().getChunkId());

    long node3 =
      ccsl.getNodeAndWriteMeta(0, NodeUtil.getNodeMetaLenByLevel(0), NodeUtil.getDataLen(12, 47));
    Assert.assertEquals(3, usedChunk.size());
    Assert.assertEquals(3, ccsl.getCurChunk().getChunkId());

    long node4 =
      ccsl.getNodeAndWriteMeta(0, NodeUtil.getNodeMetaLenByLevel(0), NodeUtil.getDataLen(13, 43));
    Assert.assertEquals(4, usedChunk.size());
    Assert.assertEquals(4, ccsl.getCurChunk().getChunkId());

    Assert.assertEquals(1312, NodeUtil.getChunkIdByNodeId(node0));
    Assert.assertEquals(1, NodeUtil.getChunkIdByNodeId(node1));
    Assert.assertEquals(2, NodeUtil.getChunkIdByNodeId(node2));
    Assert.assertEquals(3, NodeUtil.getChunkIdByNodeId(node3));
    Assert.assertEquals(4, NodeUtil.getChunkIdByNodeId(node4));

    Assert.assertEquals(4, usedChunk.size());
    IChunk chunk = usedChunk.poll();
    long last = -1;
    while (chunk != null) {
      long nowLen = (chunk.getLimit() - chunk.getPosition());
      System.out.println("id=" + chunk.getChunkId() + " ;" + nowLen);
      if (last != -1) {
        Assert.assertTrue(nowLen <= last);
      }

      if (!chunk.isPooledChunk()) {
        Assert.assertEquals(0, nowLen);
      }
      last = nowLen;

      chunk = usedChunk.poll();
    }
  }

  @Test
  public void testReplace() throws Exception {
    long testValue = 123;
    byte[] readBB = new byte[8];
    Bytes.putLong(readBB, 0, testValue);

    byte[] lessNode = new byte[8];
    Bytes.putLong(lessNode, 0, testValue - 1);

    byte[] bigNode = new byte[8];
    Bytes.putLong(bigNode, 0, testValue + 1);

    CompactedConcurrentSkipList<byte[]> ccsl = newCCSList(HeapMode.onHeap, 128 * 1024, 100);
    //ccsl is empty
    Assert.assertEquals(NIL_NODE_ID, ccsl.findFirst());
    Assert.assertEquals(NIL_NODE_ID, ccsl.findLast());
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, LT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, LT | EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, GT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, GT | EQ));

    long node1 =
      ccsl.getNodeAndWriteMeta(0, NodeUtil.getNodeMetaLenByLevel(0), NodeUtil.getDataLen(8, 8));
    ByteBuffer keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(ccsl, node1);
    ByteBufferUtils.putInt(keyvalueBB, 0, 8);
    ByteBufferUtils.putInt(keyvalueBB, Integer.BYTES, 8);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES, testValue);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES + 8, testValue);

    //only put one data to ccsl
    ccsl.put(node1, 0);
    Assert.assertEquals(1, ccsl.getSize());
    Assert.assertEquals(ccsl.findFirst(), node1);
    Assert.assertEquals(ccsl.findLast(), node1);
    Assert.assertEquals(ccsl.findNear(readBB, EQ), node1);
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, LT));
    Assert.assertEquals(ccsl.findNear(readBB, LT | EQ), node1);
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, GT));
    Assert.assertEquals(ccsl.findNear(readBB, GT | EQ), node1);

    Assert.assertEquals(ccsl.findNear(lessNode, EQ), node1);
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, LT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, LT | EQ));
    Assert.assertEquals(ccsl.findNear(lessNode, GT), node1);
    Assert.assertEquals(ccsl.findNear(lessNode, GT | EQ), node1);

    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, EQ));
    Assert.assertEquals(ccsl.findNear(bigNode, LT), node1);
    Assert.assertEquals(ccsl.findNear(bigNode, LT | EQ), node1);
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, GT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, GT | EQ));

    printStat("replace test", ccsl);

    long getNode = ccsl.get(readBB);
    Assert.assertEquals(getNode, node1);

    doTestAfterPutManySameKeyDataToCCSL(testValue, readBB, lessNode, bigNode, ccsl);

    //test after remove the key
    getNode = ccsl.get(readBB);
    long removeNOde = ccsl.remove(readBB);

    Assert.assertEquals(0, ccsl.getSize());
    Assert.assertEquals(removeNOde, getNode);

    Assert.assertEquals(NIL_NODE_ID, ccsl.findFirst());
    Assert.assertEquals(NIL_NODE_ID, ccsl.findLast());

    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, LT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, LT | EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, GT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, GT | EQ));

    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, LT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, LT | EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, GT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, GT | EQ));

    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, LT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, LT | EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, GT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, GT | EQ));

    removeNOde = ccsl.remove(readBB);
    Assert.assertEquals(NIL_NODE_ID, removeNOde);
    printStat("replace test", ccsl);
  }

  private void doTestAfterPutManySameKeyDataToCCSL(final long testValue, byte[] readBB,
                                                    byte[] lessNode, byte[] bigNode,
                                                    final CompactedConcurrentSkipList<byte[]> ccsl)
    throws InterruptedException, CCSMapException {

    final List<Long> totalExpectedValue = doPutSameKeyToCCSLReturnAllValueList(testValue, ccsl);

    ILevelIndexHeader index = ccsl.getLevelIndexHeader();
    long scanNode = index.getLevelNextNodeId(0);

    boolean first1 = true;
    long firstNodeId = -999;
    List<Long> lastTotalValue1 = new ArrayList<>();
    List<Long> lastTotalNodeid = new ArrayList<>();
    IIterCCSList iter = ccsl.nodeIdIter();
    while (iter.hasNext()) {
      long iterId = iter.next();
      ByteBuffer bb = NodeUtil.duplicateNodeDataByteBuffer(ccsl, iterId);
      long currentKey = ByteBufferUtils.toLong(bb, 8);
      long currentValue = ByteBufferUtils.toLong(bb, 16);
      lastTotalValue1.add(currentValue);
      System.out.println(
        iterId + " : " + NodeUtil.isNodeRemoving(ccsl, iterId) + " ;key=" + currentKey + " ,value="
          + currentValue);
      //The same key data, the first one is valid

      if (first1) {
        Assert.assertFalse(NodeUtil.isNodeRemoving(ccsl, iterId));
        first1 = false;
        firstNodeId = iterId;
      } else {
        Assert.assertTrue(NodeUtil.isNodeRemoving(ccsl, iterId));
      }
      lastTotalNodeid.add(iterId);
    }

    Assert.assertEquals(1, ccsl.getSize());
    Assert.assertEquals(100, ccsl.getCcslStat().getRemovedNodeCount());
    Assert.assertEquals(100, ccsl.getCcslStat().getReplaceRequestCount());
    Assert.assertEquals(firstNodeId, ccsl.findFirst());
    Assert.assertEquals(firstNodeId, ccsl.findLast());

    Assert.assertEquals(firstNodeId, ccsl.findNear(readBB, EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, LT));
    Assert.assertEquals(firstNodeId, ccsl.findNear(readBB, LT | EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, GT));
    Assert.assertEquals(firstNodeId, ccsl.findNear(readBB, GT | EQ));

    Assert.assertEquals(firstNodeId, ccsl.findNear(lessNode, EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, LT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, LT | EQ));
    Assert.assertEquals(firstNodeId, ccsl.findNear(lessNode, GT));
    Assert.assertEquals(firstNodeId, ccsl.findNear(lessNode, GT | EQ));

    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, EQ));
    Assert.assertEquals(firstNodeId, ccsl.findNear(bigNode, LT));
    Assert.assertEquals(firstNodeId, ccsl.findNear(bigNode, LT | EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, GT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, GT | EQ));

    Assert.assertEquals(1, lastTotalValue1.size());
    Assert.assertEquals(1, lastTotalNodeid.size());
    Assert.assertFalse(NodeUtil.isNodeRemoving(ccsl, lastTotalNodeid.get(0)));

    boolean first = true;
    firstNodeId = -999;
    List<Long> totalValue = new ArrayList<>();
    while (scanNode != NIL_NODE_ID) {
      ByteBuffer bb = NodeUtil.duplicateNodeDataByteBuffer(ccsl, scanNode);
      long currentKey = ByteBufferUtils.toLong(bb, 8);
      long currentValue = ByteBufferUtils.toLong(bb, 16);
      totalValue.add(currentValue);
      System.out.println(
        scanNode + " : " + NodeUtil.isNodeRemoving(ccsl, scanNode) + " ;key=" + currentKey
          + " ,value=" + currentValue);

      if (first) {
        Assert.assertFalse(NodeUtil.isNodeRemoving(ccsl, scanNode));
        first = false;
        firstNodeId = scanNode;
      } else {
        Assert.assertTrue(NodeUtil.isNodeRemoving(ccsl, scanNode));
      }

      scanNode = NodeUtil.getNextNodeIdForLevel(ccsl, scanNode, 0);

    }

    Assert.assertEquals(1, ccsl.getSize());
    Assert.assertEquals(100, ccsl.getCcslStat().getRemovedNodeCount());
    Assert.assertEquals(100, ccsl.getCcslStat().getReplaceRequestCount());
    Assert.assertEquals(firstNodeId, ccsl.findFirst());
    Assert.assertEquals(firstNodeId, ccsl.findLast());

    Assert.assertEquals(firstNodeId, ccsl.findNear(readBB, EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, LT));
    Assert.assertEquals(firstNodeId, ccsl.findNear(readBB, LT | EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(readBB, GT));
    Assert.assertEquals(firstNodeId, ccsl.findNear(readBB, GT | EQ));

    Assert.assertEquals(firstNodeId, ccsl.findNear(lessNode, EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, LT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(lessNode, LT | EQ));
    Assert.assertEquals(firstNodeId, ccsl.findNear(lessNode, GT));
    Assert.assertEquals(firstNodeId, ccsl.findNear(lessNode, GT | EQ));

    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, EQ));
    Assert.assertEquals(firstNodeId, ccsl.findNear(bigNode, LT));
    Assert.assertEquals(firstNodeId, ccsl.findNear(bigNode, LT | EQ));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, GT));
    Assert.assertEquals(NIL_NODE_ID, ccsl.findNear(bigNode, GT | EQ));

    Collections.sort(totalExpectedValue);
    Collections.sort(totalValue);
    Assert.assertArrayEquals(totalExpectedValue.toArray(), totalValue.toArray());

    printStat("replace test", ccsl);
  }

  private List<Long> doPutSameKeyToCCSLReturnAllValueList(
    final long testValue, final CompactedConcurrentSkipList<byte[]> ccsl)
    throws InterruptedException {
    // replica put same key , but value is different
    Thread[] threads = new Thread[100];
    final List<Long> totalExpectedValue = new ArrayList<>();
    totalExpectedValue.add(testValue);
    AtomicBoolean hasException = new AtomicBoolean(false);
    for (int i = 0; i < threads.length; i++) {
      final int id = i + 1;
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            long node = ccsl.getNodeAndWriteMeta(0, NodeUtil.getNodeMetaLenByLevel(0),
              NodeUtil.getDataLen(8, 8));
            ByteBuffer keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(ccsl, node);
            ByteBufferUtils.putInt(keyvalueBB, 0, 8);
            ByteBufferUtils.putInt(keyvalueBB, Integer.BYTES, 8);
            ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES, testValue);
            ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES + 8, testValue + id);

            ccsl.put(node, 0);
            synchronized (totalExpectedValue) {
              totalExpectedValue.add(testValue + id);
            }
          } catch (Exception e) {
            e.printStackTrace();
            hasException.set(true);
          }
        }
      });
    }

    for (Thread th : threads) {
      th.start();
    }

    for (Thread th : threads) {
      th.join();
    }

    Assert.assertFalse(hasException.get());
    Assert.assertEquals(1, ccsl.getSize());
    return totalExpectedValue;
  }

  @Test
  public void testFindNear() throws Exception {
    CompactedConcurrentSkipList<byte[]> ccsl = newCCSList(HeapMode.onHeap, 128 * 1024, 100);

    boolean first = true;
    long firstNodeId = -999;
    long lastNodeId = -999;
    for (int i = 0; i < 100; i++) {
      int level = ccsl.generateRandomIndexLevel();
      long node = ccsl.getNodeAndWriteMeta(level, NodeUtil.getNodeMetaLenByLevel(level),
        NodeUtil.getDataLen(8, 8));
      ByteBuffer keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(ccsl, node);
      ByteBufferUtils.putInt(keyvalueBB, 0, 8);
      ByteBufferUtils.putInt(keyvalueBB, Integer.BYTES, 8);
      ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES, i * 2);
      ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES + 8, i);

      ccsl.put(node, level);
      if (first) {
        firstNodeId = node;
        first = false;
      } else {
        lastNodeId = node;
      }
    }

    Assert.assertEquals(100, ccsl.getSize());
    Assert.assertEquals(firstNodeId, ccsl.findFirst());
    Assert.assertEquals(lastNodeId, ccsl.findLast());

    for (int i = 99; i >= 0; i--) {
      byte[] existedBB = new byte[8];
      Bytes.putLong(existedBB, 0, i * 2);

      Assert.assertEquals(i * 2, readKey(ccsl.findNear(existedBB, EQ), ccsl));
      if (i == 0) {
        Assert.assertEquals(NIL_NODE_ID, readKey(ccsl.findNear(existedBB, LT), ccsl));
      } else {
        Assert.assertEquals(i * 2 - 2, readKey(ccsl.findNear(existedBB, LT), ccsl));
      }
      Assert.assertEquals(i * 2, readKey(ccsl.findNear(existedBB, LT | EQ), ccsl));
      if (i == 99) {
        Assert.assertEquals(NIL_NODE_ID, readKey(ccsl.findNear(existedBB, GT), ccsl));
      } else {
        Assert.assertEquals(i * 2 + 2, readKey(ccsl.findNear(existedBB, GT), ccsl));
      }
      Assert.assertEquals(i * 2, readKey(ccsl.findNear(existedBB, GT | EQ), ccsl));

      byte[] noExistedBB = new byte[8];
      Bytes.putLong(noExistedBB, 0, i * 2 + 1);

      if (i == 99) {
        Assert.assertEquals(NIL_NODE_ID, readKey(ccsl.findNear(noExistedBB, EQ), ccsl));
      } else {
        Assert.assertEquals(i * 2 + 2, readKey(ccsl.findNear(noExistedBB, EQ), ccsl));
      }

      Assert.assertEquals(i * 2, readKey(ccsl.findNear(noExistedBB, LT), ccsl));
      Assert.assertEquals(i * 2, readKey(ccsl.findNear(noExistedBB, LT | EQ), ccsl));

      if (i == 99) {
        Assert.assertEquals(NIL_NODE_ID, readKey(ccsl.findNear(noExistedBB, GT), ccsl));
        Assert.assertEquals(NIL_NODE_ID, readKey(ccsl.findNear(noExistedBB, GT | EQ), ccsl));
      } else {
        Assert.assertEquals(i * 2 + 2, readKey(ccsl.findNear(noExistedBB, GT), ccsl));
        Assert.assertEquals(i * 2 + 2, readKey(ccsl.findNear(noExistedBB, GT | EQ), ccsl));
      }
    }
  }

  public static long readKey(long nodeid, IChunkVisible ccsl) {
    if (nodeid == NIL_NODE_ID) {
      return NIL_NODE_ID;
    }
    ByteBuffer keyvaluebb = NodeUtil.duplicateNodeDataByteBuffer(ccsl, nodeid);
    return ByteBufferUtils.toLong(keyvaluebb, 8);
  }

  @Test
  public void testLevel() throws Exception {

    AtomicInteger idGenerator = new AtomicInteger(0);
    CompactedConcurrentSkipList<byte[]> ccsl = newCCSList(HeapMode.onHeap, 128 * 1024, 100);
    Assert.assertEquals(NIL_NODE_ID, ccsl.findFirst());
    Assert.assertEquals(NIL_NODE_ID, ccsl.findLast());

    long testValue = 123;
    List<Long> totalNodeId = new ArrayList<>();
    Map<Integer, List<Long>> totalLevelNodeIdMaps = new HashMap<>();

    for (int m = 1; m <= 10; m++) {
      totalLevelNodeIdMaps.put(m, new ArrayList<>());
    }

    long node1 =
      ccsl.getNodeAndWriteMeta(1, NodeUtil.getNodeMetaLenByLevel(1), NodeUtil.getDataLen(8, 8));
    totalNodeId.add(node1);
    copyDataToNode(ccsl, node1, testValue, idGenerator.incrementAndGet());
    totalLevelNodeIdMaps.get(1).add(node1);
    ccsl.put(node1, 1);
    Assert.assertEquals(node1, ccsl.findFirst());
    Assert.assertEquals(node1, ccsl.findLast());

    long headNode = SKIP_LIST_HEAD_NODE;
    Assert.assertEquals(node1, ccsl.getNextNodeByIdAndLevel(headNode, 1));
    Assert.assertEquals(NIL_NODE_ID,
      ccsl.getNextNodeByIdAndLevel(ccsl.getNextNodeByIdAndLevel(headNode, 1), 1));

    Assert.assertEquals(node1, ccsl.getNextNodeByIdAndLevel(headNode, 0));
    Assert.assertEquals(NIL_NODE_ID,
      ccsl.getNextNodeByIdAndLevel(ccsl.getNextNodeByIdAndLevel(headNode, 0), 0));

    for (int i = 0; i < 10; i++) {
      long node = ccsl.getNodeAndWriteMeta(i + 1, NodeUtil.getNodeMetaLenByLevel(i + 1),
        NodeUtil.getDataLen(8, 8));

      totalNodeId.add(node);

      for (int d = i + 1; d >= 1; d--) {
        totalLevelNodeIdMaps.get(d).add(node);
      }
      copyDataToNode(ccsl, node, testValue, idGenerator.incrementAndGet());
      System.out.println("cur nodeId=" + node);
      ccsl.put(node, i + 1);
    }

    System.out.println("total nodeids=" + totalNodeId);
    long node = ccsl.getNextNodeByIdAndLevel(headNode, 0);
    List<Long> nodeIdList = new ArrayList<>();
    while (node != NIL_NODE_ID) {
      ByteBuffer keyvaluebb = NodeUtil.duplicateNodeDataByteBuffer(ccsl, node);
      long currentKey = ByteBufferUtils.toLong(keyvaluebb, 8);

      int level = ChunkUtil.getNodeLevel(ccsl.getChunkById(NodeUtil.getChunkIdByNodeId(node)),
        NodeUtil.getChunkOffsetByNodeId(node));
      for (int i = 1; i <= level; i++) {
        System.out.println(
          "nodeId=" + node + " ,level=" + level + ",key=" + currentKey + " ;level" + i + " next=>"
            + ccsl.getNextNodeByIdAndLevel(node, i));
      }
      nodeIdList.add(node);

      node = ccsl.getNextNodeByIdAndLevel(node, 0);

    }

    Assert.assertArrayEquals(totalNodeId.toArray(), nodeIdList.toArray());

    Assert.assertEquals(ccsl.findFirst(), totalNodeId.get(0).longValue());
    Assert.assertEquals(ccsl.findLast(), totalNodeId.get(totalNodeId.size() - 1).longValue());

    Assert.assertEquals(10, ccsl.getLevelIndexHeader().getLevel());
    Map<Integer, List<Long>> levelNodeMaps = new HashMap<>();
    for (int j = 10; j >= 1; j--) {
      long nodeLevel = ccsl.getNextNodeByIdAndLevel(SKIP_LIST_HEAD_NODE, j);
      List<Long> levelNodeList = new ArrayList<>();
      while (nodeLevel != NIL_NODE_ID) {
        levelNodeList.add(nodeLevel);
        nodeLevel = ccsl.getNextNodeByIdAndLevel(nodeLevel, j);
      }
      levelNodeMaps.put(j, levelNodeList);
    }
    for (int j = 10; j >= 1; j--) {
      if (j == 1) {
        Assert.assertEquals(11, levelNodeMaps.get(j).size());
      } else {
        Assert.assertEquals(10 - j + 1, levelNodeMaps.get(j).size());
      }
      Assert
        .assertArrayEquals(totalLevelNodeIdMaps.get(j).toArray(), levelNodeMaps.get(j).toArray());
    }

    for (int m = 1; m <= 10; m++) {

      byte[] readBB = new byte[8];
      Bytes.putLong(readBB, 0, testValue + m);
      long getNode = ccsl.get(readBB);

      long currentKey =
        ByteBufferUtils.toLong(NodeUtil.duplicateNodeDataByteBuffer(ccsl, getNode), 8);
      Assert.assertEquals(currentKey, testValue + m);
    }

  }

  @Test
  public void testForMultiTimes() throws Exception {
    //Correctness verification of large concurrency, and 5 repeated runs
    for (int i = 0; i < 5; i++) {
      int size = 111;
      int thread = 301;

      System.out.println(
        "============================ " + i + " time test" + "============================ ");
      doMultiPutGet(HeapMode.offHeap, size, thread);
      doMultiPutGet(HeapMode.onHeap, size, thread);
    }

  }

  private void doMultiPutGet(HeapMode heapmode, int size, int thread) throws Exception {
    CompactedConcurrentSkipList<byte[]> ccsl = newCCSList(heapmode, 128 * 1024 * 1024, 4 * 1024);

    Assert.assertEquals(0, ccsl.getCcslStat().getCprRequestCount());
    Assert.assertEquals(0, ccsl.getCcslStat().getPutRequestCount());
    Assert.assertEquals(0, ccsl.getCcslStat().getRemovedNodeCount());
    Assert.assertEquals(0, ccsl.getCcslStat().getRemoveRequestRaceFailCount());
    Assert.assertEquals(0, ccsl.getCcslStat().getReplaceRequestCount());

    final ConcurrentLinkedQueue<Long> keyQueueOri = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Long> keyQueueForPut = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Long> nodeIdQueueForPut = new ConcurrentLinkedQueue<>();
    AtomicBoolean hasException = new AtomicBoolean(false);

    //put by multi threas
    Thread[] threads = new Thread[thread];
    for (int th = 0; th < thread; th++) {
      threads[th] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            for (int i = 0; i < size; i++) {
              long node1 = generateNode(ccsl, keyQueueOri);
              nodeIdQueueForPut.add(node1);
              keyQueueForPut
                .add(ByteBufferUtils.toLong(NodeUtil.duplicateNodeDataByteBuffer(ccsl, node1), 8));
              ccsl.put(node1, ChunkUtil
                                .getNodeLevel(ccsl.getChunkById(NodeUtil.getChunkIdByNodeId(node1)),
                                  NodeUtil.getChunkOffsetByNodeId(node1)));
            }
          } catch (Exception e) {
            e.printStackTrace();
            hasException.set(true);
          }
        }
      });
    }

    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      t.join();
    }

    Assert.assertFalse(hasException.get());

    Assert.assertEquals(size * thread, ccsl.getSize());
    Assert.assertEquals(0, ccsl.getCcslStat().getReplaceRequestCount());

    List<Long> keyListForScan = new ArrayList<>();
    List<Long> nodeIdListForScan = new ArrayList<>();

    doCheckScanForHead(ccsl, keyListForScan, nodeIdListForScan,
      (Long t, Long u) -> t.longValue() == u);

    Long[] keyArrayOri = keyQueueOri.toArray(new Long[0]);
    Long[] keyArrayForPut = keyQueueForPut.toArray(new Long[0]);
    Long[] nodeIdArrayForPut = nodeIdQueueForPut.toArray(new Long[0]);
    Arrays.sort(keyArrayOri);
    Arrays.sort(keyArrayForPut);
    Arrays.sort(nodeIdArrayForPut);

    Assert.assertArrayEquals(keyArrayOri, keyListForScan.toArray());
    Assert.assertArrayEquals(keyArrayForPut, keyListForScan.toArray());

    Collections.sort(nodeIdListForScan);
    Assert.assertArrayEquals(nodeIdArrayForPut, nodeIdListForScan.toArray());

    //check level
    System.out.println(" total key => " + keyListForScan);
    checkLevel(ccsl);

    //check mem
    BlockingQueue<IChunk> chunks = ccsl.getUsedChunkQueue();
    long totalSize = 0;
    int countNum = 0;
    long maxOccupancy = Long.MIN_VALUE;
    int totalOccupancy = 0;
    int totalAlignedOccupancy = 0;
    for (IChunk chunk : chunks) {
      totalSize += chunk.getPosition();
      countNum++;
      totalOccupancy += chunk.occupancy();
      totalAlignedOccupancy += ((AbstractChunk) chunk).alignOccupancy();
      if (maxOccupancy < chunk.occupancy()) {
        maxOccupancy = chunk.occupancy();
      }
    }
    System.out.println(
      "cal totalSize=" + totalSize + " ;countNum=" + countNum + " ; maxOcc=" + maxOccupancy
        + " ;totalOccupancy=" + totalOccupancy + " ;alignOccupancy=" + totalAlignedOccupancy);
    System.out.println(
      "ccsl totalNum=" + ccsl.getSize() + " ; count=" + ccsl.getCcslStat().getChunkCount()
        + " ; replace=" + ccsl.getCcslStat().getReplaceRequestCount());

    // test get

    List<Long> keyListForGet = new ArrayList<>();
    List<Long> nodeIdListForGet = new ArrayList<>();

    for (Long id : keyArrayOri) {
      byte[] readBB = new byte[8];
      Bytes.putLong(readBB, 0, id);
      long getNode = ccsl.get(readBB);
      keyListForGet
        .add(ByteBufferUtils.toLong(NodeUtil.duplicateNodeDataByteBuffer(ccsl, getNode), 8));
      nodeIdListForGet.add(getNode);
    }

    Collections.sort(keyListForGet);
    Collections.sort(nodeIdListForGet);

    Assert.assertArrayEquals(keyListForGet.toArray(), keyListForScan.toArray());
    Assert.assertArrayEquals(nodeIdListForGet.toArray(), nodeIdListForScan.toArray());

    // test get no existed key
    long noexisted = (long) random.nextInt(Integer.MAX_VALUE);
    for (int i = 0; i < 100; i++) {
      while (keyListForGet.contains(noexisted)) {
        noexisted = (long) random.nextInt(Integer.MAX_VALUE);
      }

      byte[] readBB = new byte[8];
      Bytes.putLong(readBB, 0, noexisted);
      long noExistedNode = ccsl.get(readBB);
      Assert.assertEquals(NIL_NODE_ID, noExistedNode);
    }

    //test put same key by multi threads
    doCheckPutSameKey(size, thread, ccsl, keyQueueOri, hasException, threads, keyArrayOri,
      keyListForGet, nodeIdListForGet);

  }

  private void doCheckPutSameKey(int size, int thread,
                                  final CompactedConcurrentSkipList<byte[]> ccsl,
                                  final ConcurrentLinkedQueue<Long> keyQueueOri,
                                  final AtomicBoolean hasException, Thread[] threads,
                                  Long[] keyArrayOri, List<Long> keyListForGet,
                                  List<Long> nodeIdListForGet)
    throws InterruptedException, CCSMapException {
    List<Long> keyListForScan;
    List<Long> nodeIdListForScan;
    Long[] nodeIdArrayForPut;//repeated write
    final ConcurrentLinkedQueue<Long> nodeIdForRepeatedList = new ConcurrentLinkedQueue<>();
    for (int th = 0; th < thread; th++) {
      threads[th] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Long id;
            while ((id = keyQueueOri.poll()) != null) {
              int level = ccsl.generateRandomIndexLevel();
              long newNode = ccsl.getNodeAndWriteMeta(level, NodeUtil.getNodeMetaLenByLevel(level),
                NodeUtil.getDataLen(8, 8));
              ByteBuffer keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(ccsl, newNode);
              ByteBufferUtils.putInt(keyvalueBB, 0, 8);
              ByteBufferUtils.putInt(keyvalueBB, Integer.BYTES, 8);
              ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES, id);
              ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES + 8, id + 1010);
              nodeIdForRepeatedList.add(newNode);

              ccsl.put(newNode, level);
            }
          } catch (Exception e) {
            e.printStackTrace();
            hasException.set(true);
          }
        }
      });
    }

    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      t.join();
    }

    Assert.assertFalse(hasException.get());
    Assert.assertEquals(size * thread, ccsl.getSize());

    //    printStat("put replica");
    keyListForScan = new ArrayList<>();
    nodeIdListForScan = new ArrayList<>();

    doCheckScanForHead(ccsl, keyListForScan, nodeIdListForScan, (Long t, Long u) -> t + 1010 == u);

    Collections.sort(nodeIdListForScan);

    nodeIdArrayForPut = nodeIdForRepeatedList.toArray(new Long[0]);
    Arrays.sort(nodeIdArrayForPut);

    Assert.assertArrayEquals(keyArrayOri, keyListForScan.toArray());

    Assert.assertArrayEquals(nodeIdArrayForPut, nodeIdListForScan.toArray());

    keyListForGet.clear();
    nodeIdListForGet.clear();
    for (Long id : keyArrayOri) {

      byte[] readBB = new byte[8];
      Bytes.putLong(readBB, 0, id);
      long getNode = ccsl.get(readBB);
      ByteBuffer bb = NodeUtil.duplicateNodeDataByteBuffer(ccsl, getNode);
      long key = ByteBufferUtils.toLong(bb, 8);
      long value = ByteBufferUtils.toLong(bb, 16);
      //After rewrite, the new get is a new value
      if (value != key + 1010) {
        System.out.println(
          "read error nodeId=" + getNode + "key=" + key + " ;" + nodeIdForRepeatedList
                                                                   .contains(getNode));
      }
      Assert.assertEquals(value, key + 1010);
      keyListForGet.add(key);
      nodeIdListForGet.add(getNode);
    }

    Collections.sort(keyListForGet);
    Collections.sort(nodeIdListForGet);

    Assert.assertArrayEquals(keyListForGet.toArray(), keyListForScan.toArray());
    Assert.assertArrayEquals(nodeIdListForGet.toArray(), nodeIdListForScan.toArray());

    printStat("get replica", ccsl);

    //check index
    checkLevel(ccsl);

    ccsl.close();
  }

  private void checkLevel(CompactedConcurrentSkipList ccsl) throws CCSMapException {
    long node = ccsl.getNextNodeByIdAndLevel(SKIP_LIST_HEAD_NODE, 0);
    Map<Integer, AtomicInteger> levelCount = new HashMap<>();

    while (node != NIL_NODE_ID) {
      int level = ChunkUtil.getNodeLevel(ccsl.getChunkById(NodeUtil.getChunkIdByNodeId(node)),
        NodeUtil.getChunkOffsetByNodeId(node));
      if (NodeUtil.isNodeRemoving(ccsl, node) || level == 0) {
        node = ccsl.getNextNodeByIdAndLevel(node, 0);
        continue;
      }
      levelCount.putIfAbsent(level, new AtomicInteger(0));
      levelCount.get(level).incrementAndGet();
      node = ccsl.getNextNodeByIdAndLevel(node, 0);
    }

    System.out.println(" level 1 => " + levelCount.get(1));
    System.out.println(" level 2 => " + levelCount.get(2));
    System.out.println(" level 3 => " + levelCount.get(3));

    Map<Integer, List<Long>> levelKeys = new HashMap<>();
    for (int level = 1; level <= ccsl.getLevelIndexHeader().getLevel(); level++) {
      long nodeLevel = ccsl.getNextNodeByIdAndLevel(SKIP_LIST_HEAD_NODE, level);
      levelKeys.put(level, new ArrayList<>());
      while (nodeLevel != NIL_NODE_ID) {
        if (NodeUtil.isNodeRemoving(ccsl, nodeLevel)) {
          nodeLevel = ccsl.getNextNodeByIdAndLevel(nodeLevel, level);
          continue;
        }
        levelKeys.get(level)
          .add(ByteBufferUtils.toLong(NodeUtil.duplicateNodeDataByteBuffer(ccsl, nodeLevel), 8));
        nodeLevel = ccsl.getNextNodeByIdAndLevel(nodeLevel, level);
      }
    }
    int totalSize = 0;
    int totalLevel = 0;
    for (int le = 1; le <= ccsl.getLevelIndexHeader().getLevel(); le++) {
      System.out.println("le => " + levelKeys.get(le));
      totalSize += levelKeys.get(le).size() * 8;
      totalLevel += levelKeys.get(le).size();
    }
    System.out.println("total Index size = " +
                         (totalSize + levelCount.get(1).get() * NodeUtil.getNodeMetaLenByLevel(1))
                         + " ; totalLevel = " + totalLevel);
  }

  private void doCheckScanForHead(CompactedConcurrentSkipList ccsl, List<Long> keyListForScan,
                            List<Long> nodeIdListForScan, BiPredicate<Long, Long> predicate)
    throws CCSMapException {
    long fistNodeId = -999;
    long lastNodeId = -999;
    IIterCCSList iter = ccsl.nodeIdIter();
    boolean first = true;
    long lastKey = -1;
    int totalSize = 0;
    int totalIndexSize = 0;
    int totalDataSize = 0;
    while (iter.hasNext()) {
      long node = iter.next();
      if (NodeUtil.isNodeRemoving(ccsl, node)) {
        continue;
      }
      IChunk chunk = ccsl.getChunkById(NodeUtil.getChunkIdByNodeId(node));
      long offset = NodeUtil.getChunkOffsetByNodeId(node);
      ByteBuffer bb = NodeUtil.duplicateNodeDataByteBuffer(ccsl, node);
      long currentKey = ByteBufferUtils.toLong(bb, 8);
      long currentValue = ByteBufferUtils.toLong(bb, 16);
      if (!predicate.test(currentKey, currentValue)) {
        System.out.println("error nodeId=" + node + ", key=" + currentKey);
        throw new RuntimeException("error nodeId=" + node + ", key=" + currentKey + " ,value="
                                     + currentValue);
      }

      if (first) {
        fistNodeId = node;
        lastKey = currentKey;
        first = false;
      } else {
        lastNodeId = node;
        Assert.assertTrue(currentKey >= lastKey);
      }
      keyListForScan.add(currentKey);
      nodeIdListForScan.add(node);
      totalSize += 24 + NodeUtil.getNodeMetaLenByLevel(ChunkUtil.getNodeLevel(chunk, offset));
      totalIndexSize += NodeUtil.getNodeMetaLenByLevel(ChunkUtil.getNodeLevel(chunk, offset));
      totalDataSize += 24;
    }
    //it looks like 40% memory savings can be achieved
    System.out.println(
      "dddddddd= " + totalSize + " ; totalLevel=" + totalIndexSize + " totalDataSize="
        + totalDataSize);
    Assert.assertEquals(fistNodeId, ccsl.findFirst());
    Assert.assertEquals(lastNodeId, ccsl.findLast());
  }

  private long generateNode(CompactedConcurrentSkipList ccsl, ConcurrentLinkedQueue<Long> keyList)
    throws CCSMapException {
    int level = ccsl.generateRandomIndexLevel();
    long newNode = ccsl.getNodeAndWriteMeta(level, NodeUtil.getNodeMetaLenByLevel(level),
      NodeUtil.getDataLen(8, 8));
    copyDataToNode(ccsl, newNode, keyList);
    return newNode;
  }

  private final Random random = new Random(7);

  private void copyDataToNode(CompactedConcurrentSkipList ccsl, long node,
                               ConcurrentLinkedQueue<Long> keyList) {
    ByteBuffer keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(ccsl, node);
    ByteBufferUtils.putInt(keyvalueBB, 0, 8);
    ByteBufferUtils.putInt(keyvalueBB, Integer.BYTES, 8);
    int keyInt = random.nextInt(Integer.MAX_VALUE);
    long key = (long) keyInt;
    while (keyList.contains(key)) {
      key = random.nextInt(Integer.MAX_VALUE);
    }
    keyList.add(key);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES, key);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES + 8, key);
  }

  private void copyDataToNode(CompactedConcurrentSkipList ccsl, long node, long testValue, int id) {
    ByteBuffer keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(ccsl, node);
    ByteBufferUtils.putInt(keyvalueBB, 0, 8);
    ByteBufferUtils.putInt(keyvalueBB, Integer.BYTES, 8);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES, testValue + id);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES + 8, testValue + id);
  }

  public static CompactedConcurrentSkipList<byte[]> newCCSList(HeapMode heapmode, long capacity,
                                                                int chunkSize) {
    AllocatorHandlerRegister.AllocatorHandlerBuilder builder =
      new AllocatorHandlerRegister.AllocatorHandlerBuilder();
    builder.setCapacity(capacity);
    builder.setChunkSize(chunkSize);
    builder.setInitialCount(Integer.MAX_VALUE);
    builder.setHeapMode(heapmode);
    builder.setUseOldChunkThreshold(-1);
    CCSMapChunkPool allocatorHandler = new CCSMapChunkPool(builder);
    AllocatorHandlerRegister.register(allocatorHandler);
    return new CompactedConcurrentSkipList<>(builder, CCSMAP_TEST_BYTE_COMPARTOR, SchemaEnum.diff);
  }

  private void printStat(String msg, CompactedConcurrentSkipList ccsl) {

    System.out.println(msg + " :cprRequest count = " + ccsl.getCcslStat().getCprRequestCount());
    System.out.println(msg + " : putReq count = " + ccsl.getCcslStat().getPutRequestCount());
    System.out.println(msg + " : removed count = " + ccsl.getCcslStat().getRemovedNodeCount());
    System.out.println(
      msg + " : removeReqRace count = " + ccsl.getCcslStat().getRemoveRequestRaceFailCount());
    System.out.println(msg + " : replace count = " + ccsl.getCcslStat().getReplaceRequestCount());

    System.out.println("ccsl queue size=" + ccsl.getUsedChunkQueue().size());
    System.out.println(
      "global chunk count = " + ((CCSMapChunkPool) AllocatorHandlerRegister.getAllocatorHandler())
                                  .getChunkCounter());
    System.out.println(
      "global map size=" + ((CCSMapChunkPool) AllocatorHandlerRegister.getAllocatorHandler())
                             .getChunkMap().length);
    System.out.println(
      "global queue size=" + ((CCSMapChunkPool) AllocatorHandlerRegister.getAllocatorHandler())
                               .getChunkQueue().size());
  }

  @Test
  public void testLevelRandom() throws Exception {
    CompactedConcurrentSkipList ccsl = newCCSList(HeapMode.offHeap, 128 * 1024, 100);
    int level;
    Map<Integer, AtomicInteger> allLevels = new ConcurrentHashMap<>();
    int loop = 5000000;
    for (int i = 0; i < loop; i++) {

      level = ccsl.generateRandomIndexLevel();

      if (level == NO_INDEX) {
        continue;
      }

      if (!allLevels.containsKey(level)) {
        allLevels.putIfAbsent(level, new AtomicInteger(0));
      }
      allLevels.get(level).incrementAndGet();

      if (level > ccsl.getLevelIndexHeader().getLevel()) {
        ccsl.getLevelIndexHeader().updateLevel(level);
      }
    }

    List<Integer> allLevelList = new ArrayList<>(allLevels.keySet());
    Collections.sort(allLevelList);

    int totalInd = 0;
    int totalLevel = 0;
    int totalSize = 0;
    for (Integer id : allLevelList) {
      int c = allLevels.get(id).get();
      totalInd += c;
      totalLevel += id * c;
      System.out.println(id + "  ===> " + c);
      totalSize += c * 8;
    }
    System.out.println(" totalInd  ===> " + totalInd);
    System.out.println(" totalLevel  ===> " + totalLevel);
    System.out.println(" loop* 26/100  ===> " + (loop / 100 * 26));
    System.out.println(" loop* 24/100  ===> " + (loop / 100 * 24));

    Assert.assertTrue(totalInd >= loop / 100 * 24);
    Assert.assertTrue(totalInd <= loop / 100 * 26);

    System.out.println("jdk index mem: " + totalLevel * 40);
    System.out.println("ccsmap index mem: " + totalSize);

  }
}

