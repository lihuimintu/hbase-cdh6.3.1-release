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
import static org.apache.hadoop.hbase.regionserver.skiplist.core.TestCompactedConcurrentSkipList.newCCSList;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.TestCompactedConcurrentSkipList.readKey;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@SuppressWarnings("PointlessBitwiseExpression")
@Category(SmallTests.class)
public class TestSubCompactedConcurrentSkipList {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSubCompactedConcurrentSkipList.class);

  @Test
  public void testNormal() throws Exception {
    CompactedConcurrentSkipList<byte[]> ccslOri = newCCSList(HeapMode.onHeap, 128 * 1024, 100);

    //This subMap have all item.
    SubCompactedConcurrentSkipList<byte[]> subCcslUnbounded =
      new SubCompactedConcurrentSkipList<>(ccslOri, null, null, false, false);

    List<Long> nodeList = doTestUnboundedSubMap(ccslOri, subCcslUnbounded);
    IIterCCSList subIter;
    List<Long> nodeIter;

    //test Head sub
    byte[] headBB = new byte[8];
    Bytes.putLong(headBB, 0, 2);

    SubCompactedConcurrentSkipList<byte[]> subCcslOnlyHiBound =
      new SubCompactedConcurrentSkipList<>(ccslOri, null, headBB, false, false);
    Assert.assertEquals(0, subCcslOnlyHiBound.getSize());

    long loNodeId2_1 = subCcslOnlyHiBound.loNode();
    long hiNodeId2_1 = subCcslOnlyHiBound.hiNode();
    System.out.println("loNodeId2_1:" + loNodeId2_1 + " => " + readKey(loNodeId2_1, ccslOri));
    System.out.println("hiNodeId2_1:" + hiNodeId2_1 + " => " + readKey(hiNodeId2_1, ccslOri));
    Assert.assertEquals(NIL_NODE_ID, readKey(loNodeId2_1, ccslOri));
    Assert.assertEquals(NIL_NODE_ID, readKey(hiNodeId2_1, ccslOri));

    subIter = subCcslOnlyHiBound.nodeIdIter();
    nodeIter = new ArrayList<>();
    while (subIter.hasNext()) {
      long iterNode = subIter.next();
      nodeIter.add(iterNode);
    }

    Assert.assertEquals(0, nodeIter.size());

    //test tail sub
    byte[] tailBB = new byte[8];
    Bytes.putLong(tailBB, 0, 200);

    SubCompactedConcurrentSkipList<byte[]> subCcsl2_2 =
      new SubCompactedConcurrentSkipList<>(ccslOri, tailBB, null, false, false);
    Assert.assertEquals(0, subCcsl2_2.getSize());

    long loNodeId2_2 = subCcsl2_2.loNode();
    long hiNodeId2_2 = subCcsl2_2.hiNode();
    System.out.println("loNodeId2_2:" + loNodeId2_2 + " => " + readKey(loNodeId2_2, ccslOri));
    System.out.println("hiNodeId2_2:" + hiNodeId2_2 + " => " + readKey(hiNodeId2_2, ccslOri));
    Assert.assertEquals(NIL_NODE_ID, readKey(loNodeId2_2, ccslOri));
    Assert.assertEquals(NIL_NODE_ID, readKey(hiNodeId2_2, ccslOri));

    subIter = subCcsl2_2.nodeIdIter();
    nodeIter = new ArrayList<>();
    while (subIter.hasNext()) {
      long iterNode = subIter.next();
      nodeIter.add(iterNode);
    }

    Assert.assertEquals(0, nodeIter.size());

    //Test open interval subset
    byte[] startBB = new byte[8];
    Bytes.putLong(startBB, 0, 4);

    byte[] endBB = new byte[8];
    Bytes.putLong(endBB, 0, 14);

    SubCompactedConcurrentSkipList<byte[]> subCcslHiLoExclusive =
      new SubCompactedConcurrentSkipList<>(ccslOri, startBB, endBB, false, false);

    long loNodeId = subCcslHiLoExclusive.loNode();
    long hiNodeId = subCcslHiLoExclusive.hiNode();
    System.out.println("loNodeId:" + loNodeId + " => " + readKey(loNodeId, ccslOri));
    System.out.println("hiNodeId:" + hiNodeId + " => " + readKey(hiNodeId, ccslOri));
    Assert.assertEquals(6, readKey(loNodeId, ccslOri));
    Assert.assertEquals(12, readKey(hiNodeId, ccslOri));
    Assert.assertEquals(4, subCcslHiLoExclusive.getSize());

    subIter = subCcslHiLoExclusive.nodeIdIter();
    nodeIter = new ArrayList<>();
    while (subIter.hasNext()) {
      long iterNode = subIter.next();
      nodeIter.add(iterNode);
    }

    Assert.assertEquals(nodeIter.size(), 4);
    for (int i = 0; i < nodeIter.size(); i++) {
      Assert.assertEquals(6 + i * 2, readKey(nodeIter.get(i), subCcslHiLoExclusive));
    }

    Assert.assertEquals(NIL_NODE_ID, subCcslHiLoExclusive.get(startBB));
    Assert.assertEquals(NIL_NODE_ID, subCcslHiLoExclusive.get(endBB));

    Assert.assertEquals(NIL_NODE_ID, subCcslHiLoExclusive.remove(startBB));
    Assert.assertEquals(NIL_NODE_ID, subCcslHiLoExclusive.remove(endBB));

    String expectException = "WANT_EXCEPTION";
    try {
      subCcslHiLoExclusive.put(nodeList.get(1), 1);
    } catch (Exception e) {
      expectException = e.getMessage();
    }

    Assert.assertEquals("key out of range", expectException);

    //Test closed interval subset

    SubCompactedConcurrentSkipList<byte[]> subCcslHiLoInclusive =
      new SubCompactedConcurrentSkipList<>(ccslOri, startBB, endBB, true, true);

    doTestHiLoInclusiveSubMap(ccslOri, nodeList, startBB, endBB, subCcslHiLoInclusive);

    doRewriteDeletedNode(ccslOri, subCcslUnbounded, subCcslHiLoInclusive);
  }

  private void doTestHiLoInclusiveSubMap(CompactedConcurrentSkipList<byte[]> ccslOri,
    List<Long> nodeList, byte[] startBB, byte[] endBB,
    SubCompactedConcurrentSkipList<byte[]> subCcslHiLoInclusive)
    throws org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException {
    long loNodeId;
    long hiNodeId;
    IIterCCSList subIter;
    List<Long> nodeIter;
    String expectException;
    loNodeId = subCcslHiLoInclusive.loNode();
    hiNodeId = subCcslHiLoInclusive.hiNode();
    System.out.println("loNodeId:" + loNodeId + " => " + readKey(loNodeId, ccslOri));
    System.out.println("hiNodeId:" + hiNodeId + " => " + readKey(hiNodeId, ccslOri));
    Assert.assertEquals(4, readKey(loNodeId, ccslOri));
    Assert.assertEquals(14, readKey(hiNodeId, ccslOri));
    Assert.assertEquals(6, subCcslHiLoInclusive.getSize());

    subIter = subCcslHiLoInclusive.nodeIdIter();
    nodeIter = new ArrayList<>();
    while (subIter.hasNext()) {
      long iterNode = subIter.next();
      nodeIter.add(iterNode);
    }

    Assert.assertEquals(6, nodeIter.size());
    for (int i = 0; i < nodeIter.size(); i++) {
      Assert.assertEquals(4 + i * 2, readKey(nodeIter.get(i), subCcslHiLoInclusive));
    }

    Assert.assertEquals(subCcslHiLoInclusive.get(startBB), nodeList.get(1).longValue());
    Assert.assertEquals(subCcslHiLoInclusive.get(endBB), nodeList.get(6).longValue());

    Assert.assertEquals(subCcslHiLoInclusive.remove(startBB), nodeList.get(1).longValue());
    Assert.assertEquals(subCcslHiLoInclusive.remove(endBB), nodeList.get(6).longValue());

    expectException = "WANT_EXCEPTION";
    try {
      subCcslHiLoInclusive.put(nodeList.get(0), 1);
    } catch (Exception e) {
      expectException = e.getMessage();
    }

    Assert.assertEquals("key out of range", expectException);

    loNodeId = subCcslHiLoInclusive.loNode();
    hiNodeId = subCcslHiLoInclusive.hiNode();

    System.out.println("loNodeId:" + loNodeId + " => " + readKey(loNodeId, ccslOri));
    System.out.println("hiNodeId:" + hiNodeId + " => " + readKey(hiNodeId, ccslOri));

    Assert.assertEquals(6, readKey(loNodeId, ccslOri));
    Assert.assertEquals(12, readKey(hiNodeId, ccslOri));

    Assert.assertEquals(4, subCcslHiLoInclusive.getSize());
    Assert.assertEquals(98, ccslOri.getSize());

    subIter = subCcslHiLoInclusive.nodeIdIter();
    nodeIter = new ArrayList<>();
    while (subIter.hasNext()) {
      long iterNode = subIter.next();
      nodeIter.add(iterNode);
    }

    IIterCCSList oriIter = ccslOri.nodeIdIter();
    List<Long> allNodeIterList = new ArrayList<>();
    while (oriIter.hasNext()) {
      long iterNode = oriIter.next();
      allNodeIterList.add(iterNode);
    }

    Assert.assertEquals(98, allNodeIterList.size());
    Assert.assertEquals(4, nodeIter.size());
    for (int i = 0; i < nodeIter.size(); i++) {
      Assert.assertEquals(6 + i * 2, readKey(nodeIter.get(i), subCcslHiLoInclusive));
    }

    expectException = "WANT_EXCEPTION";
    try {
      subCcslHiLoInclusive.put(nodeList.get(1), 1);
    } catch (Exception e) {
      expectException = e.getMessage();
    }

    Assert.assertEquals("can't put existed node", expectException);
  }

  private List<Long> doTestUnboundedSubMap(CompactedConcurrentSkipList<byte[]> ccslOri,
    SubCompactedConcurrentSkipList<byte[]> subCcslUnbounded)
    throws org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException {
    IIterCCSList subIter = subCcslUnbounded.nodeIdIter();
    Assert.assertFalse(subIter.hasNext());

    boolean first = true;
    long firstNodeId = -999;
    long lastNodeId = -999;
    List<Long> nodeList = new ArrayList<>();
    //put 100 data
    for (int i = 1; i <= 100; i++) {
      int level = subCcslUnbounded.generateRandomIndexLevel();
      long node = subCcslUnbounded.getNodeAndWriteMeta(level, NodeUtil.getNodeMetaLenByLevel(level),
        NodeUtil.getDataLen(8, 8));
      ByteBuffer keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(subCcslUnbounded, node);
      ByteBufferUtils.putInt(keyvalueBB, 0, 8);
      ByteBufferUtils.putInt(keyvalueBB, Integer.BYTES, 8);
      ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES, i * 2);
      ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES + 8, i);

      subCcslUnbounded.put(node, level);
      if (first) {
        firstNodeId = node;
        first = false;
      } else {
        lastNodeId = node;
      }

      nodeList.add(i - 1, node);
    }

    Assert.assertEquals(100, subCcslUnbounded.getSize());
    Assert.assertEquals(100, ccslOri.getSize());
    Assert.assertEquals(firstNodeId, ccslOri.findFirst());
    Assert.assertEquals(firstNodeId, subCcslUnbounded.findFirst());
    Assert.assertEquals(lastNodeId, ccslOri.findLast());
    Assert.assertEquals(lastNodeId, subCcslUnbounded.findLast());

    subIter = subCcslUnbounded.nodeIdIter();
    List<Long> nodeIter = new ArrayList<>();
    while (subIter.hasNext()) {
      long iterNode = subIter.next();
      nodeIter.add(iterNode);
    }

    Assert.assertArrayEquals(nodeList.toArray(), nodeIter.toArray());

    for (int i = 100; i >= 1; i--) {
      byte[] existedBB = new byte[8];
      Bytes.putLong(existedBB, 0, i * 2);

      Assert
        .assertEquals(i * 2, readKey(subCcslUnbounded.findNear(existedBB, EQ), subCcslUnbounded));
      if (i == 0) {
        Assert.assertEquals(NIL_NODE_ID,
          readKey(subCcslUnbounded.findNear(existedBB, LT), subCcslUnbounded));
      } else {
        Assert.assertEquals(i * 2 - 2,
          readKey(subCcslUnbounded.findNear(existedBB, LT), subCcslUnbounded));
      }
      Assert.assertEquals(i * 2,
        readKey(subCcslUnbounded.findNear(existedBB, LT | EQ), subCcslUnbounded));
      if (i == 100) {
        Assert.assertEquals(NIL_NODE_ID,
          readKey(subCcslUnbounded.findNear(existedBB, GT), subCcslUnbounded));
      } else {
        Assert.assertEquals(i * 2 + 2,
          readKey(subCcslUnbounded.findNear(existedBB, GT), subCcslUnbounded));
      }
      Assert.assertEquals(i * 2,
        readKey(subCcslUnbounded.findNear(existedBB, GT | EQ), subCcslUnbounded));

      byte[] noExistedBB = new byte[8];
      Bytes.putLong(noExistedBB, 0, i * 2 + 1);

      if (i == 100) {
        Assert.assertEquals(NIL_NODE_ID,
          readKey(subCcslUnbounded.findNear(noExistedBB, EQ), subCcslUnbounded));
      } else {
        Assert.assertEquals(i * 2 + 2,
          readKey(subCcslUnbounded.findNear(noExistedBB, EQ), subCcslUnbounded));
      }

      Assert
        .assertEquals(i * 2, readKey(subCcslUnbounded.findNear(noExistedBB, LT), subCcslUnbounded));
      Assert.assertEquals(i * 2,
        readKey(subCcslUnbounded.findNear(noExistedBB, LT | EQ), subCcslUnbounded));

      if (i == 100) {
        Assert.assertEquals(NIL_NODE_ID,
          readKey(subCcslUnbounded.findNear(noExistedBB, GT), subCcslUnbounded));
        Assert.assertEquals(NIL_NODE_ID,
          readKey(subCcslUnbounded.findNear(noExistedBB, GT | EQ), subCcslUnbounded));
      } else {
        Assert.assertEquals(i * 2 + 2,
          readKey(subCcslUnbounded.findNear(noExistedBB, GT), subCcslUnbounded));
        Assert.assertEquals(i * 2 + 2,
          readKey(subCcslUnbounded.findNear(noExistedBB, GT | EQ), subCcslUnbounded));
      }
    }
    return nodeList;
  }

  private void doRewriteDeletedNode(CompactedConcurrentSkipList<byte[]> ccslOri,
    SubCompactedConcurrentSkipList<byte[]> subCcsl1,
    SubCompactedConcurrentSkipList<byte[]> subCcsl3)
    throws org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException {
    long loNodeId;
    long hiNodeId;
    IIterCCSList subIter;
    List<Long> nodeIter;//rewrite the data which are deleted.
    int level = subCcsl1.generateRandomIndexLevel();
    long node1_2 = subCcsl1
      .getNodeAndWriteMeta(level, NodeUtil.getNodeMetaLenByLevel(level), NodeUtil.getDataLen(8, 8));
    ByteBuffer keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(subCcsl1, node1_2);
    ByteBufferUtils.putInt(keyvalueBB, 0, 8);
    ByteBufferUtils.putInt(keyvalueBB, Integer.BYTES, 8);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES, 4);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES + 8, 4);

    subCcsl1.put(node1_2, level);

    long node6_2 = subCcsl1
      .getNodeAndWriteMeta(level, NodeUtil.getNodeMetaLenByLevel(level), NodeUtil.getDataLen(8, 8));
    keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(subCcsl1, node6_2);
    ByteBufferUtils.putInt(keyvalueBB, 0, 8);
    ByteBufferUtils.putInt(keyvalueBB, Integer.BYTES, 8);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES, 14);
    ByteBufferUtils.putLong(keyvalueBB, 2 * Integer.BYTES + 8, 14);

    subCcsl1.put(node6_2, level);

    Assert.assertEquals(6, subCcsl3.getSize());
    Assert.assertEquals(100, ccslOri.getSize());

    loNodeId = subCcsl3.loNode();
    hiNodeId = subCcsl3.hiNode();
    System.out.println("loNodeId:" + loNodeId + " => " + readKey(loNodeId, ccslOri));
    System.out.println("hiNodeId:" + hiNodeId + " => " + readKey(hiNodeId, ccslOri));
    Assert.assertEquals(4, readKey(loNodeId, ccslOri));
    Assert.assertEquals(14, readKey(hiNodeId, ccslOri));

    subIter = subCcsl3.nodeIdIter();
    nodeIter = new ArrayList<>();
    while (subIter.hasNext()) {
      long iterNode = subIter.next();
      nodeIter.add(iterNode);
    }

    Assert.assertEquals(6, nodeIter.size());
    for (int i = 0; i < nodeIter.size(); i++) {
      Assert.assertEquals(4 + i * 2, readKey(nodeIter.get(i), subCcsl3));
    }

    Assert.assertEquals(node1_2, nodeIter.get(0).longValue());
    Assert.assertEquals(node6_2, nodeIter.get(5).longValue());
  }
}