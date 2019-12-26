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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.ChunkUtil.getNodeLevel;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.INDEX_NOT_INITIAL;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NIL_NODE_ID;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.MockBaseCCSMap.CCSMAP_TEST_BYTE_COMPARTOR;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestNodeUtil {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNodeUtil.class);

  @Test
  public void testNormal() throws Exception {
    testNormal(HeapMode.offHeap, SchemaEnum.diff);
    testNormal(HeapMode.onHeap, SchemaEnum.diff);
    testNormal(HeapMode.offHeap, SchemaEnum.same);
    testNormal(HeapMode.onHeap, SchemaEnum.same);
  }

  private void testNormal(HeapMode heapMode, SchemaEnum schemaEnum) throws CCSMapException {
    AllocatorHandlerRegister.AllocatorHandlerBuilder builder =
      new AllocatorHandlerRegister.AllocatorHandlerBuilder();
    builder.setCapacity(8 * 1024 * 1024);
    builder.setChunkSize(4 * 1024);
    builder.setInitialCount(Integer.MAX_VALUE);
    builder.setHeapMode(heapMode);
    CCSMapChunkPool allocatorHandler = new CCSMapChunkPool(builder);
    AllocatorHandlerRegister.register(allocatorHandler);
    CompactedConcurrentSkipList<byte[]> ccsl =
      new CompactedConcurrentSkipList<>(builder, CCSMAP_TEST_BYTE_COMPARTOR, schemaEnum);
    int keyLen = 6;
    int valueLen = 21;
    long node1 = ccsl.getNodeAndWriteMeta(0, NodeUtil.getNodeMetaLenByLevel(0),
      NodeUtil.getDataLen(keyLen, valueLen));
    long node2 = ccsl.getNodeAndWriteMeta(1, NodeUtil.getNodeMetaLenByLevel(1),
      NodeUtil.getDataLen(keyLen + 1, valueLen + 1));

    IChunk chunk1 = allocatorHandler.getChunkById(NodeUtil.getChunkIdByNodeId(node1));
    IChunk chunk2 = allocatorHandler.getChunkById(NodeUtil.getChunkIdByNodeId(node2));

    int offset1 = (int) NodeUtil.getChunkOffsetByNodeId(node1);

    int startOffset1 = offset1 + NodeUtil.getNodeMetaLenByLevel(getNodeLevel(chunk1, offset1));
    //write data
    ByteBufferUtils.putInt(chunk1.getByteBuffer(), startOffset1, keyLen);
    ByteBufferUtils.putInt(chunk1.getByteBuffer(), startOffset1 + Integer.BYTES, valueLen);

    int offset2 = (int) NodeUtil.getChunkOffsetByNodeId(node2);

    int startOffset2 = offset2 + NodeUtil.getNodeMetaLenByLevel(getNodeLevel(chunk2, offset2));
    ByteBufferUtils.putInt(chunk2.getByteBuffer(), startOffset2, keyLen + 1);
    ByteBufferUtils.putInt(chunk2.getByteBuffer(), startOffset2 + Integer.BYTES, valueLen + 1);

    Assert.assertEquals(16, NodeUtil.getNodeMetaLenByLevel(0));
    Assert.assertEquals(16 + 8, NodeUtil.getNodeMetaLenByLevel(1));
    Assert.assertEquals(16 + 2 * 8, NodeUtil.getNodeMetaLenByLevel(2));

    Assert.assertEquals(51, NodeUtil.getNodeLen(0, keyLen, valueLen));
    Assert.assertEquals(59, NodeUtil.getNodeLen(1, keyLen, valueLen));

    Assert.assertEquals(1, NodeUtil.getChunkIdByNodeId(node1));
    Assert.assertEquals(0, NodeUtil.getChunkOffsetByNodeId(node1));
    Assert.assertEquals(1, NodeUtil.getChunkIdByNodeId(node2));
    Assert.assertEquals(56, NodeUtil.getChunkOffsetByNodeId(node2));

    //keyOffset is right, when do lock or unlock
    Assert.assertEquals(schemaEnum == SchemaEnum.diff ? 24 : 16,
      ChunkUtil.getKeyOffset(chunk1, NodeUtil.getChunkOffsetByNodeId(node1), schemaEnum));
    Assert.assertEquals(schemaEnum == SchemaEnum.diff ? 88 : 80,
      ChunkUtil.getKeyOffset(chunk1, NodeUtil.getChunkOffsetByNodeId(node2), schemaEnum));

    Assert.assertFalse(NodeUtil.isNodeRemoving(allocatorHandler, node1));
    Assert.assertTrue(NodeUtil.casSetNodeStat(allocatorHandler, node1, NodeStat.REMOVE));
    Assert.assertTrue(NodeUtil.isNodeRemoving(allocatorHandler, node1));
    Assert.assertFalse(NodeUtil.casSetNodeStat(allocatorHandler, node1, NodeStat.REMOVE));
    Assert.assertTrue(NodeUtil.isNodeRemoving(allocatorHandler, node1));

    Assert.assertTrue(NodeUtil.casNodeLock(allocatorHandler, node2, NodeStat.PUT));
    Assert.assertFalse(NodeUtil.isNodeRemoving(allocatorHandler, node2));
    Assert.assertFalse(NodeUtil.casNodeLock(allocatorHandler, node2, NodeStat.PUT));
    Assert.assertFalse(NodeUtil.casNodeLock(allocatorHandler, node2, NodeStat.REMOVE));
    Assert.assertFalse(NodeUtil.casNodeUnLock(allocatorHandler, node2, NodeStat.REMOVE));
    Assert.assertFalse(NodeUtil.isNodeRemoving(allocatorHandler, node2));
    Assert.assertTrue(NodeUtil.casNodeUnLock(allocatorHandler, node2, NodeStat.PUT));
    Assert.assertTrue(NodeUtil.casNodeLock(allocatorHandler, node2, NodeStat.REMOVE));

    Assert.assertFalse(NodeUtil.isNodeRemoving(allocatorHandler, node2));
    Assert.assertFalse(NodeUtil.casSetNodeStat(allocatorHandler, node2, NodeStat.REMOVE));

    Assert.assertTrue(NodeUtil.isNodeRemoving(allocatorHandler, node1));

    Assert.assertEquals(NIL_NODE_ID, NodeUtil.getNextNodeIdForLevel(allocatorHandler, node1, 0));
    Assert.assertEquals(NIL_NODE_ID, NodeUtil.getNextNodeIdForLevel(allocatorHandler, node2, 0));
    Assert
      .assertEquals(INDEX_NOT_INITIAL, NodeUtil.getNextNodeIdForLevel(allocatorHandler, node2, 1));

    //keyOffset is right, when do lock or unlock
    Assert.assertEquals(schemaEnum == SchemaEnum.diff ? 24 : 16,
      ChunkUtil.getKeyOffset(chunk1, NodeUtil.getChunkOffsetByNodeId(node1), schemaEnum));
    Assert.assertEquals(schemaEnum == SchemaEnum.diff ? 88 : 80,
      ChunkUtil.getKeyOffset(chunk1, NodeUtil.getChunkOffsetByNodeId(node2), schemaEnum));

    Assert
      .assertTrue(NodeUtil.casUpdateNextNodeForLevel(allocatorHandler, 0, node1, NIL_NODE_ID, 123));
    Assert.assertEquals(123, NodeUtil.getNextNodeIdForLevel(allocatorHandler, node1, 0));

    Assert.assertFalse(NodeUtil.casUpdateNextNodeForLevel(allocatorHandler, 0, node2, 1, 123));
    Assert.assertEquals(NIL_NODE_ID, NodeUtil.getNextNodeIdForLevel(allocatorHandler, node2, 0));

    Assert.assertTrue(NodeUtil.casUpdateNextNodeForLevel(allocatorHandler, 0, node1, 123, 124));
    Assert.assertEquals(124, NodeUtil.getNextNodeIdForLevel(allocatorHandler, node1, 0));

    Assert.assertEquals(keyLen, ChunkUtil.testGetNodeKeyLen(chunk1, offset1));
    Assert
      .assertEquals(schemaEnum == SchemaEnum.diff ? keyLen : NodeUtil.getDataLen(keyLen, valueLen),
        ChunkUtil.getKeyLen(chunk1, NodeUtil.getChunkOffsetByNodeId(node1),
          ChunkUtil.getKeyOffset(chunk1, NodeUtil.getChunkOffsetByNodeId(node1), schemaEnum),
          schemaEnum));

    ByteBuffer keyvalueBB = NodeUtil.duplicateNodeDataByteBuffer(allocatorHandler, node1);
    Assert.assertEquals(0, keyvalueBB.position());
    Assert.assertEquals(keyLen + valueLen + 8, keyvalueBB.limit());
    Assert.assertEquals(keyLen + valueLen + 8, keyvalueBB.capacity());

    Assert.assertEquals(keyLen, ByteBufferUtils.toInt(keyvalueBB, 0));
    Assert.assertEquals(keyLen, keyvalueBB.getInt(0));
    Assert.assertEquals(valueLen, keyvalueBB.getInt(4));

    Assert.assertEquals(keyvalueBB.getInt(0), chunk1.getByteBuffer().getInt(startOffset1));
    Assert.assertEquals(keyvalueBB.getInt(Integer.BYTES),
      chunk1.getByteBuffer().getInt(startOffset1 + Integer.BYTES));

    ByteBuffer keyvalueBB2 = NodeUtil.duplicateNodeDataByteBuffer(allocatorHandler, node2);
    Assert.assertEquals(0, keyvalueBB2.position());
    Assert.assertEquals(keyLen + 1 + valueLen + 1 + 8, keyvalueBB2.limit());
    Assert.assertEquals(keyLen + 1 + valueLen + 1 + 8, keyvalueBB2.capacity());

    Assert.assertEquals(keyLen + 1, ByteBufferUtils.toInt(keyvalueBB2, 0));
    Assert.assertEquals(keyLen + 1, keyvalueBB2.getInt(0));
    Assert.assertEquals(valueLen + 1, keyvalueBB2.getInt(4));

    Assert.assertEquals(keyvalueBB2.getInt(0), chunk1.getByteBuffer().getInt(startOffset2));
    Assert.assertEquals(keyvalueBB2.getInt(Integer.BYTES),
      chunk1.getByteBuffer().getInt(startOffset2 + Integer.BYTES));

  }

}