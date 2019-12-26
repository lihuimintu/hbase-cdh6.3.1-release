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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.INDEX_NOT_INITIAL;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestChunkUtil {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestChunkUtil.class);

  @Test
  public void testNormal() throws Exception {
    int offset = 1;
    IChunk chunk = new OnHeapChunk(offset, 130);
    dotest(chunk, offset, 0, SchemaEnum.diff);
    dotest(chunk, offset, 3, SchemaEnum.diff);
    dotest(chunk, offset, 0, SchemaEnum.same);
    dotest(chunk, offset, 3, SchemaEnum.same);

    offset = 11;
    chunk = new OffHeapChunk(offset, 130);
    dotest(chunk, offset, 0, SchemaEnum.diff);
    dotest(chunk, offset, 3, SchemaEnum.diff);
    dotest(chunk, offset, 0, SchemaEnum.same);
    dotest(chunk, offset, 3, SchemaEnum.same);
  }

  private void dotest(IChunk chunk, int offset, int level, SchemaEnum schemaEnum) throws Exception {
    int keylen = 12;
    int valueLen = 23;

    int dataLen = NodeUtil.getDataLen(keylen, valueLen);
    int metaLen = NodeUtil.getNodeMetaLenByLevel(level);

    ChunkUtil.newNodeOnChunk(chunk, offset, level, dataLen);
    for (int i = 1; i < level; i++) {
      Assert.assertEquals(INDEX_NOT_INITIAL, ChunkUtil.getNextNodeIdForLevel(chunk, offset, i));
    }
    Assert.assertEquals(level, ChunkUtil.getNodeLevel(chunk, offset));
    Assert.assertEquals(ChunkUtil.getKeyOffset(chunk, offset, schemaEnum),
      schemaEnum == SchemaEnum.diff ? offset + metaLen + 2 * Integer.BYTES : offset + metaLen);
    Assert.assertEquals(dataLen, ChunkUtil.getNodeDataLen(chunk, offset));
    Assert.assertEquals(offset + metaLen, ChunkUtil.getNodeDataOffset(chunk, offset));
    if (schemaEnum == SchemaEnum.same) {
      Assert.assertEquals(dataLen, ChunkUtil.getKeyLen(chunk, offset,
        ChunkUtil.getKeyOffset(chunk, offset, schemaEnum), schemaEnum));
      //if SchemaEnum is diff, the Keylen can be read only after written.
    }

    ChunkUtil
      .writeNodeData(chunk, offset, metaLen, new Object(), new Object(), serde, serde, keylen,
        valueLen);

    //put data
    int startOffset =
      offset + NodeUtil.getNodeMetaLenByLevel(ChunkUtil.getNodeLevel(chunk, offset));
    for (int i = 0; i < keylen; i++) {
      Assert.assertEquals('N', chunk.getByteBuffer().get(startOffset + 8 + i));
    }

    for (int i = 0; i < valueLen; i++) {
      Assert.assertEquals('N', chunk.getByteBuffer().get(startOffset + 8 + keylen + i));
    }

    Assert.assertEquals(valueLen, chunk.getByteBuffer().getInt(startOffset + 4));
    Assert.assertEquals(keylen, ChunkUtil.testGetNodeKeyLen(chunk, offset));

    ChunkUtil.setNextNodeIdForLevel(chunk, offset, 0, 1234);
    Assert.assertEquals(1234, ChunkUtil.getNextNodeIdForLevel(chunk, offset, 0));

    String expectedException = "WANT_EXCEPTION";
    try {
      ChunkUtil.newNodeOnChunk(null, offset, level, dataLen);
    } catch (Exception e) {
      expectedException = e.getMessage();
    }
    Assert.assertEquals("chunk must be not null.", expectedException);

    expectedException = "WANT_EXCEPTION";
    try {
      ChunkUtil.newNodeOnChunk(chunk, -1, level, dataLen);
    } catch (Exception e) {
      expectedException = e.getMessage();
    }
    Assert.assertEquals("offset can't be negative.", expectedException);

    expectedException = "WANT_EXCEPTION";
    try {
      ChunkUtil.newNodeOnChunk(chunk, 0, level, -1);
    } catch (Exception e) {
      expectedException = e.getMessage();
    }
    Assert.assertEquals("dataLen error.", expectedException);
  }

  private final ISerde<Object> serde = new ISerde<Object>() {

    @Override
    public void serialize(Object obj, ByteBuffer destination, int offset, int len)
      throws SerdeException {
      for (int i = 0; i < len; i++) {
        destination.put(offset + i, (byte) 'N');
      }
    }

    @Override
    public int getSerializedSize(Object obj) throws SerdeException {
      return 0;
    }

    @Override
    public Object deserialize(ByteBuffer bb, int offset, int len) throws SerdeException {
      return null;
    }

  };
}