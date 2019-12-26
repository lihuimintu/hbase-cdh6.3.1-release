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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.MockBaseCCSMap.CCSMAP_TEST_BYTE_COMPARTOR;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestBaseTwinCCSMap {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBaseTwinCCSMap.class);
  private final Random random = new Random(7);

  private static final ISerde<byte[]> sameSerde = new ISerde<byte[]>() {
    @Override
    public void serialize(byte[] obj, ByteBuffer destination, int offset, int len)
        throws SerdeException {
      org.apache.hadoop.hbase.util.ByteBufferUtils
          .copyFromArrayToBuffer(destination, offset, obj, 0, obj.length);
    }

    @Override
    public int getSerializedSize(byte[] obj) throws SerdeException {
      return obj.length;
    }

    @Override
    public byte[] deserialize(ByteBuffer bb, int offset, int len) throws SerdeException {
      byte[] result = new byte[len];
      org.apache.hadoop.hbase.util.ByteBufferUtils
          .copyFromBufferToArray(result, bb, offset, 0, result.length);
      return result;
    }

  };

  private static final ISerde<byte[]> errorSerde = new ISerde<byte[]>() {
    @Override
    public void serialize(byte[] obj, ByteBuffer destination, int offset, int len)
        throws SerdeException {
      org.apache.hadoop.hbase.util.ByteBufferUtils
          .copyFromArrayToBuffer(destination, offset, obj, 0, obj.length);
    }

    @Override
    public int getSerializedSize(byte[] obj) throws SerdeException {
      return obj.length;
    }

    @Override
    public byte[] deserialize(ByteBuffer bb, int offset, int len) throws SerdeException {
      byte[] result = new byte[len];
      org.apache.hadoop.hbase.util.ByteBufferUtils
          .copyFromBufferToArray(result, bb, offset, 0, result.length);
      return result;
    }

  };

  @Test
  public void testNormal() throws Exception {
    doNormal(sameSerde);
    doNormal(errorSerde);
  }

  private void doNormal(ISerde<byte[]> serde){
    BaseTwinCCSMap<byte[]> testCCSMap = newBaseTwinCCSMap(HeapMode.onHeap, serde);

    int keyLenght = 50;
    byte[] bytes = new byte[keyLenght];
    random.nextBytes(bytes);
    testCCSMap.put(bytes, bytes);

    Assert.assertEquals(1, testCCSMap.size());

    byte[] dst = testCCSMap.get(bytes);
    Assert.assertArrayEquals(bytes, dst);

    byte[] rm = testCCSMap.remove(bytes);
    Assert.assertArrayEquals(bytes, rm);

    Assert.assertEquals(null, testCCSMap.get(bytes));
  }

  public static BaseTwinCCSMap<byte[]> newBaseTwinCCSMap(HeapMode heapMode) {
    return newBaseTwinCCSMap(heapMode, sameSerde);
  }

  public static BaseTwinCCSMap<byte[]> newBaseTwinCCSMap(HeapMode heapMode, ISerde<byte[]> serde) {

    AllocatorHandlerRegister.AllocatorHandlerBuilder builder =
      new AllocatorHandlerRegister.AllocatorHandlerBuilder();
    builder.setCapacity(512 * 1024 * 1024);
    builder.setChunkSize(4 * 1024 * 1024);
    builder.setInitialCount(Integer.MAX_VALUE);
    builder.setHeapMode(heapMode);
    CCSMapChunkPool allocatorHandler = new CCSMapChunkPool(builder);
    AllocatorHandlerRegister.register(allocatorHandler);

    return new BaseTwinCCSMap<>(builder, CCSMAP_TEST_BYTE_COMPARTOR, serde, byte[].class);
  }
}