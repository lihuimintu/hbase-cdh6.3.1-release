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

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public final class MockBaseCCSMap extends BaseCCSMap<byte[], byte[]> {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(MockBaseCCSMap.class);

  public static final INodeComparator<byte[]> CCSMAP_TEST_BYTE_COMPARTOR =
    new INodeComparator<byte[]>() {
      @Override
      public int compareTo(ByteBuffer left, int leftOffset, int leftLen, ByteBuffer right,
                            int rightOffset, int rightLen) {
        return ByteBufferUtils.compareTo(left, leftOffset, leftLen, right, rightOffset, rightLen);
      }

      @Override
      public int compareTo(byte[] left, ByteBuffer right, int rightKeyOffset, int rightKeyLen) {
        ByteBuffer leftBB = ByteBuffer.wrap(left);
        return ByteBufferUtils
                 .compareTo(leftBB, 0, left.length, right, rightKeyOffset, rightKeyLen);
      }

      @Override
      public int compareTo(ByteBuffer left, int leftKeyOffset, int leftKeyLen, byte[] right) {
        ByteBuffer rightBB = ByteBuffer.wrap(right);
        return ByteBufferUtils.compareTo(left, leftKeyOffset, leftKeyLen, rightBB, 0, right.length);
      }

      @Override
      public int compareTo(byte[] left, byte[] right) {
        return Bytes.compareTo(left, right);
      }
    };

  private MockBaseCCSMap(AllocatorHandlerRegister.AllocatorHandlerBuilder builder,
                         INodeComparator<byte[]> comparator, ISerde<byte[]> keySerde,
                         ISerde<byte[]> valueSerde, Class<byte[]> keyClass) {

    super(builder, comparator, keySerde, valueSerde, keyClass);
  }

  public static MockBaseCCSMap newMockBaseCCSMap(HeapMode heapMode) {

    AllocatorHandlerRegister.AllocatorHandlerBuilder builder =
      new AllocatorHandlerRegister.AllocatorHandlerBuilder();
    builder.setCapacity(512 * 1024 * 1024);
    builder.setChunkSize(4 * 1024 * 1024);
    builder.setInitialCount(Integer.MAX_VALUE);
    builder.setHeapMode(heapMode);
    CCSMapChunkPool allocatorHandler = new CCSMapChunkPool(builder);
    AllocatorHandlerRegister.register(allocatorHandler);
    ISerde<byte[]> keySerde = new ISerde<byte[]>() {
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
          .copyFromBufferToArray(result, bb, offset, 0, len);
        return result;
      }

    };
    return new MockBaseCCSMap(builder, CCSMAP_TEST_BYTE_COMPARTOR, keySerde, keySerde,
                               byte[].class);
  }
}
