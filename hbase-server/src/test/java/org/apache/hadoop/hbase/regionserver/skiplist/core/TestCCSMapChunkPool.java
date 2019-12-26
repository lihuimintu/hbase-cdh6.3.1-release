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

import static org.mockito.Mockito.mock;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCCSMapChunkPool {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCCSMapChunkPool.class);
  @Test
  public void testNormal() throws Exception {
    AllocatorHandlerRegister.AllocatorHandlerBuilder builder =
        new AllocatorHandlerRegister.AllocatorHandlerBuilder();
    builder.setCapacity(8 * 1024 * 1024);
    builder.setChunkSize(4 * 1024);
    builder.setInitialCount(Integer.MAX_VALUE);
    builder.setHeapMode(HeapMode.offHeap);
    CCSMapChunkPool allocatorHandler = new CCSMapChunkPool(builder);

    Field chunkQueueField = CCSMapChunkPool.class.getDeclaredField("chunkQueue");
    chunkQueueField.setAccessible(true);
    @SuppressWarnings("unchecked") ConcurrentLinkedQueue<IChunk> chunkQueue =
        (ConcurrentLinkedQueue<IChunk>) chunkQueueField.get(allocatorHandler);
    Assert.assertEquals(2 * 1024, chunkQueue.size());

    Field chunkMapField = CCSMapChunkPool.class.getDeclaredField("totalPoolChunkArray");
    chunkMapField.setAccessible(true);
    IChunk[] totalPoolChunkArray = (IChunk[]) chunkMapField.get(allocatorHandler);

    Map<Long, IChunk> hugeChunkMap = allocatorHandler.getExtraChunkMap();

    Assert.assertEquals(totalPoolChunkArray.length, chunkQueue.size());

    Assert.assertEquals(2 * 1024, allocatorHandler.getChunkCounter());
    Assert.assertEquals(2 * 1024, allocatorHandler.getMaxCount());
    Assert.assertEquals(0, allocatorHandler.getRequestCounter());
    Assert.assertEquals(0, allocatorHandler.getExtraRequestCounter());
    Assert.assertEquals(0, hugeChunkMap.size());

    IChunk chunk = allocatorHandler.allocate(4 * 1024 - 1);
    Assert.assertTrue(chunk.isPooledChunk());
    Assert.assertEquals(HeapMode.offHeap, chunk.getHeapMode());
    Assert.assertEquals(4 * 1024, chunk.getLimit());
    Assert.assertEquals(1, chunk.getChunkId());
    Assert.assertEquals(0, chunk.getPosition());

    Assert.assertEquals(2 * 1024 - 1, chunkQueue.size());
    Assert.assertEquals(2 * 1024, totalPoolChunkArray.length);

    for (int i = 0; i < totalPoolChunkArray.length; i++) {
      Assert.assertEquals(i + 1, totalPoolChunkArray[i].getChunkId());
    }

    Assert.assertEquals(2 * 1024, allocatorHandler.getChunkCounter());
    Assert.assertEquals(1, allocatorHandler.getRequestCounter());
    Assert.assertEquals(0, allocatorHandler.getExtraRequestCounter());
    Assert.assertEquals(0, allocatorHandler.getExtraChunkCapacityUsed().get());

    IChunk hugeChunk = allocatorHandler.allocate(4 * 1024 + 1);
    Assert.assertTrue(hugeChunk instanceof HugeOnHeapChunk);
    Assert.assertFalse(hugeChunk.isPooledChunk());
    Assert.assertEquals(HeapMode.onHeap, hugeChunk.getHeapMode());
    Assert.assertEquals(4 * 1024 + 1, hugeChunk.getLimit());
    Assert.assertEquals(2 * 1024 + 2, hugeChunk.getChunkId());

    Assert.assertEquals(2 * 1024 - 1, chunkQueue.size());
    Assert.assertEquals(2 * 1024, totalPoolChunkArray.length);

    Assert.assertEquals(2 * 1024, allocatorHandler.getChunkCounter());
    Assert.assertEquals(2, allocatorHandler.getRequestCounter());
    Assert.assertEquals(1, allocatorHandler.getExtraRequestCounter());
    Assert.assertEquals(4097, allocatorHandler.getExtraChunkCapacityUsed().get());
    Assert.assertEquals(1, hugeChunkMap.size());
    Assert.assertEquals(hugeChunk, hugeChunkMap.get(hugeChunk.getChunkId()));

    System.out.println("chunk position=" + chunk.getPosition());
    System.out.println("chunk limit=" + chunk.getLimit());
    System.out.println("chunk BB position=" + chunk.getByteBuffer().position());
    System.out.println("chunk BB limit=" + chunk.getByteBuffer().limit());
    System.out.println("chunk BB capacity=" + chunk.getByteBuffer().capacity());
    allocatorHandler.putbackChunk(chunk);
    Assert.assertEquals(2 * 1024, chunkQueue.size());
    Assert.assertEquals(2 * 1024, totalPoolChunkArray.length);
    Assert.assertEquals(2 * 1024, allocatorHandler.getChunkCounter());
    Assert.assertEquals(2, allocatorHandler.getRequestCounter());
    Assert.assertEquals(1, allocatorHandler.getExtraRequestCounter());

    Assert.assertEquals(0, chunk.getPosition());
    Assert.assertEquals(0, chunk.getByteBuffer().position());
    Assert.assertEquals(4 * 1024, chunk.getByteBuffer().limit());
    Assert.assertEquals(4 * 1024, chunk.getLimit());
    Assert.assertEquals(1, chunk.getChunkId());
    System.out.println("chunk position=" + chunk.getPosition());
    System.out.println("chunk limit=" + chunk.getLimit());
    System.out.println("chunk BB position=" + chunk.getByteBuffer().position());
    System.out.println("chunk BB limit=" + chunk.getByteBuffer().limit());
    System.out.println("chunk BB capacity=" + chunk.getByteBuffer().capacity());

    allocatorHandler.putbackChunk(hugeChunk);
    Assert.assertEquals(2 * 1024, chunkQueue.size());
    Assert.assertEquals(2 * 1024, totalPoolChunkArray.length);
    Assert.assertEquals(2 * 1024, allocatorHandler.getChunkCounter());
    Assert.assertEquals(2, allocatorHandler.getRequestCounter());
    Assert.assertEquals(1, allocatorHandler.getExtraRequestCounter());
    Assert.assertEquals(0, hugeChunkMap.size());
    Assert.assertEquals(0, allocatorHandler.getExtraChunkCapacityUsed().get());

    AllocatorHandlerRegister.register(allocatorHandler);
    Assert.assertEquals(allocatorHandler, AllocatorHandlerRegister.getAllocatorHandler());

    Method methodgetExtraChunkId = CCSMapChunkPool.class.getDeclaredMethod("getExtraChunkId");
    methodgetExtraChunkId.setAccessible(true);
    long id = (long) methodgetExtraChunkId.invoke(allocatorHandler);
    Assert.assertEquals(2051, id);
    hugeChunkMap.put(2052L, mock(IChunk.class));
    hugeChunkMap.put(2053L, mock(IChunk.class));
    id = (long) methodgetExtraChunkId.invoke(allocatorHandler);
    Assert.assertEquals(2054, id);

    Field fieldExtraChunkIdGenerator =
        CCSMapChunkPool.class.getDeclaredField("extraChunkIdGenerator");
    fieldExtraChunkIdGenerator.setAccessible(true);
    AtomicLong hugeIdGen = (AtomicLong) fieldExtraChunkIdGenerator.get(allocatorHandler);
    hugeIdGen.set(Integer.MAX_VALUE - 2);
    id = (long) methodgetExtraChunkId.invoke(allocatorHandler);
    Assert.assertEquals(2050, id);
  }

  @Test
  public void testExaustedNormalChunk() throws Exception {
    AllocatorHandlerRegister.AllocatorHandlerBuilder builder =
        new AllocatorHandlerRegister.AllocatorHandlerBuilder();
    builder.setCapacity(8 * 1024);
    builder.setExtraChunkWarnCapacity(8 * 1024);
    builder.setChunkSize(4 * 1024);
    builder.setInitialCount(Integer.MAX_VALUE);
    builder.setHeapMode(HeapMode.offHeap);
    CCSMapChunkPool allocatorHandler = new CCSMapChunkPool(builder);

    Field chunkQueueField = CCSMapChunkPool.class.getDeclaredField("chunkQueue");
    chunkQueueField.setAccessible(true);
    @SuppressWarnings("unchecked") ConcurrentLinkedQueue<IChunk> chunkQueue =
        (ConcurrentLinkedQueue<IChunk>) chunkQueueField.get(allocatorHandler);
    Assert.assertEquals(2, chunkQueue.size());

    Field chunkMapField = CCSMapChunkPool.class.getDeclaredField("totalPoolChunkArray");
    chunkMapField.setAccessible(true);
    IChunk[] totalPoolChunkArray = (IChunk[]) chunkMapField.get(allocatorHandler);

    Map<Long, IChunk> hugeChunkMap = allocatorHandler.getExtraChunkMap();

    Assert.assertEquals(totalPoolChunkArray.length, chunkQueue.size());

    Assert.assertEquals(2, allocatorHandler.getChunkCounter());
    Assert.assertEquals(2, allocatorHandler.getMaxCount());
    Assert.assertEquals(0, allocatorHandler.getRequestCounter());
    Assert.assertEquals(0, allocatorHandler.getExtraRequestCounter());
    Assert.assertEquals(0, hugeChunkMap.size());

    IChunk chunk = allocatorHandler.allocate(4 * 1024 - 1);
    Assert.assertTrue(chunk.isPooledChunk());
    Assert.assertEquals(HeapMode.offHeap, chunk.getHeapMode());
    Assert.assertEquals(4 * 1024, chunk.getLimit());
    Assert.assertEquals(1, chunk.getChunkId());
    Assert.assertEquals(0, chunk.getPosition());

    Assert.assertEquals(0, hugeChunkMap.size());
    Assert.assertEquals(1, chunkQueue.size());
    Assert.assertEquals(2, totalPoolChunkArray.length);

    IChunk chunk2 = allocatorHandler.allocate(4 * 1024 - 2);
    Assert.assertTrue(chunk2.isPooledChunk());
    Assert.assertEquals(HeapMode.offHeap, chunk2.getHeapMode());
    Assert.assertEquals(4 * 1024, chunk2.getLimit());
    Assert.assertEquals(2, chunk2.getChunkId());
    Assert.assertEquals(0, chunk2.getPosition());

    Assert.assertEquals(0, hugeChunkMap.size());
    Assert.assertEquals(0, chunkQueue.size());
    Assert.assertEquals(2, totalPoolChunkArray.length);

    //exhausted

    IChunk chunk3 = allocatorHandler.allocate(4 * 1024 - 3);
    Assert.assertEquals("OnHeapChunk", chunk3.getClass().getSimpleName());
    Assert.assertFalse(chunk3.isPooledChunk());
    Assert.assertEquals(HeapMode.onHeap, chunk3.getHeapMode());
    Assert.assertEquals(4 * 1024, chunk3.getLimit());
    Assert.assertEquals(4, chunk3.getChunkId());

    Assert.assertEquals(1, hugeChunkMap.size());
    Assert.assertEquals(0, chunkQueue.size());
    Assert.assertEquals(2, totalPoolChunkArray.length);

    IChunk chunk4 = allocatorHandler.allocate(4 * 1024 - 4);
    Assert.assertEquals("OnHeapChunk", chunk4.getClass().getSimpleName());
    Assert.assertFalse(chunk4.isPooledChunk());
    Assert.assertEquals(HeapMode.onHeap, chunk4.getHeapMode());
    Assert.assertEquals(4 * 1024, chunk4.getLimit());
    Assert.assertEquals(5, chunk4.getChunkId());

    Assert.assertEquals(2, hugeChunkMap.size());
    Assert.assertEquals(0, chunkQueue.size());
    Assert.assertEquals(2, totalPoolChunkArray.length);

    allocatorHandler.putbackChunk(chunk4);
    Assert.assertEquals(1, hugeChunkMap.size());
    Assert.assertEquals(0, chunkQueue.size());
    Assert.assertEquals(2, totalPoolChunkArray.length);

    chunk4 = allocatorHandler.allocate(4 * 1024 - 4);
    Assert.assertEquals("OnHeapChunk", chunk4.getClass().getSimpleName());
    Assert.assertTrue(chunk4 instanceof OnHeapChunk);
    Assert.assertFalse(chunk4.isPooledChunk());
    Assert.assertEquals(HeapMode.onHeap, chunk4.getHeapMode());
    Assert.assertEquals(4 * 1024, chunk4.getLimit());
    Assert.assertEquals(6, chunk4.getChunkId());

    Assert.assertEquals(2, hugeChunkMap.size());
    Assert.assertEquals(0, chunkQueue.size());
    Assert.assertEquals(2, totalPoolChunkArray.length);

    //Huge exhausted
    allocatorHandler.allocate(4 * 1024 - 4);
  }
}