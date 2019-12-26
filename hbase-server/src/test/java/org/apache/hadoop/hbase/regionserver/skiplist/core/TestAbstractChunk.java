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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestAbstractChunk {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAbstractChunk.class);
  @Test
  public void testNormal() throws Exception {
    int len = 4 * 1024 * 1024;
    OnHeapChunk chunk = new OnHeapChunk(1234, len);
    Assert.assertEquals(1234, chunk.getChunkId());
    Assert.assertEquals(0, chunk.getPosition());
    Assert.assertEquals(len, chunk.getLimit());
    Assert.assertEquals(0, chunk.getUsedPercent());
    Assert.assertTrue(chunk.isPooledChunk());

    Assert.assertEquals(len, chunk.occupancy());
    //
    long position1 = chunk.allocate(1023);
    Assert.assertEquals(0, position1);
    long position2 = chunk.allocate(1025);
    Assert.assertEquals(1024, position2);
    Assert.assertEquals(1024 + 1032, chunk.getPosition());
    chunk.reclaim(position1, 1025);
    Assert.assertEquals(len - 1024 - 1032 + 1025, chunk.occupancy());

    //test usedPercent
    chunk.setLastAllocateStartTime();
    Thread.sleep(1000);
    chunk.setLastAllocateEndTime();
    Thread.sleep(1100);
    Assert.assertTrue(chunk.getUsedPercent() > 0);
    Assert.assertTrue(chunk.getUsedPercent() < 50);

    String expectedException = "WANT_EXCEPTION";
    try {
      chunk.setLastAllocateEndTime();
    } catch (Exception e) {
      expectedException = e.getMessage();
    }
    Assert.assertEquals("chunk stat error. no allocateTime, cur=-1", expectedException);

    chunk.setLastAllocateStartTime();
    expectedException = "WANT_EXCEPTION";
    try {
      chunk.setLastAllocateStartTime();
    } catch (Exception e) {
      expectedException = e.getMessage();
    }
    Assert.assertTrue(expectedException.contains("chunk stat error. existed allocateTime"));

    ByteBuffer bb = chunk.getByteBuffer();
    Assert.assertEquals(len, bb.limit());
    Assert.assertEquals(len, bb.capacity());
    Assert.assertEquals(0, bb.position());

    int len2 = 4096;
    ByteBuffer bb2 = chunk.asSubByteBuffer(100, len2);
    Assert.assertEquals(len2, bb2.limit());
    Assert.assertEquals(len2, bb2.capacity());
    Assert.assertEquals(0, bb2.position());

    Assert.assertEquals(1234, chunk.hashCode());

    OnHeapChunk chunk2 = new OnHeapChunk(1234, len);
    //As long as chunkId is same, Chunk is the same
    Assert.assertEquals(chunk, chunk2);
    Assert.assertEquals(len, chunk.getByteBuffer().limit());

    OnHeapChunk chunk3 = new OnHeapChunk(1235, len, false);
    Assert.assertFalse(chunk3.isPooledChunk());
  }

  @Test
  public void testHugeChunk() throws Exception {
    int len = 1023;
    HugeOnHeapChunk chunk = new HugeOnHeapChunk(1234, len);
    Assert.assertFalse(chunk.isPooledChunk());

    String expectException = "WANT_EXCEPTION";
    try {
      chunk.allocate(1233);
    } catch (Exception e) {
      expectException = e.getMessage();
    }
    Assert.assertEquals("HugeChunk's capacity not fit the nodeLen(1233).", expectException);

    chunk.allocate(1023);
    Assert.assertEquals(len, chunk.getPosition());

    expectException = "WANT_EXCEPTION";
    try {
      chunk.allocate(1023);
    } catch (Exception e) {
      expectException = e.getMessage();
    }
    Assert.assertEquals("HugeChunk only use once, cur offset=1023", expectException);
  }

  @SuppressWarnings("AssertionFailureIgnored")
  @Test
  public void testConcurrentWriteOffHeap() throws Exception {
    int len = 4 * 1024 * 1024;
    OffHeapChunk chunk = new OffHeapChunk(1234, len);

    int concurrent = 50;
    ByteBuffer[] bbArray = new ByteBuffer[concurrent];

    for (int i = 0; i < concurrent; i++) {
      bbArray[i] = chunk.asSubByteBuffer(i * 2049, 1023);
    }

    AtomicBoolean hasError = new AtomicBoolean(false);
    Thread[] ths = new Thread[concurrent];

    for (int i = 0; i < concurrent; i++) {
      final int thid = i;
      ths[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          ByteBuffer bb = ByteBuffer.allocate(13);
          bb.put((byte) thid);
          bb.putInt(thid);
          bb.putLong(thid);
          bb.flip();
          try {
            Assert.assertEquals(0, bbArray[thid].position());
            Thread.sleep(1000);
            bbArray[thid].put((byte) thid);
            Assert.assertEquals(1, bbArray[thid].position());
            Thread.sleep(1000);
            bbArray[thid].putInt(thid);
            Assert.assertEquals(1 + 4, bbArray[thid].position());
            Thread.sleep(1000);
            bbArray[thid].putLong((long) thid);
            Assert.assertEquals(1 + 4 + 8, bbArray[thid].position());
            Thread.sleep(1000);
            bbArray[thid].put(bb);
            Assert.assertEquals(1 + 4 + 8 + 13, bbArray[thid].position());
          } catch (Throwable e) {
            e.printStackTrace();
            hasError.set(true);
          }
        }
      });
    }

    for (int j = 0; j < concurrent; j++) {
      ths[j].start();
    }

    for (int j = 0; j < concurrent; j++) {
      ths[j].join();
    }

    Assert.assertTrue(!hasError.get());

    for (int j = 0; j < concurrent; j++) {
      bbArray[j].rewind();
      Assert.assertEquals(0, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].get());
      Assert.assertEquals(1, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getInt());
      Assert.assertEquals(1 + 4, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getLong());
      Assert.assertEquals(1 + 4 + 8, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].get());
      Assert.assertEquals(1 + 4 + 8 + 1, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getInt());
      Assert.assertEquals(1 + 4 + 8 + 1 + 4, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getLong());
      Assert.assertEquals(1 + 4 + 8 + 1 + 4 + 8, bbArray[j].position());
    }

    ByteBuffer bb = chunk.getByteBuffer();
    bb.rewind();
    for (int j = 0; j < concurrent; j++) {
      bb.position(j * 2049);
      Assert.assertEquals(j, bb.get());
      Assert.assertEquals(j, bb.getInt());
      Assert.assertEquals(j, bb.getLong());
      Assert.assertEquals(j, bb.get());
      Assert.assertEquals(j, bb.getInt());
      Assert.assertEquals(j, bb.getLong());
    }
  }

  @SuppressWarnings("AssertionFailureIgnored")
  @Test
  public void testConcurrentWriteOnHeap() throws Exception {
    int len = 4 * 1024 * 1024;
    OnHeapChunk chunk = new OnHeapChunk(1234, len);

    int concurrent = 50;
    ByteBuffer[] bbArray = new ByteBuffer[concurrent];

    for (int i = 0; i < concurrent; i++) {
      bbArray[i] = chunk.asSubByteBuffer(i * 2049, 1023);
    }

    AtomicBoolean hasError = new AtomicBoolean(false);
    Thread[] ths = new Thread[concurrent];

    for (int i = 0; i < concurrent; i++) {
      final int thid = i;
      ths[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          ByteBuffer bb = ByteBuffer.allocate(13);
          bb.put((byte) thid);
          bb.putInt(thid);
          bb.putLong(thid);
          bb.flip();
          try {
            Assert.assertEquals(0, bbArray[thid].position());
            Thread.sleep(100);
            bbArray[thid].put((byte) thid);
            Assert.assertEquals(1, bbArray[thid].position());
            Thread.sleep(100);
            bbArray[thid].putInt(thid);
            Assert.assertEquals(1 + 4, bbArray[thid].position());
            Thread.sleep(100);
            bbArray[thid].putLong((long) thid);
            Assert.assertEquals(1 + 4 + 8, bbArray[thid].position());
            Thread.sleep(100);
            bbArray[thid].put(bb);
            Assert.assertEquals(1 + 4 + 8 + 13, bbArray[thid].position());
          } catch (Throwable e) {
            e.printStackTrace();
            hasError.set(true);
          }
        }
      });
    }

    for (int j = 0; j < concurrent; j++) {
      ths[j].start();
    }

    for (int j = 0; j < concurrent; j++) {
      ths[j].join();
    }

    Assert.assertTrue(!hasError.get());

    for (int j = 0; j < concurrent; j++) {
      bbArray[j].rewind();
      Assert.assertEquals(0, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].get());
      Assert.assertEquals(1, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getInt());
      Assert.assertEquals(1 + 4, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getLong());
      Assert.assertEquals(1 + 4 + 8, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].get());
      Assert.assertEquals(1 + 4 + 8 + 1, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getInt());
      Assert.assertEquals(1 + 4 + 8 + 1 + 4, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getLong());
      Assert.assertEquals(1 + 4 + 8 + 1 + 4 + 8, bbArray[j].position());
    }

    ByteBuffer bb = chunk.getByteBuffer();
    bb.rewind();
    for (int j = 0; j < concurrent; j++) {
      bb.position(j * 2049);
      Assert.assertEquals(j, bb.get());
      Assert.assertEquals(j, bb.getInt());
      Assert.assertEquals(j, bb.getLong());
      Assert.assertEquals(j, bb.get());
      Assert.assertEquals(j, bb.getInt());
      Assert.assertEquals(j, bb.getLong());
    }
  }
}