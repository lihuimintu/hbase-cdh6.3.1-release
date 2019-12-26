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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestByteBufferUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestByteBufferUtils.class);
  @Test
  public void testBBWriteAndRead() throws Exception {
    ByteBuffer bb = ByteBuffer.allocateDirect(4096);
    doTest(bb, 0);
    doTest(bb, 1);
    doTest(bb, 2);
    doTest(bb, 3);
    doTest(bb, 4);
    doTest(bb, 5);
    doTest(bb, 6);
    doTest(bb, 7);

    bb = ByteBuffer.allocate(4096);
    doTest(bb, 0);
    doTest(bb, 1);
    doTest(bb, 2);
    doTest(bb, 3);
    doTest(bb, 4);
    doTest(bb, 5);
    doTest(bb, 6);
    doTest(bb, 7);
  }

  private void doTest(ByteBuffer bb, int offset) {
    int positionOri = bb.position();
    ByteBufferUtils.putInt(bb, offset, 123);
    Assert.assertEquals(positionOri, bb.position());
    Assert.assertEquals(123, ByteBufferUtils.toInt(bb, offset));
    Assert.assertEquals(positionOri, bb.position());

    ByteBufferUtils.putLong(bb, offset + 4, 1234);
    Assert.assertEquals(positionOri, bb.position());
    Assert.assertEquals(1234, ByteBufferUtils.toLong(bb, offset + 4));
    Assert.assertEquals(positionOri, bb.position());

    Assert.assertEquals(123, ByteBufferUtils.toInt(bb, offset));
    Assert.assertEquals(positionOri, bb.position());
  }

  @Test
  public void testBBCAS() throws Exception {
    ByteBuffer bb = ByteBuffer.allocate(4096);
    doTestBBCAS(bb, 0);
    doTestBBCAS(bb, 12);
    doTestBBCAS(bb, 25);

    bb = ByteBuffer.allocateDirect(4096);
    ByteBufferUtils.toLong(bb, 0);
    Assert.assertEquals(0, ByteBufferUtils.toLong(bb, 0));

    doTestBBCAS(bb, 0);
    doTestBBCAS(bb, 12);
    doTestBBCAS(bb, 25);
  }

  private void doTestBBCAS(ByteBuffer bb, int offset) {

    int positionOri = bb.position();
    Assert.assertTrue(!ByteBufferUtils.compareAndSetInt(bb, offset, 11, 101));

    Assert.assertEquals(positionOri, bb.position());
    int v0 = ByteBufferUtils.toInt(bb, offset);
    Assert.assertEquals(0, v0);
    Assert.assertEquals(positionOri, bb.position());

    Assert.assertTrue(ByteBufferUtils.compareAndSetInt(bb, offset, 0, 101));
    Assert.assertEquals(positionOri, bb.position());

    int v1 = ByteBufferUtils.toInt(bb, offset);
    Assert.assertEquals(101, v1);
    Assert.assertEquals(positionOri, bb.position());

    Assert.assertTrue(ByteBufferUtils.compareAndSetInt(bb, offset, v1, 102));
    Assert.assertEquals(positionOri, bb.position());

    int v2 = ByteBufferUtils.toInt(bb, offset);
    Assert.assertEquals(102, v2);
    Assert.assertEquals(positionOri, bb.position());

    Assert.assertTrue(!ByteBufferUtils.compareAndSetInt(bb, offset, 11, 103));

    int v3 = ByteBufferUtils.toInt(bb, offset);
    Assert.assertEquals(v3, 102);
    Assert.assertEquals(positionOri, bb.position());

    Assert.assertTrue(!ByteBufferUtils.compareAndSetLong(bb, offset + 4, 11, 101));

    long value0 = ByteBufferUtils.toLong(bb, offset + 4);
    Assert.assertEquals(0, value0);
    Assert.assertEquals(positionOri, bb.position());

    Assert.assertTrue(ByteBufferUtils.compareAndSetLong(bb, offset + 4, 0, 101));
    Assert.assertEquals(positionOri, bb.position());

    long value1 = ByteBufferUtils.toLong(bb, offset + 4);
    Assert.assertEquals(101, value1);
    Assert.assertEquals(positionOri, bb.position());

    Assert.assertTrue(ByteBufferUtils.compareAndSetLong(bb, offset + 4, value1, 102));
    Assert.assertEquals(positionOri, bb.position());

    long value2 = ByteBufferUtils.toLong(bb, offset + 4);
    Assert.assertEquals(102, value2);
    Assert.assertEquals(positionOri, bb.position());

    Assert.assertTrue(!ByteBufferUtils.compareAndSetLong(bb, offset + 4, 11, 103));

    long value3 = ByteBufferUtils.toLong(bb, offset + 4);
    Assert.assertEquals(102, value3);
    Assert.assertEquals(positionOri, bb.position());

    ByteBufferUtils.compareTo(bb, 0, 4, bb, 0, 4);
    Assert.assertEquals(positionOri, bb.position());

    ByteBuffer bb2 = ByteBuffer.allocate(4096);
    ByteBufferUtils.copyFromBufferToBuffer(bb, bb2, 0, 0, 4);
    Assert.assertEquals(positionOri, bb.position());
  }

}