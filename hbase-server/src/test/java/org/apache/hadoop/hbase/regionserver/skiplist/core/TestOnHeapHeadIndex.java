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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.DEFAULT_LEVEL;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.FOUR_BYTES_MARK;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NIL_NODE_ID;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestOnHeapHeadIndex {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOnHeapHeadIndex.class);

  @Test
  public void testNormal() throws Exception {

    OnHeapIndexHeader heapHeadIndex = new OnHeapIndexHeader();
    Assert.assertEquals(1, heapHeadIndex.getLevel());
    Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelNodeNext().size());
    int l = 0;
    for (long index : heapHeadIndex.getLevelNodeNext()) {
      Assert.assertEquals(NIL_NODE_ID, index);
      if (l + 1 <= heapHeadIndex.getLevel()) {
        Assert.assertEquals(NIL_NODE_ID, heapHeadIndex.getLevelNextNodeId(l + 1));
      }
      l++;
    }

    //update level 1, unsigned test
    long offsetLevel1 = 0xFFFFFFFFL;
    long chunkIdLevel1 = 0xFFFFFFFFL;
    heapHeadIndex.updateLevel(1);

    Assert.assertEquals(1, heapHeadIndex.getLevel());
    Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelNodeNext().size());

    long node1 = getNodeId(chunkIdLevel1, offsetLevel1);
    Assert.assertTrue(heapHeadIndex.casUpdateLevelNextNodeId(1, NIL_NODE_ID, node1));
    Assert.assertFalse(heapHeadIndex.casUpdateLevelNextNodeId(1, NIL_NODE_ID, node1));

    for (int i = 0; i < heapHeadIndex.getLevelNodeNext().size(); i++) {
      if (i == 0) {
        long chunkIdOffset = heapHeadIndex.getLevelNodeNext().get(0);
        Assert.assertEquals(chunkIdLevel1, ((chunkIdOffset >>> 32) & FOUR_BYTES_MARK));
        Assert.assertEquals(offsetLevel1, (chunkIdOffset & FOUR_BYTES_MARK));
      } else {
        Assert.assertEquals(NIL_NODE_ID, heapHeadIndex.getLevelNodeNext().get(i).longValue());
      }
    }

    offsetLevel1 = 1;
    chunkIdLevel1 = 0xFFFFFFFEL;
    long node2 = getNodeId(chunkIdLevel1, offsetLevel1);
    Assert.assertTrue(heapHeadIndex.casUpdateLevelNextNodeId(1, node1, node2));

    Assert.assertEquals(1, heapHeadIndex.getLevel());
    Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelNodeNext().size());
    for (int i = 0; i < heapHeadIndex.getLevelNodeNext().size(); i++) {
      if (i == 0) {
        long chunkIdOffset = heapHeadIndex.getLevelNodeNext().get(0);
        Assert.assertEquals(chunkIdLevel1, ((chunkIdOffset >>> 32) & FOUR_BYTES_MARK));
        Assert.assertEquals(offsetLevel1, (chunkIdOffset & FOUR_BYTES_MARK));
      } else {
        Assert.assertEquals(NIL_NODE_ID, heapHeadIndex.getLevelNodeNext().get(i).longValue());
      }
    }

    offsetLevel1 = 0xFFFFFFF1L;
    chunkIdLevel1 = 1;
    long node3 = getNodeId(chunkIdLevel1, offsetLevel1);
    Assert.assertTrue(heapHeadIndex.casUpdateLevelNextNodeId(1, node2, node3));

    Assert.assertEquals(1, heapHeadIndex.getLevel());
    Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelNodeNext().size());
    for (int i = 0; i < heapHeadIndex.getLevelNodeNext().size(); i++) {
      if (i == 0) {
        long chunkIdOffset = heapHeadIndex.getLevelNodeNext().get(0);
        Assert.assertEquals(chunkIdLevel1, ((chunkIdOffset >>> 32) & FOUR_BYTES_MARK));
        Assert.assertEquals(offsetLevel1, (chunkIdOffset & FOUR_BYTES_MARK));
      } else {
        Assert.assertEquals(NIL_NODE_ID, heapHeadIndex.getLevelNodeNext().get(i).longValue());
      }
    }

    // try to grow by one level
    long offsetLevel2 = 23456;
    long chunkIdLevel2 = 12345;
    long node4 = getNodeId(chunkIdLevel2, offsetLevel2);
    heapHeadIndex.updateLevel(2);
    Assert.assertTrue(heapHeadIndex.casUpdateLevelNextNodeId(2, NIL_NODE_ID, node4));

    Assert.assertEquals(2, heapHeadIndex.getLevel());
    Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelNodeNext().size());
    for (int i = 0; i < heapHeadIndex.getLevelNodeNext().size(); i++) {
      if (i == 0) {
        long chunkIdOffset = heapHeadIndex.getLevelNodeNext().get(0);
        Assert.assertEquals(chunkIdLevel1, ((chunkIdOffset >>> 32) & FOUR_BYTES_MARK));
        Assert.assertEquals(offsetLevel1, (chunkIdOffset & FOUR_BYTES_MARK));
      } else if (i == 1) {
        long chunkIdOffset = heapHeadIndex.getLevelNodeNext().get(1);
        Assert.assertEquals(chunkIdLevel2, ((chunkIdOffset >>> 32) & FOUR_BYTES_MARK));
        Assert.assertEquals(offsetLevel2, (chunkIdOffset & FOUR_BYTES_MARK));
      } else {
        Assert.assertEquals(NIL_NODE_ID, heapHeadIndex.getLevelNodeNext().get(i).longValue());
      }
    }

    //error level
    String expectExceptionMsg = "WANT_EXCEPTION";
    try {
      heapHeadIndex.updateLevel(4);
    } catch (Exception e) {
      expectExceptionMsg = e.getMessage();
    }

    Assert.assertEquals("updateLevel can't skip. now level=2 ,dst level=4", expectExceptionMsg);

    //test access DEFAULT_LEVEL, heapHeadIndex expand the indexArray

    for (int i = 1; i < 34; i++) {
      heapHeadIndex.updateLevel(i);

    }

    Assert.assertEquals(33, heapHeadIndex.getLevel());
    Assert.assertEquals(DEFAULT_LEVEL * 2, heapHeadIndex.getLevelNodeNext().size());

    for (int i = 0; i < heapHeadIndex.getLevelNodeNext().size(); i++) {
      if (i == 0) {
        long chunkIdOffset = heapHeadIndex.getLevelNodeNext().get(0);
        Assert.assertEquals(chunkIdLevel1, ((chunkIdOffset >>> 32) & FOUR_BYTES_MARK));
        Assert.assertEquals(offsetLevel1, (chunkIdOffset & FOUR_BYTES_MARK));
      } else if (i == 1) {
        long chunkIdOffset = heapHeadIndex.getLevelNodeNext().get(1);
        Assert.assertEquals(chunkIdLevel2, ((chunkIdOffset >>> 32) & FOUR_BYTES_MARK));
        Assert.assertEquals(offsetLevel2, (chunkIdOffset & FOUR_BYTES_MARK));
      } else {
        Assert.assertEquals(NIL_NODE_ID, heapHeadIndex.getLevelNodeNext().get(i).longValue());
      }
    }
  }

  @Test
  public void testException() throws Exception {
    OnHeapIndexHeader heapHeadIndex = new OnHeapIndexHeader();
    //error level
    String expectExceptionMsg = "WANT_EXCEPTION";
    try {
      heapHeadIndex.updateLevel(3);
    } catch (Exception e) {
      expectExceptionMsg = e.getMessage();
    }

    Assert.assertEquals("updateLevel can't skip. now level=1 ,dst level=3", expectExceptionMsg);

    expectExceptionMsg = "WANT_EXCEPTION";
    try {
      heapHeadIndex.updateLevel(0);
    } catch (Exception e) {
      expectExceptionMsg = e.getMessage();
    }

    Assert.assertEquals("dst level less than 1.", expectExceptionMsg);

    expectExceptionMsg = "WANT_EXCEPTION";
    try {
      heapHeadIndex.updateLevel(256);
    } catch (Exception e) {
      expectExceptionMsg = e.getMessage();
    }

    Assert.assertEquals("dst level>255, It's unbelievable.", expectExceptionMsg);
  }

  private long getNodeId(long chunkId, long offset) {
    return ((chunkId & FOUR_BYTES_MARK) << 32) | (offset & FOUR_BYTES_MARK);
  }
}