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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.TestBaseTwinCCSMap.newBaseTwinCCSMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCCSMap {
  private final Random random = new Random(7);
  private byte[] startKey = new byte[4];
  private byte[] endKey = new byte[4];

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCCSMap.class);

  @Before
  public void setUp() throws Exception {
    Bytes.putInt(startKey, 0, 8);
    Bytes.putInt(endKey, 0, 28);
  }

  @Test
  public void testCCSMapBasic() throws Exception {
    NavigableMap<byte[], byte[]> testCCSMap = MockBaseCCSMap.newMockBaseCCSMap(HeapMode.onHeap);

    Assert.assertArrayEquals(null, testCCSMap.firstKey());
    Assert.assertArrayEquals(null, testCCSMap.lastKey());

    int keyLenght = 50;
    byte[] bytes = new byte[keyLenght];
    random.nextBytes(bytes);
    testCCSMap.put(bytes, bytes);

    Assert.assertEquals(1, testCCSMap.size());

    byte[] dst = testCCSMap.get(bytes);
    Assert.assertArrayEquals(bytes, dst);

    Set<Map.Entry<byte[], byte[]>> entrySet = testCCSMap.entrySet();
    Assert.assertEquals(1, entrySet.size());
    Map.Entry<byte[], byte[]> entry = entrySet.iterator().next();
    Assert.assertArrayEquals(bytes, entry.getKey());
    Assert.assertArrayEquals(bytes, entry.getValue());

    Assert.assertArrayEquals(bytes, testCCSMap.firstKey());
    Assert.assertArrayEquals(bytes, testCCSMap.lastKey());

    byte[] rm = testCCSMap.remove(bytes);
    Assert.assertArrayEquals(bytes, rm);

    Assert.assertEquals(null, testCCSMap.get(bytes));

    Assert.assertArrayEquals(null, testCCSMap.firstKey());
    Assert.assertArrayEquals(null, testCCSMap.lastKey());
  }

  @Test
  public void testBaseTwinCCSMap() throws Exception {
    int keyLenght = 50;
    final NavigableMap<byte[], byte[]> testMap = newBaseTwinCCSMap(HeapMode.onHeap);

    Assert.assertArrayEquals(null, testMap.firstKey());
    Assert.assertArrayEquals(null, testMap.lastKey());

    byte[] bytes = new byte[keyLenght];
    random.nextBytes(bytes);
    testMap.put(bytes, bytes);

    Assert.assertEquals(1, testMap.size());

    byte[] dst = testMap.get(bytes);
    Assert.assertArrayEquals(bytes, dst);

    Assert.assertArrayEquals(bytes, testMap.firstKey());
    Assert.assertArrayEquals(bytes, testMap.lastKey());

    byte[] rm = testMap.remove(bytes);
    Assert.assertArrayEquals(bytes, rm);

    Assert.assertEquals(null, testMap.get(bytes));
    Assert.assertArrayEquals(null, testMap.firstKey());
    Assert.assertArrayEquals(null, testMap.lastKey());
  }

  @Test
  public void testCCSLBasicMapSubMapWhenMapIsEmpty() throws Exception {
    final NavigableMap<byte[], byte[]> testMap = MockBaseCCSMap.newMockBaseCCSMap(HeapMode.onHeap);
    //map is empty
    Assert.assertEquals(null, testMap.firstKey());
    Assert.assertEquals(null, testMap.lastKey());
    Assert.assertEquals(null, testMap.lowerKey(startKey));
    Assert.assertEquals(null, testMap.floorKey(startKey));
    Assert.assertEquals(null, testMap.ceilingKey(startKey));
    Assert.assertEquals(null, testMap.higherKey(startKey));

    Assert.assertEquals(null, testMap.firstEntry());
    Assert.assertEquals(null, testMap.lastEntry());
    Assert.assertEquals(null, testMap.lowerEntry(startKey));
    Assert.assertEquals(null, testMap.floorEntry(startKey));
    Assert.assertEquals(null, testMap.ceilingEntry(startKey));
    Assert.assertEquals(null, testMap.higherEntry(startKey));
  }

  @Test
  public void testCCSLBasicMapSubMapWhenMapIsNotEmpty() throws Exception {
    final NavigableMap<byte[], byte[]> testMap = MockBaseCCSMap.newMockBaseCCSMap(HeapMode.onHeap);

    //put some data
    for (int i = 0; i < 50; i++) {
      byte[] bb = new byte[4];
      Bytes.putInt(bb, 0, (i + 1) * 2);
      testMap.put(bb, bb);
    }

    Assert.assertEquals(50, testMap.size());
    Assert.assertEquals(2, Bytes.toInt(testMap.firstKey()));
    Assert.assertEquals(100, Bytes.toInt(testMap.lastKey()));
    Assert.assertEquals(6, Bytes.toInt(testMap.lowerKey(startKey)));
    Assert.assertEquals(8, Bytes.toInt(testMap.floorKey(startKey)));
    Assert.assertEquals(8, Bytes.toInt(testMap.ceilingKey(startKey)));
    Assert.assertEquals(28, Bytes.toInt(testMap.ceilingKey(endKey)));
    Assert.assertEquals(30, Bytes.toInt(testMap.higherKey(endKey)));

    Assert.assertEquals(2, Bytes.toInt(testMap.firstEntry().getKey()));
    Assert.assertEquals(100, Bytes.toInt(testMap.lastEntry().getKey()));
    Assert.assertEquals(6, Bytes.toInt(testMap.lowerEntry(startKey).getKey()));
    Assert.assertEquals(8, Bytes.toInt(testMap.floorEntry(startKey).getKey()));
    Assert.assertEquals(8, Bytes.toInt(testMap.ceilingEntry(startKey).getKey()));
    Assert.assertEquals(28, Bytes.toInt(testMap.ceilingEntry(endKey).getKey()));
    Assert.assertEquals(30, Bytes.toInt(testMap.higherEntry(endKey).getKey()));

    final NavigableMap<byte[], byte[]> headMap1 = testMap.headMap(endKey, false);
    Assert.assertEquals(13, headMap1.size());
    Assert.assertEquals(2, Bytes.toInt(headMap1.firstKey()));
    Assert.assertEquals(26, Bytes.toInt(headMap1.lastKey()));

    final NavigableMap<byte[], byte[]> headMap2 = testMap.headMap(endKey, true);
    Assert.assertEquals(14, headMap2.size());
    Assert.assertEquals(2, Bytes.toInt(headMap2.firstKey()));
    Assert.assertEquals(28, Bytes.toInt(headMap2.lastKey()));

    final NavigableMap<byte[], byte[]> tailMap1 = testMap.tailMap(startKey, false);
    Assert.assertEquals(46, tailMap1.size());
    Assert.assertEquals(10, Bytes.toInt(tailMap1.firstKey()));
    Assert.assertEquals(100, Bytes.toInt(tailMap1.lastKey()));

    final NavigableMap<byte[], byte[]> tailMap2 = testMap.tailMap(startKey, true);
    Assert.assertEquals(47, tailMap2.size());
    Assert.assertEquals(8, Bytes.toInt(tailMap2.firstKey()));
    Assert.assertEquals(100, Bytes.toInt(tailMap2.lastKey()));

    Set<Map.Entry<byte[], byte[]>> allEntrySet = testMap.entrySet();
    Collection<byte[]> allValues = testMap.values();
    Set<byte[]> allKeys = testMap.keySet();

    Assert.assertEquals(50, allEntrySet.size());
    Assert.assertEquals(50, allValues.size());
    Assert.assertEquals(50, allKeys.size());

    int i = 0;
    for (Map.Entry<byte[], byte[]> entry : testMap.entrySet()) {
      Assert.assertEquals((i + 1) * 2, Bytes.toInt(entry.getKey()));
      i++;
    }

    i = 0;
    for (byte[] value : testMap.values()) {
      Assert.assertEquals((i + 1) * 2, Bytes.toInt(value));
      i++;
    }

    i = 0;
    for (byte[] value : testMap.keySet()) {
      Assert.assertEquals((i + 1) * 2, Bytes.toInt(value));
      i++;
    }

    Assert.assertEquals(13, headMap1.entrySet().size());
    Assert.assertEquals(13, headMap1.values().size());
    Assert.assertEquals(13, headMap1.keySet().size());

    i = 0;
    for (Map.Entry<byte[], byte[]> entry : headMap1.entrySet()) {
      Assert.assertEquals((i + 1) * 2, Bytes.toInt(entry.getKey()));
      i++;
    }

    i = 0;
    for (byte[] value : headMap1.values()) {
      Assert.assertEquals((i + 1) * 2, Bytes.toInt(value));
      i++;
    }

    i = 0;
    for (byte[] value : headMap1.keySet()) {
      Assert.assertEquals((i + 1) * 2, Bytes.toInt(value));
      i++;
    }
    doTestAfterRemoveHead(testMap, headMap1);
    doTestAfterRemoveTail(testMap, headMap1, tailMap1);
  }

  private void doTestAfterRemoveTail(NavigableMap<byte[], byte[]> testMap,
                                      NavigableMap<byte[], byte[]> headMap1,
                                      NavigableMap<byte[], byte[]> tailMap1) {
    int i;//remove the tail

    Iterator<byte[]> tailIterator = tailMap1.keySet().iterator();
    i = 0;
    while (tailIterator.hasNext()) {
      Assert.assertEquals((i + 5) * 2, Bytes.toInt(tailIterator.next()));
      if (i == 45) {
        //remove the tail
        tailIterator.remove();
      }
      i++;
    }

    Assert.assertEquals(12, headMap1.entrySet().size());
    Assert.assertEquals(12, headMap1.values().size());
    Assert.assertEquals(12, headMap1.keySet().size());

    Assert.assertEquals(48, testMap.entrySet().size());
    Assert.assertEquals(48, testMap.values().size());
    Assert.assertEquals(48, testMap.keySet().size());

    Assert.assertEquals(45, tailMap1.entrySet().size());
    Assert.assertEquals(45, tailMap1.values().size());
    Assert.assertEquals(45, tailMap1.keySet().size());

    Assert.assertEquals(4, Bytes.toInt(headMap1.firstKey()));
    Assert.assertEquals(26, Bytes.toInt(headMap1.lastKey()));

    Assert.assertEquals(4, Bytes.toInt(testMap.firstKey()));
    Assert.assertEquals(98, Bytes.toInt(testMap.lastKey()));

    Assert.assertEquals(45, tailMap1.size());
    Assert.assertEquals(10, Bytes.toInt(tailMap1.firstKey()));
    Assert.assertEquals(98, Bytes.toInt(tailMap1.lastKey()));
  }

  private void doTestAfterRemoveHead(NavigableMap<byte[], byte[]> testMap,
                                      NavigableMap<byte[], byte[]> headMap1) {
    int i;//remove Head
    Iterator<byte[]> headIterable = headMap1.keySet().iterator();
    i = 0;
    while (headIterable.hasNext()) {
      Assert.assertEquals((i + 1) * 2, Bytes.toInt(headIterable.next()));
      if (i == 0) {
        //remove Head
        headIterable.remove();
      }
      i++;
    }

    Assert.assertEquals(12, headMap1.entrySet().size());
    Assert.assertEquals(12, headMap1.values().size());
    Assert.assertEquals(12, headMap1.keySet().size());

    Assert.assertEquals(49, testMap.entrySet().size());
    Assert.assertEquals(49, testMap.values().size());
    Assert.assertEquals(49, testMap.keySet().size());

    Assert.assertEquals(4, Bytes.toInt(headMap1.firstKey()));
    Assert.assertEquals(26, Bytes.toInt(headMap1.lastKey()));

    Assert.assertEquals(4, Bytes.toInt(testMap.firstKey()));
    Assert.assertEquals(100, Bytes.toInt(testMap.lastKey()));

    i = 0;
    for (Map.Entry<byte[], byte[]> entry : headMap1.entrySet()) {
      Assert.assertEquals((i + 2) * 2, Bytes.toInt(entry.getKey()));
      i++;
    }

    i = 0;
    for (byte[] value : headMap1.values()) {
      Assert.assertEquals((i + 2) * 2, Bytes.toInt(value));
      i++;
    }

    i = 0;
    for (byte[] value : headMap1.keySet()) {
      Assert.assertEquals((i + 2) * 2, Bytes.toInt(value));
      i++;
    }
  }

  @Test
  public void testCCSMapTwinSubMap() throws Exception {
    byte[] startKey = new byte[4];
    byte[] endKey = new byte[4];

    Bytes.putInt(startKey, 0, 8);
    Bytes.putInt(endKey, 0, 28);

    final NavigableMap<byte[], byte[]> testMap = newBaseTwinCCSMap(HeapMode.onHeap);

    Assert.assertEquals(null, testMap.firstKey());
    Assert.assertEquals(null, testMap.lastKey());
    Assert.assertEquals(null, testMap.lowerKey(startKey));
    Assert.assertEquals(null, testMap.floorKey(startKey));
    Assert.assertEquals(null, testMap.ceilingKey(startKey));
    Assert.assertEquals(null, testMap.higherKey(startKey));

    Assert.assertEquals(null, testMap.firstEntry());
    Assert.assertEquals(null, testMap.lastEntry());
    Assert.assertEquals(null, testMap.lowerEntry(startKey));
    Assert.assertEquals(null, testMap.floorEntry(startKey));
    Assert.assertEquals(null, testMap.ceilingEntry(startKey));
    Assert.assertEquals(null, testMap.higherEntry(startKey));

    for (int i = 0; i < 50; i++) {
      byte[] bb = new byte[4];
      Bytes.putInt(bb, 0, (i + 1) * 2);
      testMap.put(bb, bb);
    }

    Assert.assertEquals(50, testMap.size());
    Assert.assertEquals(2, Bytes.toInt(testMap.firstKey()));
    Assert.assertEquals(100, Bytes.toInt(testMap.lastKey()));
    Assert.assertEquals(6, Bytes.toInt(testMap.lowerKey(startKey)));
    Assert.assertEquals(8, Bytes.toInt(testMap.floorKey(startKey)));
    Assert.assertEquals(8, Bytes.toInt(testMap.ceilingKey(startKey)));
    Assert.assertEquals(28, Bytes.toInt(testMap.ceilingKey(endKey)));
    Assert.assertEquals(30, Bytes.toInt(testMap.higherKey(endKey)));

    Assert.assertEquals(2, Bytes.toInt(testMap.firstEntry().getKey()));
    Assert.assertEquals(100, Bytes.toInt(testMap.lastEntry().getKey()));
    Assert.assertEquals(6, Bytes.toInt(testMap.lowerEntry(startKey).getKey()));
    Assert.assertEquals(8, Bytes.toInt(testMap.floorEntry(startKey).getKey()));
    Assert.assertEquals(8, Bytes.toInt(testMap.ceilingEntry(startKey).getKey()));
    Assert.assertEquals(28, Bytes.toInt(testMap.ceilingEntry(endKey).getKey()));
    Assert.assertEquals(30, Bytes.toInt(testMap.higherEntry(endKey).getKey()));

    final NavigableMap<byte[], byte[]> headMap1 = testMap.headMap(endKey, false);
    Assert.assertEquals(13, headMap1.size());
    Assert.assertEquals(2, Bytes.toInt(headMap1.firstKey()));
    Assert.assertEquals(26, Bytes.toInt(headMap1.lastKey()));

    final NavigableMap<byte[], byte[]> headMap2 = testMap.headMap(endKey, true);
    Assert.assertEquals(14, headMap2.size());
    Assert.assertEquals(2, Bytes.toInt(headMap2.firstKey()));
    Assert.assertEquals(28, Bytes.toInt(headMap2.lastKey()));

    final NavigableMap<byte[], byte[]> tailMap1 = testMap.tailMap(startKey, false);
    Assert.assertEquals(46, tailMap1.size());
    Assert.assertEquals(10, Bytes.toInt(tailMap1.firstKey()));
    Assert.assertEquals(100, Bytes.toInt(tailMap1.lastKey()));

    final NavigableMap<byte[], byte[]> tailMap2 = testMap.tailMap(startKey, true);
    Assert.assertEquals(47, tailMap2.size());
    Assert.assertEquals(8, Bytes.toInt(tailMap2.firstKey()));
    Assert.assertEquals(100, Bytes.toInt(tailMap2.lastKey()));

  }

}