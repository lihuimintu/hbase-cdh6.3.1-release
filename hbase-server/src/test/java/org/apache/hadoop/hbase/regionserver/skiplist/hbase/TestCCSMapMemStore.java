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
package org.apache.hadoop.hbase.regionserver.skiplist.hbase;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.skiplist.core.AllocatorHandlerRegister;
import org.apache.hadoop.hbase.regionserver.skiplist.core.CCSMapChunkPool;
import org.apache.hadoop.hbase.regionserver.skiplist.core.HeapMode;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCCSMapMemStore {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCCSMapMemStore.class);

  private static CCSMapMemStore buildCCSMapMemStore() {
    AllocatorHandlerRegister.AllocatorHandlerBuilder builder =
      new AllocatorHandlerRegister.AllocatorHandlerBuilder();
    builder.setCapacity(1024 * 1024 * 1024);
    builder.setChunkSize(8 * 1024 * 1024);
    builder.setInitialCount(Integer.MAX_VALUE);
    builder.setHeapMode(HeapMode.offHeap);
    CCSMapChunkPool allocatorHandler = new CCSMapChunkPool(builder);
    AllocatorHandlerRegister.register(allocatorHandler);
    AllocatorHandlerRegister.AllocatorHandlerBuilder.setINSTANCE(builder);

    Configuration conf = HBaseConfiguration.create();
    return new CCSMapMemStore(conf, CellComparatorImpl.COMPARATOR);
  }

  @Test
  public void testNormal() throws Exception {
    CCSMapMemStore ms = buildCCSMapMemStore();
    Cell cell1 = TestCellCCSMap.buildCell(1234, 8, 2345, 1);
    ms.add(cell1, null);
    List<KeyValueScanner> xx = ms.getScanners(2);
    int count = 0;
    for (KeyValueScanner scanner : xx) {

      Cell cell2 = scanner.next();
      if (cell2 != null) {
        Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(cell1, cell2));
        count++;
      }
    }
    Assert.assertEquals(1, count);

    System.out.println(ms.getFlushableSize());
  }
}