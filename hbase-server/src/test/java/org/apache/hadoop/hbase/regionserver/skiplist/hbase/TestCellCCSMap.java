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

import static org.apache.hadoop.hbase.regionserver.skiplist.hbase.CellCCSMap.CCSMAP_COMPARATOR_KEY;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.NoTagsByteBufferKeyValue;
import org.apache.hadoop.hbase.regionserver.skiplist.core.AllocatorHandlerRegister;
import org.apache.hadoop.hbase.regionserver.skiplist.core.CCSMapChunkPool;
import org.apache.hadoop.hbase.regionserver.skiplist.core.HeapMode;
import org.apache.hadoop.hbase.regionserver.skiplist.core.INodeComparator;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCellCCSMap {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCellCCSMap.class);

  private Cell keyValue1;
  private Cell bbCell1;
  private Cell cellBigRow;
  private Cell cellSmallRow;
  private Cell cellBigF;
  private Cell cellSmallF;
  private Cell cellBigTs;
  private Cell cellSmallTs;
  private Cell cellBigSeqId;
  private Cell cellSmallSeqId;
  private Cell noExistedBigCell;
  private Cell noExistedSmallCell;
  private Map<Integer, Cell> orderMaps;
  private final ByteBuffer oriBB = ByteBuffer.allocate(1024);
  private final long seqId = 123456L;

  @Before
  public void setUp() throws Exception {
    int offset = 3;
    keyValue1 = buildCell(1234, 8, 2345, seqId);
    int len = ((KeyValue) keyValue1).getSerializedSize(true);
    bbCell1 = copyCellTo(keyValue1, oriBB, offset, len);

  }

  private void init(boolean returnByteBufferCell) {
    cellBigRow = buildCell(1235, 8, 2345, seqId, returnByteBufferCell);
    cellSmallRow = buildCell(1233, 8, 2345, seqId, returnByteBufferCell);

    cellBigF = buildCell(1234, 9, 2345, seqId, returnByteBufferCell);
    cellSmallF = buildCell(1234, 7, 2345, seqId, returnByteBufferCell);

    cellBigTs = buildCell(1234, 8, 2346, seqId, returnByteBufferCell);
    cellSmallTs = buildCell(1234, 8, 2344, seqId, returnByteBufferCell);

    cellBigSeqId = buildCell(1234, 8, 2345, seqId + 1, returnByteBufferCell);
    cellSmallSeqId = buildCell(1234, 8, 2345, seqId - 1, returnByteBufferCell);

    noExistedBigCell = buildCell(2000, 8, 2345, seqId, returnByteBufferCell);
    noExistedSmallCell = buildCell(1000, 8, 2345, seqId, returnByteBufferCell);

    //cellSmallRow < cellSmallF < cellBigTs < cellBigSeqId < keyvalue1 < cellSmallSeqId
    // < cellSmallTs < cellBigF < cellBigRow
    orderMaps = new HashMap<>();
    orderMaps.put(0, cellSmallRow);
    orderMaps.put(1, cellSmallF);
    orderMaps.put(2, cellBigTs);
    orderMaps.put(3, cellBigSeqId);
    orderMaps.put(4, keyValue1);
    orderMaps.put(5, cellSmallSeqId);
    orderMaps.put(6, cellSmallTs);
    orderMaps.put(7, cellBigF);
    orderMaps.put(8, cellBigRow);
  }

  static Cell buildCell(long rowValue, int id, long ts, long seqId) {
    return buildCell(rowValue, id, ts, seqId, false);
  }

  private static Cell buildCell(long rowValue, int id, long ts, long seqId,
                                 boolean returnByteBufferCell) {
    byte[] row = new byte[8];
    Bytes.putLong(row, 0, rowValue);
    byte[] fam = Bytes.toBytes("testfamily" + id);
    byte[] qf = Bytes.toBytes("testqualifier" + id);
    byte[] val = Bytes.toBytes("testval" + id);
    Cell keyValue = new KeyValue(row, fam, qf, ts, val);
    ((KeyValue) keyValue).setSequenceId(seqId);

    if (returnByteBufferCell) {
      ByteBuffer oriBB = ByteBuffer.allocate(1024);
      int offset = 1;
      int len = ((KeyValue) keyValue).getSerializedSize(true);
      return copyCellTo(keyValue, oriBB, offset, len);
    }
    return keyValue;
  }

  @Test
  public void testUtilsNotChangePosition() throws Exception {

    ByteBuffer buf = ByteBuffer.allocate(1024);
    Assert.assertEquals(0, buf.position());
    Assert.assertEquals(1024, buf.limit());
    Assert.assertEquals(1024, buf.capacity());
    int offset = 2;
    System.out.println("==>" + ((KeyValue) keyValue1).getSerializedSize(true));
    System.out.println("==>" + ((KeyValue) keyValue1).getSerializedSize(false));
    System.out.println("==>" + KeyValueUtil.appendTo(keyValue1, buf, offset, true));
    Assert.assertEquals(0, buf.position());
    Assert.assertEquals(1024, buf.limit());
    Assert.assertEquals(1024, buf.capacity());

    ByteBuffer buf2 = ByteBuffer.allocate(1024);
    Assert.assertEquals(0, buf2.position());
    Assert.assertEquals(1024, buf2.limit());
    Assert.assertEquals(1024, buf2.capacity());
    offset = 0;
    System.out.println("==>" + ((KeyValue) keyValue1).getSerializedSize(true));
    System.out.println("==>" + ((KeyValue) keyValue1).getSerializedSize(false));
    ((ExtendedCell) keyValue1).write(buf2, offset);
    Assert.assertEquals(0, buf2.position());
    Assert.assertEquals(1024, buf2.limit());
    Assert.assertEquals(1024, buf2.capacity());

  }

  @Test
  public void testSerde() throws Exception {
    init(false);
    doTestSerdeAndComparator(keyValue1, CCSMapCellComparatorDefault.INSTNANCE);
    doTestSerdeAndComparator(bbCell1, CCSMapCellComparatorDefault.INSTNANCE);

    doTestSerdeAndComparator(keyValue1, CCSMapCellComparatorDirectly.INSTNANCE);
    doTestSerdeAndComparator(bbCell1, CCSMapCellComparatorDirectly.INSTNANCE);

    init(true);
    doTestSerdeAndComparator(keyValue1, CCSMapCellComparatorDefault.INSTNANCE);
    doTestSerdeAndComparator(bbCell1, CCSMapCellComparatorDefault.INSTNANCE);

    doTestSerdeAndComparator(keyValue1, CCSMapCellComparatorDirectly.INSTNANCE);
    doTestSerdeAndComparator(bbCell1, CCSMapCellComparatorDirectly.INSTNANCE);
  }

  private void doTestSerdeAndComparator(Cell cell, INodeComparator<Cell> comparator)
    throws Exception {
    CellSerde cellSerde = CellSerde.INSTANCE;

    Assert.assertEquals(69, cellSerde.getSerializedSize(cell));

    ByteBuffer dstBB = ByteBuffer.allocate(2048);
    int offset = 123;

    cellSerde.serialize(keyValue1, dstBB, offset, cellSerde.getSerializedSize(cell));

    Cell deserializeCell = cellSerde.deserialize(dstBB, offset, cellSerde.getSerializedSize(cell));

    Assert.assertEquals(seqId, deserializeCell.getSequenceId());
    Assert.assertEquals(seqId, cell.getSequenceId());
    Assert.assertEquals(61, ((ExtendedCell) cell).getSerializedSize(true));
    Assert.assertEquals(61, ((ExtendedCell) deserializeCell).getSerializedSize(true));
    Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(cell, deserializeCell));

    Assert
      .assertEquals(comparator.compareTo(dstBB, offset, cellSerde.getSerializedSize(cell), cell),
        0);

    Assert
      .assertEquals(comparator.compareTo(cell, dstBB, offset, cellSerde.getSerializedSize(cell)),
        0);

    //Comare
    Assert.assertTrue(
      comparator.compareTo(cellBigRow, dstBB, offset, cellSerde.getSerializedSize(cell)) > 0);
    Assert.assertTrue(
      comparator.compareTo(dstBB, offset, cellSerde.getSerializedSize(cell), cellBigRow) < 0);

    Assert.assertTrue(
      comparator.compareTo(cellSmallRow, dstBB, offset, cellSerde.getSerializedSize(cell)) < 0);
    Assert.assertTrue(
      comparator.compareTo(dstBB, offset, cellSerde.getSerializedSize(cell), cellSmallRow) > 0);

    Assert.assertTrue(
      comparator.compareTo(cellBigF, dstBB, offset, cellSerde.getSerializedSize(cell)) > 0);
    Assert.assertTrue(
      comparator.compareTo(dstBB, offset, cellSerde.getSerializedSize(cell), cellBigF) < 0);

    Assert.assertTrue(
      comparator.compareTo(cellSmallF, dstBB, offset, cellSerde.getSerializedSize(cell)) < 0);
    Assert.assertTrue(
      comparator.compareTo(dstBB, offset, cellSerde.getSerializedSize(cell), cellSmallF) > 0);

    Assert.assertTrue(comparator.compareTo(cellBigTs, cell) < 0);

    Assert.assertTrue(
      comparator.compareTo(cellBigTs, dstBB, offset, cellSerde.getSerializedSize(cell)) < 0);
    Assert.assertTrue(
      comparator.compareTo(dstBB, offset, cellSerde.getSerializedSize(cell), cellBigTs) > 0);

    Assert.assertTrue(comparator.compareTo(cellSmallTs, cell) > 0);

    Assert.assertTrue(
      comparator.compareTo(cellSmallTs, dstBB, offset, cellSerde.getSerializedSize(cell)) > 0);
    Assert.assertTrue(
      comparator.compareTo(dstBB, offset, cellSerde.getSerializedSize(cell), cellSmallTs) < 0);

    Assert.assertTrue(comparator.compareTo(cellBigSeqId, cell) < 0);

    Assert.assertTrue(
      comparator.compareTo(cellBigSeqId, dstBB, offset, cellSerde.getSerializedSize(cell)) < 0);
    Assert.assertTrue(
      comparator.compareTo(dstBB, offset, cellSerde.getSerializedSize(cell), cellBigSeqId) > 0);

    Assert.assertTrue(comparator.compareTo(cellSmallSeqId, cell) > 0);

    Assert.assertTrue(
      comparator.compareTo(cellSmallSeqId, dstBB, offset, cellSerde.getSerializedSize(cell)) > 0);
    Assert.assertTrue(
      comparator.compareTo(dstBB, offset, cellSerde.getSerializedSize(cell), cellSmallSeqId) < 0);
  }

  @Test
  public void testCellCCSMap() throws Exception {
    init(false);
    Configuration conf = HBaseConfiguration.create();
    conf.set(CCSMAP_COMPARATOR_KEY, "useDefault");
    doTestCellCCSMap(conf);

    init(true);
    conf = HBaseConfiguration.create();
    conf.set(CCSMAP_COMPARATOR_KEY, "useDefault");
    doTestCellCCSMap(conf);

    init(true);
    conf = HBaseConfiguration.create();
    conf.set(CCSMAP_COMPARATOR_KEY, "directly");
    doTestCellCCSMap(conf);

    init(false);
    conf = HBaseConfiguration.create();
    conf.set(CCSMAP_COMPARATOR_KEY, "directly");
    doTestCellCCSMap(conf);
  }

  private void doTestCellCCSMap(Configuration conf) {
    AllocatorHandlerRegister.AllocatorHandlerBuilder builder =
      new AllocatorHandlerRegister.AllocatorHandlerBuilder();
    builder.setCapacity(8 * 1024 * 1024);
    builder.setChunkSize(4 * 1024);
    builder.setInitialCount(Integer.MAX_VALUE);
    builder.setHeapMode(HeapMode.offHeap);
    CCSMapChunkPool allocatorHandler = new CCSMapChunkPool(builder);
    AllocatorHandlerRegister.register(allocatorHandler);
    AllocatorHandlerRegister.AllocatorHandlerBuilder.setINSTANCE(builder);

    CellCCSMap cellCCSMap = CellCCSMap.buildCellCCSMap(conf, CellComparatorImpl.COMPARATOR, false);

    cellCCSMap.put(keyValue1, keyValue1);
    Assert.assertEquals(1, cellCCSMap.size());
    Cell getCell = cellCCSMap.get(keyValue1);
    Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(getCell, keyValue1));

    //rewrite the Cell
    cellCCSMap.put(bbCell1, bbCell1);
    Assert.assertEquals(1, cellCCSMap.size());
    getCell = cellCCSMap.get(keyValue1);
    Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(getCell, keyValue1));

    //put the cellSmallRow
    doPutNoRepicaCellPutAndGet(cellCCSMap, cellSmallRow, 1);
    doPutNoRepicaCellPutAndGet(cellCCSMap, cellBigRow, 2);
    doPutNoRepicaCellPutAndGet(cellCCSMap, cellSmallF, 3);
    doPutNoRepicaCellPutAndGet(cellCCSMap, cellBigF, 4);
    doPutNoRepicaCellPutAndGet(cellCCSMap, cellSmallTs, 5);
    doPutNoRepicaCellPutAndGet(cellCCSMap, cellBigTs, 6);
    doPutNoRepicaCellPutAndGet(cellCCSMap, cellSmallSeqId, 7);
    doPutNoRepicaCellPutAndGet(cellCCSMap, cellBigSeqId, 8);

    Cell firstCell = cellCCSMap.firstKey();
    Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(firstCell, cellSmallRow));

    Cell lastCell = cellCCSMap.lastKey();
    Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(lastCell, cellBigRow));

    List<Cell> sortList = new ArrayList<>();
    sortList.add(cellSmallRow);
    sortList.add(cellBigRow);
    sortList.add(cellSmallF);
    sortList.add(cellBigF);
    sortList.add(cellSmallTs);
    sortList.add(cellBigTs);
    sortList.add(cellSmallSeqId);
    sortList.add(cellBigSeqId);
    sortList.add(keyValue1);
    Collections.sort(sortList, CellComparatorImpl.COMPARATOR);

    //full scan the map
    Iterator<Map.Entry<Cell, Cell>> entryIterator = cellCCSMap.entrySet().iterator();
    int i = 0;
    while (entryIterator.hasNext()) {
      Cell cell = entryIterator.next().getKey();
      Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(cell, sortList.get(i)));
      Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(cell, orderMaps.get(i)));

      i++;
    }
    Assert.assertEquals(9, i);

    //null tail
    SortedMap<Cell, Cell> nullTailMap = cellCCSMap.tailMap(noExistedBigCell);
    Assert.assertEquals(0, nullTailMap.size());
    Assert.assertEquals(null, nullTailMap.firstKey());
    Assert.assertEquals(null, nullTailMap.lastKey());

    Iterator<Map.Entry<Cell, Cell>> nullTailMapEntries = nullTailMap.entrySet().iterator();
    int zz = 0;
    while (nullTailMapEntries.hasNext()) {
      nullTailMapEntries.next().getKey();
      zz++;
    }
    Assert.assertEquals(0, zz);

    //null head
    SortedMap<Cell, Cell> nullHeadMap = cellCCSMap.headMap(noExistedSmallCell);
    Assert.assertEquals(0, nullHeadMap.size());
    Assert.assertEquals(null, nullHeadMap.firstKey());
    Assert.assertEquals(null, nullHeadMap.lastKey());

    Iterator<Map.Entry<Cell, Cell>> nullHeadMapEntries = nullHeadMap.entrySet().iterator();
    int zzz = 0;
    while (nullHeadMapEntries.hasNext()) {
      nullHeadMapEntries.next().getKey();
      zzz++;
    }
    Assert.assertEquals(0, zzz);

    //Full scan to verify the tailMap
    for (int j = 0; j < sortList.size(); j++) {
      //headMap inclusive is false
      Map<Cell, Cell> tailMap = cellCCSMap.tailMap(sortList.get(j));
      Assert.assertEquals(tailMap.size(), sortList.size() - j);

      for (int m = 0; m < sortList.size(); m++) {
        Cell getCellTailMap = tailMap.get(sortList.get(m));
        if (m < (sortList.size() - tailMap.size())) {
          Assert.assertEquals(null, getCellTailMap);
        } else {
          Assert.assertEquals(0,
            CellComparatorImpl.COMPARATOR.compare(getCellTailMap, sortList.get(m)));
        }
      }

      Iterator<Map.Entry<Cell, Cell>> entrys = tailMap.entrySet().iterator();
      int z = 0;
      while (entrys.hasNext()) {
        Cell cell = entrys.next().getKey();
        Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(cell, sortList.get(z + j)));
        Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(cell, orderMaps.get(z + j)));
        z++;
      }
      Assert.assertEquals(z, sortList.size() - j);
    }

    //Full scan to verify the heapMap
    for (int j = 0; j < sortList.size(); j++) {
      //headMap inclusive is false
      Map<Cell, Cell> headMap = cellCCSMap.headMap(sortList.get(j));
      Assert.assertEquals(j, headMap.size());

      for (int m = 0; m < sortList.size(); m++) {
        Cell getCellHeadMap = headMap.get(sortList.get(m));
        if (m < headMap.size()) {
          Assert.assertEquals(0,
            CellComparatorImpl.COMPARATOR.compare(getCellHeadMap, sortList.get(m)));
        } else {
          Assert.assertEquals(null, getCellHeadMap);
        }
      }

      Iterator<Map.Entry<Cell, Cell>> entrys = headMap.entrySet().iterator();
      int z = 0;
      while (entrys.hasNext()) {
        Cell cell = entrys.next().getKey();
        Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(cell, sortList.get(z)));
        Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(cell, orderMaps.get(z)));
        z++;
      }
      Assert.assertEquals(z, j);
    }

  }

  private void doPutNoRepicaCellPutAndGet(CellCCSMap cellCCSMap, Cell cell, int lastSize) {
    cellCCSMap.put(cell, cell);
    Assert.assertEquals(lastSize + 1, cellCCSMap.size());

    Cell getCell = cellCCSMap.get(cell);
    Assert.assertEquals(0, CellComparatorImpl.COMPARATOR.compare(getCell, cell));
  }

  /**
   * Clone the passed cell by copying its data into the passed buf.
   */
  private static Cell copyCellTo(Cell cell, ByteBuffer buf, int offset, int len) {
    int tagsLen = cell.getTagsLength();
    if (cell instanceof ExtendedCell) {
      ((ExtendedCell) cell).write(buf, offset);
    } else {
      // Normally all Cell impls within Server will be of type ExtendedCell. Just considering the
      // other case also. The data fragments within Cell is copied into buf as in KeyValue
      // serialization format only.
      KeyValueUtil.appendTo(cell, buf, offset, true);
    }
    if (tagsLen == 0) {
      // When tagsLen is 0, make a NoTagsByteBufferKeyValue version. This is an optimized class
      // which directly return tagsLen as 0. So we avoid parsing many length components in
      // reading the tagLength stored in the backing buffer. The Memstore addition of every Cell
      // call getTagsLength().
      return new NoTagsByteBufferKeyValue(buf, offset, len, cell.getSequenceId());
    } else {
      return new ByteBufferKeyValue(buf, offset, len, cell.getSequenceId());
    }
  }
}