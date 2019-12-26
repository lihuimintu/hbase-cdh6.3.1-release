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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.CellSet;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.MemStoreSizing;
import org.apache.hadoop.hbase.regionserver.MutableSegment;
import org.apache.hadoop.hbase.regionserver.skiplist.core.Constant;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A mutable segment in memstore, specifically the active segment.
 */
@InterfaceAudience.Private
public class MutableSegmentOnCCSMap extends MutableSegment {

  public MutableSegmentOnCCSMap(CellSet cellSet, CellComparator comparator,
      MemStoreLAB memStoreLAB) {
    super(cellSet, comparator, memStoreLAB, null);
  }

  protected void updateMetaInfo(Cell cellToAdd, boolean succ, boolean mslabUsed,
      MemStoreSizing memstoreSize) {
    //We can't get the really size from CellSet API.
    //8 is mean 8B, this value come from the more test. it's a conservative estimate value.
    //Feature, we can expand the CellSet API to support return the real size.
    long cellSize = getDataSizeByCell(cellToAdd);
    long heapSize = getHeapSizeByCell(cellToAdd);
    incMemStoreSize(cellSize, heapSize, cellSize, 1);
    if (memstoreSize != null) {
      memstoreSize.incMemStoreSize(cellSize, heapSize, cellSize, 1);
    }
    getTimeRangeTracker().includeTimestamp(cellToAdd);
    minSequenceId = Math.min(minSequenceId, cellToAdd.getSequenceId());
    // In no tags case this NoTagsKeyValue.getTagsLength() is a cheap call.
    // When we use ACL CP or Visibility CP which deals with Tags during
    // mutation, the TagRewriteCell.getTagsLength() is a cheaper call. We do not
    // parse the byte[] to identify the tags length.
    if (cellToAdd.getTagsLength() > 0) {
      tagsPresent = true;
    }
  }

  protected int getDataSizeByCell(Cell cell) {
    return getCellLength(cell) + Constant.LEVEL_INDEX_START_OFFSET + Long.BYTES //seq
        + 8;
  }

  protected long getHeapSizeByCell(Cell cell) {
    return 0;
  }

}
