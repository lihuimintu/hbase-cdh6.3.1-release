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

import static org.apache.hadoop.hbase.regionserver.HStore.MEMSTORE_CLASS_NAME;
import static org.apache.hadoop.hbase.regionserver.skiplist.hbase.CellCCSMap.CCSMAP_COMPARATOR_DEFAULT;
import static org.apache.hadoop.hbase.regionserver.skiplist.hbase.CellCCSMap.CCSMAP_COMPARATOR_KEY;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.apache.hadoop.hbase.regionserver.DefaultMemStore;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.MemStoreSizing;
import org.apache.hadoop.hbase.regionserver.MemStoreSnapshot;
import org.apache.hadoop.hbase.regionserver.SegmentFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class CCSMapMemStore extends DefaultMemStore {
  private static final Logger LOG = LoggerFactory.getLogger(CCSMapMemStore.class);
  public static final String CHUNK_CAPACITY_KEY = "hbase.hregion.memstore.ccsmap.chunkcapacity";
  public static final String EXTRA_CHUNK_WARN_CAPACITY =
      "hbase.hregion.memstore.ccsmap.extra.chunk.warn.capacity";
  public static final String CHUNK_SIZE_KEY = "hbase.hregion.memstore.ccsmap.chunksize";
  public static final String INITIAL_CHUNK_COUNT_KEY =
      "hbase.hregion.memstore.ccsmap.chunk.initial.count";
  public static final int CHUNK_SIZE_DEFAULT = 8 * 1024 * 1024;
  public static final String CCSMAP_USE_OFFHEAP = "hbase.hregion.memstore.ccsmap.use.offheap";

  public CCSMapMemStore(final Configuration conf, final CellComparator c) {
    super(conf, c);
    LOG.debug("CCSMap memStore, comparator type={}; isMeta={}",
      conf.get(CCSMAP_COMPARATOR_KEY, CCSMAP_COMPARATOR_DEFAULT), isMeta);
  }

  protected void resetActive() {
    // Reset heap to not include any keys
    this.active = SegmentFactory.instance().createMutableSegmentCCSMap(conf, comparator, isMeta);
    this.timeOfOldestEdit = Long.MAX_VALUE;
  }

  protected void initSnapshot(final CellComparator c) {
    this.snapshot = SegmentFactory.instance().createImmutableSegment(c);
  }

  @Override
  public MemStoreSnapshot snapshot() {
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!this.snapshot.isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. "
          + "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      this.snapshotId = EnvironmentEdgeManager.currentTime();
      if (!this.active.isEmpty()) {
        this.snapshot = SegmentFactory.instance().
                                                   createImmutableSegmentCCSMap(this.active);
        resetActive();
      }
    }
    return new MemStoreSnapshot(this.snapshotId, this.snapshot);

  }

  @Override
  public void add(Cell cell, MemStoreSizing memstoreSize) {
    internalAdd(cell, memstoreSize);
  }

  private void internalAdd(final Cell toAdd, MemStoreSizing memstoreSize) {
    active.add(toAdd, true, memstoreSize);
    setOldestEditTimeToNow();
    checkActiveSize();
  }

  public void upsert(Cell cell, long readpoint, MemStoreSizing memstoreSize) {

    this.active.upsert(cell, readpoint, memstoreSize);
    setOldestEditTimeToNow();
    checkActiveSize();
  }

  public static boolean isEnabled(Configuration conf) {
    String className = conf.get(MEMSTORE_CLASS_NAME, HStore.DEFAULT_MEMSTORE_CLASS_NAME);
    MemoryCompactionPolicy inMemoryCompaction = MemoryCompactionPolicy.valueOf(
        conf.get(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
            CompactingMemStore.COMPACTING_MEMSTORE_TYPE_DEFAULT));
    return inMemoryCompaction == MemoryCompactionPolicy.NONE && className
        .equals(CCSMapMemStore.class.getName());
  }
}
