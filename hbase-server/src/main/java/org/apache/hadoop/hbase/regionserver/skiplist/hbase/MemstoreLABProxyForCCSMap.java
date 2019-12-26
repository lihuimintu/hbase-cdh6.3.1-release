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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.Chunk;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.skiplist.core.ICCSMap;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Actually, CCSMap not need MemStoreLAB, but need interface as incScannerCount/decScannerCount
 * TODO introduce the new interface ScannerCountable, and MemStoreLAB implement ScannerCountable
 */
@InterfaceAudience.Private
public class MemstoreLABProxyForCCSMap implements MemStoreLAB {
  public static final String CCSMAP_USE_OFFHEAP = "hbase.hregion.memstore.ccsmap.use.offheap";
  private static Configuration conf = HBaseConfiguration.create();
  private static final Logger LOG = LoggerFactory.getLogger(MemstoreLABProxyForCCSMap.class);
  // Current count of open scanners which reading data from this MemStoreLAB
  private final AtomicInteger openScannerCount = new AtomicInteger();
  private volatile boolean closed = false;

  private final ICCSMap ccsmap;

  public MemstoreLABProxyForCCSMap(ICCSMap ccsmap) {
    this.ccsmap = ccsmap;
  }

  @Override
  public Cell copyCellInto(Cell cell) {
    return cell;
  }

  @Override
  public Cell forceCopyOfBigCellInto(Cell cell) {
    return null;
  }

  @Override
  public void close() {
    this.closed = true;
    // We could put back the chunks to pool for reusing only when there is no
    // opening scanner which will read their data
    int count = openScannerCount.get();
    if (count == 0) {
      LOG.debug("will close CCSMap: {}", ccsmap.getCcslStat().toString());
      ccsmap.close();
    }
  }

  @Override
  public void incScannerCount() {
    this.openScannerCount.incrementAndGet();
  }

  @Override
  public void decScannerCount() {
    int count = this.openScannerCount.decrementAndGet();
    if (this.closed && count == 0) {
      LOG.debug("will close CCSMap on decScanner: {}", ccsmap.getCcslStat().toString());
      ccsmap.close();
    }
  }

  @Override
  public Chunk getNewExternalChunk(ChunkCreator.ChunkType chunkType) {
    throw new UnsupportedOperationException("ccsmap not support");
  }

  @Override
  public Chunk getNewExternalChunk(int size) {
    throw new UnsupportedOperationException("ccsmap not support");
  }

  @Override
  public boolean isOnHeap() {
    return !isOffHeap();
  }

  @Override
  public boolean isOffHeap() {
    return conf.getBoolean(CCSMapMemStore.CCSMAP_USE_OFFHEAP, false);
  }
}
