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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.skiplist.core.AllocatorHandlerRegister;
import org.apache.hadoop.hbase.regionserver.skiplist.core.AllocatorHandlerRegister.AllocatorHandlerBuilder;
import org.apache.hadoop.hbase.regionserver.skiplist.core.BaseTwinCCSMap;
import org.apache.hadoop.hbase.regionserver.skiplist.core.CCSMapChunkPool;
import org.apache.hadoop.hbase.regionserver.skiplist.core.INodeComparator;
import org.apache.hadoop.hbase.regionserver.skiplist.core.ISerde;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * for HBase use NavigableMap<\Cell,Cell\> simplify.
 */
@InterfaceAudience.Private
public class CellCCSMap extends BaseTwinCCSMap<Cell> {
  public static final String CCSMAP_COMPARATOR_KEY = "hbase.hregion.memstore.ccsmap.comparator";
  public static final String CCSMAP_COMPARATOR_DEFAULT = "directly";

  public static final String TEST_CHUNK_CAPACITY_KEY =
      "test.hbase.hregion.memstore.ccsmap.chunkcapacity";
  public static final String TEST_CHUNK_SIZE_KEY = "test.hbase.hregion.memstore.ccsmap.chunksize";
  public static final String TEST_CCSMAP_USE_OFFHEAP =
      "test.hbase.hregion.memstore.ccsmap.use.offheap";

  public CellCCSMap(AllocatorHandlerRegister.AllocatorHandlerBuilder builder,
      INodeComparator<Cell> cellComparator, ISerde<Cell> keyvalueSerde) {
    super(builder, cellComparator, keyvalueSerde, Cell.class);
  }

  public static CellCCSMap buildCellCCSMap(final Configuration conf, CellComparator comparator,
      boolean isMeta) {
    ISerde<Cell> cellSerde = CellSerde.INSTANCE;
    if (AllocatorHandlerBuilder.getINSTANCE() == null) {
      //only for test
      long capacity = conf.getLong(TEST_CHUNK_CAPACITY_KEY, 1024 * 1024 * 1024L);
      int chunkSize = conf.getInt(TEST_CHUNK_SIZE_KEY, 4 * 1024);
      boolean offheap = conf.getBoolean(TEST_CCSMAP_USE_OFFHEAP, false);
      CCSMapChunkPool.initialize(conf, capacity, chunkSize, 10, offheap);
    }

    INodeComparator<Cell> nodeComparator = CCSMapCellComparatorDirectly.INSTNANCE;
    if ("useDefault".equalsIgnoreCase(conf.get(CCSMAP_COMPARATOR_KEY, CCSMAP_COMPARATOR_DEFAULT))) {
      nodeComparator = CCSMapCellComparatorDefault.INSTNANCE;
    }
    if (isMeta) {
      nodeComparator = CCSMapMetaCellComparatorDefault.INSTNANCE;
    }
    return new CellCCSMap(AllocatorHandlerRegister.AllocatorHandlerBuilder.getINSTANCE(),
        nodeComparator, cellSerde);
  }

}
