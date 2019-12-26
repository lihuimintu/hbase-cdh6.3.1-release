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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.skiplist.hbase.CCSMapMemStore;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Global Register for IAllocatorHandler.
 * CCSMap will get IAllocatorHandler from the Register to allocate the mem.
 */
@InterfaceAudience.Private
public final class AllocatorHandlerRegister {
  private static final Logger LOG = LoggerFactory.getLogger(AllocatorHandlerRegister.class);

  private static final int INITIAL_COUNT_DEFAULT = 100;
  private static final int USE_OLD_CHUNK_THRESHOLD = 3;
  private static final int CHUNK_SIZE_DEFAULT = 4 * 1024 * 1024;
  private static final long CAPACITY_DEFAULT = 10 * 1024 * 1024 * 1024L;

  private static IAllocatorHandler INSTANCE;

  private AllocatorHandlerRegister(){}

  public static void register(IAllocatorHandler allocatorHandler) {
    //provided extended capabilities. such as extend this INSTANCE to chain or decorate pattern
    INSTANCE = allocatorHandler;
  }

  public static IAllocatorHandler getAllocatorHandler() {
    if (INSTANCE == null) {
      throw new IllegalStateException("not any registered allocatorHandler");
    }
    return INSTANCE;
  }

  public static class AllocatorHandlerBuilder {
    private static AllocatorHandlerBuilder INSTANCE = null;
    private long capacity = CAPACITY_DEFAULT;
    private int chunkSize = CHUNK_SIZE_DEFAULT;
    private int initialCount = INITIAL_COUNT_DEFAULT;
    private int useOldChunkThreshold = USE_OLD_CHUNK_THRESHOLD;
    private IAllocatorHandler.IChunkAllocator chunkAllocator;
    private HeapMode heapMode = HeapMode.offHeap;
    private long extraChunkWarnCapacity = CAPACITY_DEFAULT;

    public long getCapacity() {
      return capacity;
    }

    public int getChunkSize() {
      return chunkSize;
    }

    public int getInitialCount() {
      return initialCount;
    }

    public IAllocatorHandler.IChunkAllocator getChunkAllocator() {
      return chunkAllocator;
    }

    public HeapMode getHeapMode() {
      return heapMode;
    }

    public int getUseOldChunkThreshold() {
      return useOldChunkThreshold;
    }

    public AllocatorHandlerBuilder setCapacity(long capacity) {
      this.capacity = capacity;
      return this;
    }

    public AllocatorHandlerBuilder setChunkSize(int chunkSize) {
      this.chunkSize = chunkSize;
      return this;
    }

    public AllocatorHandlerBuilder setInitialCount(int initialCount) {
      this.initialCount = initialCount;
      return this;
    }

    public AllocatorHandlerBuilder setChunkAllocator(
        IAllocatorHandler.IChunkAllocator chunkAllocator) {
      this.chunkAllocator = chunkAllocator;
      return this;
    }

    public AllocatorHandlerBuilder setHeapMode(HeapMode heapMode) {
      this.heapMode = heapMode;
      return this;
    }

    public AllocatorHandlerBuilder setUseOldChunkThreshold(int useOldChunkThreshold) {
      if (useOldChunkThreshold < 1) {
        useOldChunkThreshold = 1;
      }
      this.useOldChunkThreshold = useOldChunkThreshold;
      return this;
    }

    public void setExtraChunkWarnCapacity(long extraChunkWarnCapacity) {
      this.extraChunkWarnCapacity = extraChunkWarnCapacity;
    }

    public long getExtraChunkWarnCapacity() {
      return extraChunkWarnCapacity;
    }

    public static synchronized AllocatorHandlerBuilder initialize(Configuration conf,
        long confCapacity, int confChunkSize, int initialCount, boolean offheap) {
      if (INSTANCE == null) {
        AllocatorHandlerBuilder builder = new AllocatorHandlerBuilder();

        long capacity = conf.getLong(CCSMapMemStore.CHUNK_CAPACITY_KEY, confCapacity);
        builder.setCapacity(capacity);

        int chunkSize = conf.getInt(CCSMapMemStore.CHUNK_SIZE_KEY, confChunkSize);
        builder.setChunkSize(chunkSize);
        builder.setInitialCount(initialCount);
        offheap = conf.getBoolean(CCSMapMemStore.CCSMAP_USE_OFFHEAP, offheap);
        builder.setHeapMode(offheap ? HeapMode.offHeap : HeapMode.onHeap);
        long extraChunkCapacity =
            conf.getLong(CCSMapMemStore.EXTRA_CHUNK_WARN_CAPACITY, confCapacity);
        builder.setExtraChunkWarnCapacity(extraChunkCapacity);
        INSTANCE = builder;
        LOG.debug("CCSMap AllocatorHandlerBuilder initInstance, capacity={}; "
            + "extraTrunkCapacity={}; chunkSize={}; offHeap={}", capacity, extraChunkCapacity,
          chunkSize, offheap);
      }
      return INSTANCE;
    }

    public static AllocatorHandlerBuilder getINSTANCE() {
      return INSTANCE;
    }

    @VisibleForTesting
    public static synchronized void setINSTANCE(AllocatorHandlerBuilder INSTANCE) {
      AllocatorHandlerBuilder.INSTANCE = INSTANCE;
    }
  }

}
