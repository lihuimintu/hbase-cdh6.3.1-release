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

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.skiplist.core.AllocatorHandlerRegister.AllocatorHandlerBuilder;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.HeapFullException;
import org.apache.hadoop.hbase.regionserver.skiplist.hbase.CCSMapMemStore;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * strategy pattern for allocate Chunk.
 */
@InterfaceAudience.Private
public class CCSMapChunkPool implements IAllocatorHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CCSMapChunkPool.class);
  private static volatile CCSMapChunkPool globalInstance;
  private static final Object initLock = new Object();
  private final HeapMode heapMode;
  private final int maxCount;
  private final long extraChunkWarnCapacity;
  private final int chunkSize;
  private final IChunk[] totalPoolChunkArray;
  //extra allocate Chunk's Map
  private final Map<Long, IChunk> extraChunkMap = new ConcurrentHashMap<>();
  private final int extraChunkInitialId;
  private final Queue<IChunk> chunkQueue;
  private final IChunkAllocator poolChunkAllocator;
  private final IChunkAllocator hugeChunkAllocator;
  private final IChunkAllocator normalExtraChunkAllocator;
  private final AtomicLong poolChunkIdGenerator = new AtomicLong(0);
  private final AtomicLong extraChunkIdGenerator;
  private final AtomicLong chunkCounter = new AtomicLong(0);
  private final AtomicLong requestCounter = new AtomicLong(0);
  private final AtomicLong extraRequestCounter = new AtomicLong(0);
  private final AtomicLong extraChunkCapacityUsed = new AtomicLong(0);

  public CCSMapChunkPool(AllocatorHandlerBuilder allocatorHandlerBuilder) {
    this.heapMode = allocatorHandlerBuilder.getHeapMode();
    long capacity = allocatorHandlerBuilder.getCapacity();
    this.chunkSize = allocatorHandlerBuilder.getChunkSize();

    this.maxCount = (int) (capacity / this.chunkSize);
    this.extraChunkWarnCapacity = allocatorHandlerBuilder.getExtraChunkWarnCapacity();
    if (this.maxCount <= 0 || extraChunkWarnCapacity <= 0) {
      // Enough for about 32TB of cache!
      throw new IllegalArgumentException("capacity or extra capacity not permit negative or zero, "
                                           + "maximum support 32TB now. now maxCount=" + maxCount
                                           + " ,extraChunkWarnCapacity=" + extraChunkWarnCapacity);
    }

    IChunkAllocator chunkAllocator;
    switch (this.heapMode) {
      case onHeap:
        chunkAllocator = (len) -> new OnHeapChunk(poolChunkIdGenerator.incrementAndGet(), len);
        break;
      case offHeap:
        chunkAllocator = (len) -> new OffHeapChunk(poolChunkIdGenerator.incrementAndGet(), len);
        break;
      default:
        throw new IllegalArgumentException("heapMode error");
    }
    this.poolChunkAllocator = chunkAllocator;
    this.extraChunkInitialId = maxCount + 1;
    this.extraChunkIdGenerator = new AtomicLong(extraChunkInitialId);
    this.hugeChunkAllocator = (len) -> new HugeOnHeapChunk(getExtraChunkId(), len);
    //extra chunk is not pooled
    this.normalExtraChunkAllocator = (len) -> new OnHeapChunk(getExtraChunkId(), len, false);
    this.totalPoolChunkArray = new IChunk[maxCount];
    this.chunkQueue = new ConcurrentLinkedQueue<>();
    int initialCount = allocatorHandlerBuilder.getInitialCount();
    if (initialCount > this.maxCount) {
      initialCount = this.maxCount;
    }
    for (int i = 0; i < initialCount; i++) {
      if (!this.chunkQueue.offer(doAllocate())) {
        throw new IllegalStateException(
            "chunkQueue can't offer. now chunkQueue size=" + chunkQueue.size());
      }
    }
  }

  private long getExtraChunkId() {
    long id = incrementAndGetExtraChunkId();
    while (this.extraChunkMap.containsKey(id)) {
      id = incrementAndGetExtraChunkId();
    }
    return id;
  }

  private long incrementAndGetExtraChunkId() {
    long id = this.extraChunkIdGenerator.incrementAndGet();
    if (id >= Integer.MAX_VALUE - 1) {
      this.extraChunkIdGenerator.set(this.extraChunkInitialId);
      id = this.extraChunkIdGenerator.incrementAndGet();
      LOG.debug("{} will overflow, reset it.", id);
    }
    return id;
  }

  @Override
  public IChunk allocate(int len) throws HeapFullException {
    this.requestCounter.incrementAndGet();
    IChunk chunk;
    if (len > this.chunkSize) {
      return allocateExtraChunk(len, true);
    }
    chunk = this.chunkQueue.poll();
    if (chunk == null) {
      synchronized (this.poolChunkAllocator) {
        if (this.chunkCounter.get() >= this.maxCount) {
          LOG.debug("No more available chunk in this pool, "
              + "will use extra chunk on heap before pooled chunks reclaimed.");
          // Permit chunk pool to use extra capacity when exhausted.
          // TODO Ideally we should throw HeapFullException and let upper layer
          // (like HRegion) to handle it (such as triggering flush)
          return allocateExtraChunk(this.chunkSize, false);
        }
        chunk = doAllocate();
      }
    }
    chunk.setLastAllocateStartTime();
    return chunk;
  }

  private IChunk allocateExtraChunk(int len, boolean isHuge) {
    //extra Chunk is onHeap
    if (this.extraChunkCapacityUsed.get() >= this.extraChunkWarnCapacity) {
      LOG.warn("Too many extra chunk allocated: {}, Check {} or {}",
        this.extraChunkCapacityUsed.get(), CCSMapMemStore.EXTRA_CHUNK_WARN_CAPACITY,
        CCSMapMemStore.CHUNK_CAPACITY_KEY);
    }
    IChunk chunk;
    if (isHuge) {
      chunk = this.hugeChunkAllocator.allocate(len);
    } else {
      chunk = this.normalExtraChunkAllocator.allocate(len);
    }
    this.extraRequestCounter.incrementAndGet();
    long curExtraSize = this.extraChunkCapacityUsed.addAndGet(len);
    this.extraChunkMap.put(chunk.getChunkId(), chunk);
    LOG.debug("allocate extra chunk. total extra chunk size={} ; chunkId={}, size={}, isHuge=",
      curExtraSize, chunk.getChunkId(), len, isHuge);
    return chunk;
  }

  @Override
  public void putbackChunk(IChunk chunk) {
    //not support putback duplicate.
    if (chunk.getHeapMode() == this.heapMode && chunk.isPooledChunk()) {
      chunk.getByteBuffer().rewind();
      chunk.setLastAllocateEndTime();
      if (!this.chunkQueue.offer(chunk)) {
        throw new IllegalStateException("chunkQueue can't offer. now chunkQueue size=" + chunkQueue
                                                                                           .size());
      }
    } else {
      this.extraChunkMap.remove(chunk.getChunkId());
      this.extraChunkCapacityUsed.addAndGet(-chunk.getLimit());
    }
  }

  private IChunk doAllocate() {
    IChunk chunk = this.poolChunkAllocator.allocate(this.chunkSize);
    this.totalPoolChunkArray[(int) (chunk.getChunkId() - 1)] = chunk;
    this.chunkCounter.incrementAndGet();
    return chunk;
  }

  @Override
  public IChunk getChunkById(long chunkId) {
    if (chunkId <= maxCount) {
      return this.totalPoolChunkArray[(int) (chunkId - 1)];
    } else {
      return this.extraChunkMap.get(chunkId);
    }
  }

  public long getChunkCounter() {
    return chunkCounter.get();
  }

  public long getRequestCounter() {
    return requestCounter.get();
  }

  public long getExtraRequestCounter() {
    return extraRequestCounter.get();
  }

  public AtomicLong getExtraChunkCapacityUsed() {
    return extraChunkCapacityUsed;
  }

  @VisibleForTesting
  public Queue<IChunk> getChunkQueue() {
    return chunkQueue;
  }

  @VisibleForTesting
  public IChunk[] getChunkMap() {
    return totalPoolChunkArray;
  }

  @VisibleForTesting
  public Map<Long, IChunk> getExtraChunkMap() {
    return extraChunkMap;
  }

  @VisibleForTesting
  public int getMaxCount() {
    return maxCount;
  }

  public static CCSMapChunkPool initialize(Configuration conf, long confCapacity, int confChunkSize,
      int initialCount, boolean offheap) {
    if (globalInstance != null) {
      return globalInstance;
    }
    synchronized (initLock) {
      if (globalInstance == null) {
        AllocatorHandlerBuilder builder = AllocatorHandlerBuilder
            .initialize(conf, confCapacity, confChunkSize, initialCount, offheap);
        CCSMapChunkPool allocatorHandler = new CCSMapChunkPool(builder);
        AllocatorHandlerRegister.register(allocatorHandler);
        globalInstance = allocatorHandler;
        LOG.debug("CCSMapChunkPool initialize");
      }
    }
    return globalInstance;
  }

}
