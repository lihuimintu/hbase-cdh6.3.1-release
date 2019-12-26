/**
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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.FOUR_BYTES_MARK;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Default chunk size is 4M，we suggest 2~8M.
 * not thread safe
 */
@InterfaceAudience.Private
abstract class AbstractChunk implements IChunk {

  private final long chunkId;
  private final long capacity;
  protected final AtomicInteger nextFreeOffset = new AtomicInteger(0);
  private final AtomicInteger alignOccupancy = new AtomicInteger(0);
  protected ByteBuffer data;
  private final long initialTime = System.currentTimeMillis();
  private volatile long lastAllocateTime = -1;
  private final AtomicLong usedTime = new AtomicLong(0);
  private final boolean isPooled;

  public AbstractChunk(long chunkId, long capacity, boolean isPooled) {
    this.chunkId = chunkId;
    this.capacity = capacity;
    this.isPooled = isPooled;
  }

  /**
   * the removed node's offset and len
   */
  private final Map<Long, Integer> removedMap = new ConcurrentHashMap<>();

  /**
   * when reuse the removed node, we need keep the fragment. offset->fragment
   */
  private final Map<Long, Integer> coverMap = new ConcurrentHashMap<>();

  @Override
  public long getChunkId() {
    return this.chunkId;
  }

  @Override
  public long getPosition() {
    return this.nextFreeOffset.get();
  }

  @Override
  public long allocate(int len) {
    int oldLen = len;
    //TODO reuse the removed node's space.
    //align
    //TODO add config for support unalign
    if (len % 8 != 0) {
      len = (len / 8 + 1) * 8;
    }
    while (true) {
      int oldOffset = nextFreeOffset.get();
      if (oldOffset + len > getLimit()) {
        return -1; // alloc doesn't fit
      }

      // Try to atomically claim this chunk
      if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + len)) {
        // we got the alloc
        this.alignOccupancy.addAndGet(oldLen - len);
        return oldOffset;
      }
    }
  }

  @Override
  public void reclaim(long offset, int len) {
    removedMap.put(offset, len);
  }

  @Override
  public long getLimit() {
    return this.capacity;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return this.data;
  }

  @Override
  public ByteBuffer asSubByteBuffer(long offset, int len) {
    ByteBuffer duplicate = this.data.duplicate();
    duplicate.limit((int) offset + len);
    duplicate.position((int) offset);
    return duplicate.slice();
  }

  @Override
  public int occupancy() {
    return (int) (getLimit() - getPosition()) + calFragment();
  }

  public int alignOccupancy() {
    return this.alignOccupancy.get();
  }

  private int calFragment() {
    Integer totalFragment = 0;
    for (Map.Entry<Long, Integer> removeEntry : removedMap.entrySet()) {
      if (coverMap.containsKey(removeEntry.getKey())) {
        totalFragment += coverMap.get(removeEntry.getKey());
      } else {
        totalFragment += removeEntry.getValue();
      }
    }
    return totalFragment;
  }

  @Override
  public void setLastAllocateStartTime() {
    Preconditions.checkArgument(this.lastAllocateTime == -1,
        "chunk stat error. existed allocateTime, cur=" + this.lastAllocateTime);
    this.nextFreeOffset.set(0);
    this.lastAllocateTime = System.currentTimeMillis();
  }

  @Override
  public void setLastAllocateEndTime() {
    Preconditions.checkArgument(this.lastAllocateTime > 0,
        "chunk stat error. no allocateTime, cur=" + this.lastAllocateTime);
    long now = System.currentTimeMillis();
    if (now > this.lastAllocateTime) {
      this.usedTime.addAndGet(now - this.lastAllocateTime);
    }
    this.nextFreeOffset.set(0);
    this.lastAllocateTime = -1;
  }

  @Override
  public long getUsedPercent() {
    long now = System.currentTimeMillis();
    return now - initialTime <= 0 ? 0 : this.usedTime.get() * 100 / (now - initialTime);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AbstractChunk)) {
      return false;
    }

    AbstractChunk that = (AbstractChunk) obj;
    return getChunkId() == that.getChunkId();
  }

  @Override
  public boolean isPooledChunk() {
    return this.isPooled;
  }

  @Override
  public int hashCode() {
    return (int) (getChunkId() & FOUR_BYTES_MARK);
  }
}
