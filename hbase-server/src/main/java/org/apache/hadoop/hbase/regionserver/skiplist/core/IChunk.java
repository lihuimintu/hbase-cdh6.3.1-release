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

import java.nio.ByteBuffer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A Chunk of memory out of which allocations are sliced.
 */
@InterfaceAudience.Private
public interface IChunk {
  /**
   * @return this Chunk's HeapMode(onHeap or offHeap)
   */
  HeapMode getHeapMode();

  /**
   * @return Id of this Chunk
   */
  long getChunkId();

  /**
   * @return offset of this Chunk
   */
  long getPosition();

  /**
   * Try to allocate len bytes from the chunk.
   *
   * @return the offset of the successful allocation, or -1 to indicate not-enough-space
   */
  long allocate(int len);

  /**
   * reclaim a node's space which allocated before.
   * TODO we need to implement clear logic first. That need reference mechanism to make sure that
   * all objects which are references of this node have been GC.
   *
   * @param offset offset of this chunk.
   * @param len how many bytes this node occupy.
   */
  void reclaim(long offset, int len);

  /**
   * @return the total len of this Chunk.
   */
  long getLimit();

  /**
   * @return This chunk's backing ByteBuffer.
   */
  ByteBuffer getByteBuffer();

  /**
   * Creates a new byte buffer that shares this buffer's content.
   *
   * @return ByteBuffer
   */
  ByteBuffer asSubByteBuffer(long offset, int len);

  /**
   * free space percent of this Chunk. It includes as following:
   * 1. the tail of this Chunk have not been allocated.
   * 2. reclaim a node's space which allocated before.
   *
   * @return N=0~100. N%
   */
  int occupancy();

  /**
   * the last allocate time of this Chunk.
   */
  void setLastAllocateStartTime();

  /**
   * the last putback time of this Chunk.
   */
  void setLastAllocateEndTime();

  /**
   * the frequency of use this chunk since it allocated.
   */
  long getUsedPercent();

  /**
   * @return if this Chunk is a pooled
   */
  boolean isPooledChunk();
}
