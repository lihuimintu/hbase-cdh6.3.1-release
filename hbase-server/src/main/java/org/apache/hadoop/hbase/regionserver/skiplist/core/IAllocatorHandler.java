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

import org.apache.hadoop.hbase.regionserver.skiplist.exception.HeapFullException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * strategy pattern for allocate Chunk.
 */
@InterfaceAudience.Private
public interface IAllocatorHandler extends IChunkVisible {
  /**
   * the allocation is done by an implementation of IAllocatorHandler
   *
   * @param len the Node data len
   * @return a Chunk
   * @throws HeapFullException when no any Chunk can be allocated.
   */
  IChunk allocate(int len) throws HeapFullException;

  /**
   * put back a Chunk which be allocated before.
   *
   * @param chunk Which Chunk be put back.
   */
  void putbackChunk(IChunk chunk);

  interface IChunkAllocator {
    /**
     * allocate a Chunk by IChunkAllocator
     *
     * @param len the Node data len
     * @return a Chunk
     * @throws HeapFullException when no any Chunk can be allocated.
     */
    IChunk allocate(int len);
  }
}
