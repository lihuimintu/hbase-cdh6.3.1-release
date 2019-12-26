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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
public class HugeOnHeapChunk extends OnHeapChunk {

  public HugeOnHeapChunk(long chunkId, int capacity) {
    //HugeChunk is not pooled
    super(chunkId, capacity, false);
  }

  /**
   * HugeChunk only allocate once.
   */
  @Override
  public long allocate(int len) {
    int oldOffset = nextFreeOffset.get();
    Preconditions
        .checkArgument(len == getLimit(), "HugeChunk's capacity not fit the nodeLen(" + len + ").");
    Preconditions.checkArgument(oldOffset == 0, "HugeChunk only use once, cur offset=" + oldOffset);

    // Try to atomically claim this chunk
    if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + len)) {
      return oldOffset;
    } else {
      return -1; // alloc doesn't fit
    }
  }
}
