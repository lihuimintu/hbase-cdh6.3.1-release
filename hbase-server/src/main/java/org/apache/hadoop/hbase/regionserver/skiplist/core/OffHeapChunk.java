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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.HeapMode.offHeap;
import java.nio.ByteBuffer;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class OffHeapChunk extends AbstractChunk {

  public OffHeapChunk(long countId, int capacity) {
    this(countId, capacity, true);
  }

  public OffHeapChunk(long countId, int capacity, boolean isPooled) {
    super(countId, capacity, isPooled);
    data = ByteBuffer.allocateDirect(capacity);
  }

  @Override
  public HeapMode getHeapMode() {
    return offHeap;
  }

}
