/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.skiplist.core;

import java.nio.ByteBuffer;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
final class NodeForKey implements INode {

  private final ByteBuffer keyBB;
  private final int keyOffset;
  private final int keyLen;

  public NodeForKey(ByteBuffer keyBB, int keyOffset, int keyLen) {
    this.keyBB = keyBB;
    this.keyOffset = keyOffset;
    this.keyLen = keyLen;
  }

  @Override
  public int getKeyOffset() {
    return this.keyOffset;
  }

  @Override
  public int getKeyLen() {
    return this.keyLen;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return this.keyBB;
  }

}


