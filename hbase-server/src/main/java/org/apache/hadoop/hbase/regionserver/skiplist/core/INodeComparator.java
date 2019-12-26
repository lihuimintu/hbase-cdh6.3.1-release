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
import org.apache.hadoop.hbase.regionserver.skiplist.exception.ChunkMissingException;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface INodeComparator<K> {

  /**
   * Compares for order.  Returns a negative integer,
   * zero, or a positive integer as the first node is less than, equal
   * to, or greater than the second.<p>
   *
   * @param left        left Key's ByteBuffer
   * @param leftOffset  left Key's ByteBuffer's offset
   * @param leftLen     left Key's ByteBuffer's len
   * @param right       right Key's ByteBuffer
   * @param rightOffset right Key's ByteBuffer's offset
   * @param rightLen    right Key's ByteBuffer's len
   * @return An integer result of the comparison.
   * @throws ChunkMissingException when can't find Chunk for leftNodeId
   **/
  int compareTo(ByteBuffer left, int leftOffset, int leftLen, ByteBuffer right, int rightOffset,
      int rightLen);

  /**
   * Compares for order.  Returns a negative integer,
   * zero, or a positive integer as the first node is less than, equal
   * to, or greater than the second.<p>
   *
   * @param left           left Key's Object
   * @param right          right Key's ByteBuffer
   * @param rightKeyOffset right Key's ByteBuffer's offset
   * @param rightKeyLen    right Key's ByteBuffer's len
   * @return An integer result of the comparison.
   * @throws ChunkMissingException when can't find Chunk for leftNodeId
   */
  int compareTo(K left, ByteBuffer right, int rightKeyOffset, int rightKeyLen);

  /**
   * Compares for order.  Returns a negative integer,
   * zero, or a positive integer as the first node is less than, equal
   * to, or greater than the second.<p>
   *
   * @param left          left Key's ByteBuffer
   * @param leftKeyOffset left Key's ByteBuffer's offset
   * @param leftKeyLen    left Key's ByteBuffer's len
   * @param right         right Key's Object
   * @return An integer result of the comparison.
   * @throws ChunkMissingException when can't find Chunk for leftNodeId
   */
  int compareTo(ByteBuffer left, int leftKeyOffset, int leftKeyLen, K right);

  /**
   * Compares for order.  Returns a negative integer,
   * zero, or a positive integer as the first Key Object is less than, equal
   * to, or greater than the second.<p>
   *
   * @param left  left Key's Object
   * @param right right Key's Object
   * @return An integer result of the comparison.
   * @throws ChunkMissingException when can't find Chunk for leftNodeId
   */
  int compareTo(K left, K right);
}
