/**
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
import org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.ChunkMissingException;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface ICCSList<K> extends IChunkVisible {

  /**
   * Returns a nodeId associated with the least
   * key in this list, or {@code NIL_NODE_ID} if the list is empty.
   *
   * @return a nodeId with the least key, or {@code NIL_NODE_ID} if this list is empty
   */
  long findFirst() throws CCSMapException;

  /**
   * Returns a nodeId associated with the greatest
   * key in this list, or {@code NIL_NODE_ID} if the list is empty.
   *
   * @return a nodeId with the greatest key, or {@code NIL_NODE_ID} if this list is empty
   */
  long findLast() throws CCSMapException;

  /**
   * @return the number of item in this list.
   */
  int getSize();

  /**
   * Closes this list and releases any memory associated
   * with it. If the list is already closed then invoking this
   * method has no effect.
   */
  void close();

  /**
   * @return stat of this List
   */
  CompactedConcurrentSkipList.Stat getCcslStat();

  /**
   * it's not use Jdk's Iterator, IIterCCSList can help to reduce the autoBoxing
   *
   * @return An iterator of this List
   * @throws CCSMapException if a CCSMap error occurs.
   */
  IIterCCSList nodeIdIter() throws CCSMapException;

  /**
   * Returns the nodeId to which the specified key,
   * or {@code NIL_NODE_ID} if this List contains no node for the key.
   */
  long get(K key) throws CCSMapException;

  /**
   * Generate the random level for new node which will be put to this List.
   *
   * @return a random level
   */
  int generateRandomIndexLevel();

  /**
   * allocate a Chunk and offset for putting new node, then write node's meta.
   *
   * @param level   the putting node's level
   * @param metaLen   the putting node's meta len
   * @param dataLen the putting node's data len.
   * @return nodeId for the putting node.
   * @throws CCSMapException if a CCSMap error occurs.
   */
  long getNodeAndWriteMeta(int level, int metaLen, int dataLen) throws CCSMapException;

  /**
   * Appends the specified node to this list.
   *
   * @param newNodeId the putting node's nodeId
   * @param level     the putting node's level
   * @return the previous nodeId associated with <tt>key</tt>, or
   * <tt>NIL_NODE_ID</tt> if there was no mapping for <tt>key</tt>.
   * @throws CCSMapException CCSMapException
   */
  long put(long newNodeId, int level) throws CCSMapException;

  /**
   * Removes the node for a key from this List if it is present.
   */
  long remove(K key) throws CCSMapException;

  /**
   * Removes the node for a nodeId from this List if it is present.
   */
  long remove(long nodeId) throws CCSMapException;

  /**
   * @return INodeComparator for this List
   */
  INodeComparator<K> getComparator();

  /**
   * Get the next node of the specified node on the corresponding layer，or {@code NIL_NODE_ID}
   *
   * @param currentNodeId  the specified node
   * @param level skip list layer
   * @return nodeId, or {@code NIL_NODE_ID}
   * @throws CCSMapException if a CCSMap error occurs.
   */
  long getNextNodeByIdAndLevel(long currentNodeId, int level) throws CCSMapException;

  /**
   * Get the next node of the specified node on the linked list which contains all nodes，
   * or {@code NIL_NODE_ID}
   *
   * @param currentNodeId the specified node
   * @return nodeId, or {@code NIL_NODE_ID}
   * @throws CCSMapException if a CCSMap error occurs.
   */
  long getNextNodeById(long currentNodeId) throws ChunkMissingException;

  /**
   * Utility for ceiling, floor, lower, higher methods.
   *
   * @param key the key
   * @param rel the relation -- OR'ed combination of EQ, LT, GT
   * @return nearest node fitting relation, or {@code NIL_NODE_ID} if no such
   */
  long findNear(K key, int rel) throws CCSMapException;

  /**
   * Compares for order.  Returns a negative integer,
   * zero, or a positive integer as the first node is less than, equal
   * to, or greater than the second.<p>
   *
   * @param leftNodeId the node data's node ID (chunkId,offset)
   * @param byteBuffer Key's ByteBuffer
   * @param keyOffset  Key's ByteBuffer's offset
   * @param keyLen     Key's ByteBuffer's len
   * @return An integer result of the comparison.
   * @throws ChunkMissingException when can't find Chunk for leftNodeId
   */
  int compareNodes(long leftNodeId, ByteBuffer byteBuffer, int keyOffset, int keyLen)
      throws ChunkMissingException;

  /**
   * Compares for order.  Returns a negative integer,
   * zero, or a positive integer as the first node is less than, equal
   * to, or greater than the second.<p>
   *
   * @param left          Key's ByteBuffer
   * @param leftKeyOffset Key's ByteBuffer's offset
   * @param leftKeyLen    Key's ByteBuffer's len
   * @param rightNodeId   the node data's node ID (chunkId,offset)
   * @return An integer result of the comparison.
   * @throws ChunkMissingException when can't find Chunk for leftNodeId
   */
  int compareNodes(ByteBuffer left, int leftKeyOffset, int leftKeyLen, long rightNodeId)
      throws ChunkMissingException;

  /**
   * Compares for order.  Returns a negative integer,
   * zero, or a positive integer as the first node is less than, equal
   * to, or greater than the second.<p>
   *
   * @param left           left Key's ByteBuffer
   * @param leftKeyOffset  left Key's ByteBuffer's offset
   * @param leftKeyLen     left Key's ByteBuffer's len
   * @param right          right Key's ByteBuffer
   * @param rightKeyOffset right Key's ByteBuffer's offset
   * @param rightKeyLen    right Key's ByteBuffer's len
   * @return An integer result of the comparison.
   * @throws ChunkMissingException when can't find Chunk for leftNodeId
   */
  int compareNodes(ByteBuffer left, int leftKeyOffset, int leftKeyLen, ByteBuffer right,
      int rightKeyOffset, int rightKeyLen) throws ChunkMissingException;

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
  int compareNodes(ByteBuffer left, int leftKeyOffset, int leftKeyLen, K right)
      throws ChunkMissingException;

  /**
   * Compares for order.  Returns a negative integer,
   * zero, or a positive integer as the first node is less than, equal
   * to, or greater than the second.<p>
   *
   * @param leftNodeId the node data's node ID (chunkId,offset)
   * @param right      right Key's Object
   * @return An integer result of the comparison.
   * @throws ChunkMissingException when can't find Chunk for leftNodeId
   */
  int compareNodes(long leftNodeId, K right) throws ChunkMissingException;

  /**
   * Compares for order.  Returns a negative integer,
   * zero, or a positive integer as the first node is less than, equal
   * to, or greater than the second.<p>
   *
   * @param left        left Key's Object
   * @param rightNodeId the node data's node ID (chunkId,offset)
   * @return An integer result of the comparison.
   * @throws ChunkMissingException when can't find Chunk for leftNodeId
   */
  int compareNodes(K left, long rightNodeId) throws ChunkMissingException;

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
  int compareNodes(K left, K right) throws ChunkMissingException;

  /**
   * @return the cur max level of this List
   */
  int getCurMaxLevel();

}
