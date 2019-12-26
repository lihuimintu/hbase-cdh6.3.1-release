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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.BYTE_MARK;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.FOUR_BYTES_MARK;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.INDEX_NOT_INITIAL;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.KEYVALUE_INFRASTRUCTURE_SIZE;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NEXT_NODE_START_OFFSET;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * The data schema:
 * - meta:
 * - int level && lock:
 * --  byte1 level（Level is not more than 127)
 * --  byte2 NodeStat
 * --  byte3 preserve (for NodeLock)
 * --  byte4 preserve
 * - int dataLen :total data len
 * - long getNextNode: the pointer to next Node.
 * - long[] levelIndex; The pointer to the corresponding level layer next Node.
 * - data:
 * -- int keyLen
 * -- int valueLen
 * -- byte[] key
 * -- byte[] value
 */
@InterfaceAudience.Private
public final class ChunkUtil {

  private ChunkUtil() {
  }
  /**
   * dataLen :
   * int keyLen
   * int valueLen
   * byte[] key
   * byte[] value
   */
  public static long newNodeOnChunk(IChunk chunk, long offset, int level, int dataLen) {
    Preconditions.checkNotNull(chunk, "chunk must be not null.");
    Preconditions.checkArgument(offset >= 0, "offset can't be negative.");
    long nodeId = ((chunk.getChunkId() & FOUR_BYTES_MARK) << 32) | (offset & FOUR_BYTES_MARK);
    Preconditions.checkArgument(dataLen > 0, "dataLen error.");
    initNodeMeta(chunk, offset, level, dataLen);
    return nodeId;
  }

  public static void initNodeMeta(IChunk chunk, long offset, int level, int dataLen) {
    ByteBufferUtils.putInt(chunk.getByteBuffer(), (int) offset, level & BYTE_MARK);
    ByteBufferUtils.putInt(chunk.getByteBuffer(), (int) offset + Integer.BYTES, dataLen);

    for (int i = level; i > 0; i--) {
      ChunkUtil.setNextNodeIdForLevel(chunk, offset, i, INDEX_NOT_INITIAL);
    }

  }

  public static int getNodeLevel(IChunk chunk, long offset) {
    int meta = ByteBufferUtils.toInt(chunk.getByteBuffer(), (int) offset);
    return meta & BYTE_MARK;
  }

  public static int getNodeDataLen(IChunk chunk, long offset) {
    return ByteBufferUtils.toInt(chunk.getByteBuffer(), (int) offset + Integer.BYTES);
  }

  public static int getNodeDataOffset(IChunk chunk, long offset) {
    int level = getNodeLevel(chunk, offset);
    return (int) offset + NodeUtil.getNodeMetaLenByLevel(level);
  }

  public static int getKeyOffset(IChunk chunk, long offset, SchemaEnum schema) {
    switch (schema) {
      case diff:
        return getNodeDataOffset(chunk, offset) + KEYVALUE_INFRASTRUCTURE_SIZE;
      case same:
        // the node total data's offset
        return getNodeDataOffset(chunk, offset);
      default:
        throw new IllegalArgumentException("error schema:" + schema);
    }

  }

  public static int getKeyLen(IChunk chunk, long offset, long keyOffset, SchemaEnum schema) {
    switch (schema) {
      case diff:
        return ByteBufferUtils
            .toInt(chunk.getByteBuffer(), (int) keyOffset - KEYVALUE_INFRASTRUCTURE_SIZE);
      case same:
        // the node total data's len
        return getNodeDataLen(chunk, offset);
      default:
        throw new IllegalArgumentException("error schema:" + schema);
    }
  }

  /**
   * write data
   */
  public static <K, V> void writeNodeData(IChunk chunk, long offset, int metaLen, K key, V value,
      ISerde<K> keySerde, ISerde<V> valueSerde, int keyLen, int valueLen) throws SerdeException {

    int startOffset = (int) offset + metaLen;
    ByteBufferUtils.putInt(chunk.getByteBuffer(), startOffset, keyLen);
    ByteBufferUtils.putInt(chunk.getByteBuffer(), startOffset + Integer.BYTES, valueLen);
    keySerde
        .serialize(key, chunk.getByteBuffer(), startOffset + KEYVALUE_INFRASTRUCTURE_SIZE, keyLen);
    valueSerde.serialize(value, chunk.getByteBuffer(),
        startOffset + KEYVALUE_INFRASTRUCTURE_SIZE + keyLen, valueLen);
  }

  public static void setNextNodeIdForLevel(IChunk chunk, long offset, int level, long nextNode) {
    //no check level
    int startOffset = (int) offset + NEXT_NODE_START_OFFSET + level * Long.BYTES;
    ByteBufferUtils.putLong(chunk.getByteBuffer(), startOffset, nextNode);
  }

  public static long getNextNodeIdForLevel(IChunk chunk, long offset, int level) {
    //no check level
    int startOffset = (int) offset + NEXT_NODE_START_OFFSET + level * Long.BYTES;
    return ByteBufferUtils.toLong(chunk.getByteBuffer(), startOffset);
  }

  @VisibleForTesting
  public static int testGetNodeKeyLen(IChunk chunk, long offset) {
    int startOffset = (int) offset + NodeUtil.getNodeMetaLenByLevel(getNodeLevel(chunk, offset));
    return ByteBufferUtils.toInt(chunk.getByteBuffer(), startOffset);
  }
}
