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
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.KEYVALUE_INFRASTRUCTURE_SIZE;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.LEVEL_INDEX_START_OFFSET;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NEXT_NODE_START_OFFSET;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.SKIP_LIST_HEAD_NODE;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.ChunkMissingException;
import org.apache.yetus.audience.InterfaceAudience;

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
public final class NodeUtil {

  //max level is 255.
  //reduce repeated meta length calculation
  private final static int[] metaLenByLevelArray = new int[255];

  static {
    for (int i = 0; i < metaLenByLevelArray.length; i++) {
      metaLenByLevelArray[i] = LEVEL_INDEX_START_OFFSET + i * Long.BYTES;
    }
  }

  private NodeUtil() {
  }

  public static int getNodeMetaLenByLevel(int level) {
    return metaLenByLevelArray[level];
  }

  public static int getNodeLen(int level, int keyLen, int totalValueLen) {
    return getNodeMetaLenByLevel(level) + getDataLen(keyLen, totalValueLen);
  }

  public static int getDataLen(int keyLen, int totalValueLen) {
    return keyLen + totalValueLen + KEYVALUE_INFRASTRUCTURE_SIZE;
  }

  public static long getChunkIdByNodeId(long nodeId) {
    return (nodeId >>> 32) & FOUR_BYTES_MARK;
  }

  public static long getChunkOffsetByNodeId(long nodeId) {
    return nodeId & FOUR_BYTES_MARK;
  }

  public static boolean casSetNodeStat(IChunkVisible chunkVisible, long nodeId, NodeStat nodeStat) {

    if (nodeId == SKIP_LIST_HEAD_NODE) {
      return true;
    }

    long chunkId = NodeUtil.getChunkIdByNodeId(nodeId);
    long offset = NodeUtil.getChunkOffsetByNodeId(nodeId);
    ByteBuffer nodeTrunk = chunkVisible.getChunkById(chunkId).getByteBuffer();
    int meta = ByteBufferUtils.toInt(nodeTrunk, (int) offset);
    int level = meta & BYTE_MARK;
    int expected = level & BYTE_MARK;
    int update = (((int) nodeStat.getValue()) & BYTE_MARK) << 8 | expected;
    return ByteBufferUtils.compareAndSetInt(nodeTrunk, (int) offset, expected, update);
  }

  public static boolean casNodeLock(IChunkVisible chunkVisible, long nodeId, NodeStat nodeStat) {
    return doCasNodeLock(chunkVisible, nodeId, nodeStat, true);
  }

  public static boolean casNodeUnLock(IChunkVisible chunkVisible, long nodeId, NodeStat nodeStat) {

    return doCasNodeLock(chunkVisible, nodeId, nodeStat, false);
  }

  private static boolean doCasNodeLock(IChunkVisible chunkVisible, long nodeId, NodeStat nodeStat,
      boolean lock) {
    if (nodeId == SKIP_LIST_HEAD_NODE) {
      return true;
    }
    long chunkId = NodeUtil.getChunkIdByNodeId(nodeId);
    long offset = NodeUtil.getChunkOffsetByNodeId(nodeId);
    ByteBuffer nodeTrunk = chunkVisible.getChunkById(chunkId).getByteBuffer();
    int meta = ByteBufferUtils.toInt(nodeTrunk, (int) offset);
    int level = meta & BYTE_MARK;
    int expected = level & BYTE_MARK;
    int update = ((int) nodeStat.getValue()) << 16 | expected;
    if (lock) {
      return ByteBufferUtils.compareAndSetInt(nodeTrunk, (int) offset, expected, update);
    } else {
      return ByteBufferUtils.compareAndSetInt(nodeTrunk, (int) offset, update, expected);
    }
  }

  public static ByteBuffer duplicateNodeDataByteBuffer(IChunkVisible chunkVisible, long nodeId) {
    long chunkId = NodeUtil.getChunkIdByNodeId(nodeId);
    long offset = NodeUtil.getChunkOffsetByNodeId(nodeId);
    IChunk chunk = chunkVisible.getChunkById(chunkId);
    return chunk.asSubByteBuffer(ChunkUtil.getNodeDataOffset(chunk, offset),
        ChunkUtil.getNodeDataLen(chunk, offset));
  }

  public static long getNextNodeIdForLevel(IChunkVisible chunkVisible, long nodeId, int curLevel)
      throws ChunkMissingException {
    long chunkId = NodeUtil.getChunkIdByNodeId(nodeId);
    long offset = NodeUtil.getChunkOffsetByNodeId(nodeId);
    IChunk chunk = chunkVisible.getChunkById(chunkId);
    if (chunk == null) {
      throw new ChunkMissingException(
          "nodeId=" + nodeId + "chunkid=" + chunkId + "offset=" + offset);
    }
    return ChunkUtil.getNextNodeIdForLevel(chunk, offset, curLevel);
  }

  public static boolean casUpdateNextNodeForLevel(IChunkVisible chunkVisible, int level,
                                                   long nodeId, long expected, long update)
    throws ChunkMissingException {
    long chunkId = (nodeId >>> 32) & FOUR_BYTES_MARK;
    long offset = nodeId & FOUR_BYTES_MARK;
    IChunk chunk = chunkVisible.getChunkById(chunkId);
    if (chunk == null) {
      throw new ChunkMissingException("no chunk found. " + chunkId + "=chunkId" + "  offset="
                                        + offset);
    }
    int offsetS = (int) offset + NEXT_NODE_START_OFFSET + level * Long.BYTES;
    return ByteBufferUtils.compareAndSetLong(chunk.getByteBuffer(), offsetS, expected, update);
  }

  public static boolean isNodeRemoving(IChunkVisible chunkVisible, long nodeId)
      throws ChunkMissingException {
    long chunkId = NodeUtil.getChunkIdByNodeId(nodeId);
    long offset = NodeUtil.getChunkOffsetByNodeId(nodeId);
    IChunk chunk = chunkVisible.getChunkById(chunkId);
    if (chunk == null) {
      throw new ChunkMissingException("no chunk found. " + chunkId + "=chunkId" + "  offset="
                                        + offset);
    }
    int meta = ByteBufferUtils.toInt(chunk.getByteBuffer(), (int) offset);
    byte lock = (byte) (meta >>> 8 & BYTE_MARK);
    return lock == NodeStat.REMOVE.getValue();
  }

}
