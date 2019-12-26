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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NIL_NODE_ID;
import java.util.AbstractMap;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.regionserver.skiplist.core.AllocatorHandlerRegister.AllocatorHandlerBuilder;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * BaseTwinCCSMap is the map who have the same key and value.
 * BaseTwinCCSMap only stores this object once.
 * for BaseTwinCCSMap:
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
 * -- all data byte[]
 */
@InterfaceAudience.Private
public class BaseTwinCCSMap<K> extends AbstractCCSMap<K, K> {
  private final ISerde<K> keyvalueSerde;

  public BaseTwinCCSMap(AllocatorHandlerBuilder builder, INodeComparator<K> comparator,
      ISerde<K> keyvalueSerde, Class<K> keyClass) {
    super(builder, comparator, keyClass, SchemaEnum.same);

    this.keyvalueSerde = keyvalueSerde;
  }

  public BaseTwinCCSMap(ICCSList<K> subCCSL, ISerde<K> keyvalueSerde, Class<K> keyClass) {
    super(subCCSL, keyClass);
    this.keyvalueSerde = keyvalueSerde;
  }

  @Override
  public K put(K key1, K key2) {
    try {
      if (key1 == null) {
        throw new NullPointerException("can't put null key");
      }
      if (key1 != key2) {
        throw new IllegalStateException("TwinCCSMap is for same key value.");
      }
      //1.flip a coin to get the level
      int level = this.ccsl.generateRandomIndexLevel();
      int dataLen = this.keyvalueSerde.getSerializedSize(key1);
      int metaLen = NodeUtil.getNodeMetaLenByLevel(level);

      //2. allocate the chunk.
      long newNodeId = this.ccsl.getNodeAndWriteMeta(level, metaLen, dataLen);
      IChunk chunk = allocatorHandler.getChunkById(NodeUtil.getChunkIdByNodeId(newNodeId));
      //3.copy the data to chunk.
      int startOffset = (int) NodeUtil.getChunkOffsetByNodeId(newNodeId) + metaLen;
      this.keyvalueSerde.serialize(key1, chunk.getByteBuffer(), startOffset, dataLen);
      long node = this.ccsl.put(newNodeId, level);

      return node == NIL_NODE_ID ? null : doGetValue(node);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  Entry<K, K> doGetEntry(long nodeId) throws SerdeException {
    K obj = this.doGetValue(nodeId);
    return new AbstractMap.SimpleImmutableEntry<>(obj, obj);
  }

  @Override
  K doGetKey(long nodeId) throws SerdeException {
    return doGetValue(nodeId);
  }

  @Override
  K doGetValue(long nodeId) throws SerdeException {
    IChunk chunk = this.ccsl.getChunkById(NodeUtil.getChunkIdByNodeId(nodeId));
    int baseOffset = (int) NodeUtil.getChunkOffsetByNodeId(nodeId);
    int dataOffset = ChunkUtil.getNodeDataOffset(chunk, baseOffset);
    int dataLen = ChunkUtil.getNodeDataLen(chunk, baseOffset);
    return this.keyvalueSerde.deserialize(chunk.getByteBuffer(), dataOffset, dataLen);
  }

  @Override
  public NavigableMap<K, K> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    ICCSList<K> subCCSL =
        new SubCompactedConcurrentSkipList<>(this.ccsl, fromKey, toKey, fromInclusive, toInclusive);
    return new BaseTwinCCSMap<>(subCCSL, this.keyvalueSerde, this.keyClass);
  }

  @Override
  public NavigableMap<K, K> headMap(K toKey, boolean inclusive) {
    ICCSList<K> subCCSL =
        new SubCompactedConcurrentSkipList<>(this.ccsl, null, toKey, false, inclusive);
    return new BaseTwinCCSMap<>(subCCSL, this.keyvalueSerde, this.keyClass);
  }

  @Override
  public NavigableMap<K, K> tailMap(K fromKey, boolean inclusive) {
    ICCSList<K> subCCSL =
        new SubCompactedConcurrentSkipList<>(this.ccsl, fromKey, null, inclusive, false);
    return new BaseTwinCCSMap<>(subCCSL, this.keyvalueSerde, this.keyClass);
  }
}
