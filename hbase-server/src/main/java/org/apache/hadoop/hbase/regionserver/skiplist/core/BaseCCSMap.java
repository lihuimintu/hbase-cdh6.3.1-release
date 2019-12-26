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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.KEYVALUE_INFRASTRUCTURE_SIZE;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NIL_NODE_ID;
import java.util.AbstractMap;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.regionserver.skiplist.core.AllocatorHandlerRegister.AllocatorHandlerBuilder;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.CCSMapException;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * BaseCCSMap is the map who have the diff key and value.
 * for BaseCCSMap:
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
public class BaseCCSMap<K, V> extends AbstractCCSMap<K, V> {

  private final ISerde<K> keySerde;
  private final ISerde<V> valueSerde;

  public BaseCCSMap(AllocatorHandlerBuilder builder, INodeComparator<K> comparator,
      ISerde<K> keySerde, ISerde<V> valueSerde, Class<K> keyClass) {
    super(builder, comparator, keyClass, SchemaEnum.diff);
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  public BaseCCSMap(ICCSList<K> ccsl, ISerde<K> keySerde, ISerde<V> valueSerde, Class<K> keyClass) {
    super(ccsl, keyClass);
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  @Override
  public V put(K key, V value) {
    try {
      if (key == null) {
        throw new NullPointerException("can't put null key");
      }
      //1.flip a coin to get the level
      int level = this.ccsl.generateRandomIndexLevel();
      int keyLen = this.keySerde.getSerializedSize(key);

      int valueLen = this.valueSerde.getSerializedSize(value);
      int metaLen = NodeUtil.getNodeMetaLenByLevel(level);

      //2. allocate the chunk.
      int dataLen = NodeUtil.getDataLen(keyLen, valueLen);
      long newNodeId = this.ccsl.getNodeAndWriteMeta(level, metaLen, dataLen);
      IChunk chunk = allocatorHandler.getChunkById(NodeUtil.getChunkIdByNodeId(newNodeId));
      //3.copy the data to chunk.
      ChunkUtil.
          writeNodeData(chunk, NodeUtil.getChunkOffsetByNodeId(newNodeId), metaLen, key, value,
              this.keySerde, this.valueSerde, keyLen, valueLen);

      long node = this.ccsl.put(newNodeId, level);
      return node == NIL_NODE_ID ? null : doGetValue(node);
    } catch (CCSMapException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  Entry<K, V> doGetEntry(long nodeId) throws SerdeException {
    IChunk chunk = this.ccsl.getChunkById(NodeUtil.getChunkIdByNodeId(nodeId));
    int baseOffset = (int) NodeUtil.getChunkOffsetByNodeId(nodeId);
    int keyOffset = ChunkUtil.getKeyOffset(chunk, baseOffset, SchemaEnum.diff);
    int keyLen = ChunkUtil.getKeyLen(chunk, baseOffset, keyOffset, SchemaEnum.diff);
    int valueOffset = keyOffset + keyLen;
    int dataLen = ChunkUtil.getNodeDataLen(chunk, baseOffset);
    int valueLen = dataLen - keyLen - KEYVALUE_INFRASTRUCTURE_SIZE;
    //only key byte[]
    K key = this.keySerde.deserialize(chunk.getByteBuffer(), keyOffset, keyLen);
    //only value byte[]
    V value = this.valueSerde.deserialize(chunk.getByteBuffer(), valueOffset, valueLen);
    return new AbstractMap.SimpleImmutableEntry<>(key, value);
  }

  @Override
  K doGetKey(long nodeId) throws SerdeException {
    IChunk chunk = this.ccsl.getChunkById(NodeUtil.getChunkIdByNodeId(nodeId));
    int baseOffset = (int) NodeUtil.getChunkOffsetByNodeId(nodeId);
    int keyOffset = ChunkUtil.getKeyOffset(chunk, baseOffset, SchemaEnum.diff);
    int keyLen = ChunkUtil.getKeyLen(chunk, baseOffset, keyOffset, SchemaEnum.diff);
    //only key byte[]
    return this.keySerde.deserialize(chunk.getByteBuffer(), keyOffset, keyLen);
  }

  @Override
  V doGetValue(long nodeId) throws SerdeException {
    IChunk chunk = this.ccsl.getChunkById(NodeUtil.getChunkIdByNodeId(nodeId));
    int baseOffset = (int) NodeUtil.getChunkOffsetByNodeId(nodeId);
    int keyOffset = ChunkUtil.getKeyOffset(chunk, baseOffset, SchemaEnum.diff);
    int keyLen = ChunkUtil.getKeyLen(chunk, baseOffset, keyOffset, SchemaEnum.diff);
    int valueOffset = keyOffset + keyLen;
    int dataLen = ChunkUtil.getNodeDataLen(chunk, baseOffset);
    int valueLen = dataLen - keyLen - KEYVALUE_INFRASTRUCTURE_SIZE;
    //only value byte[]
    return this.valueSerde.deserialize(chunk.getByteBuffer(), valueOffset, valueLen);
  }

  @Override
  public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    ICCSList<K> subCCSL =
        new SubCompactedConcurrentSkipList<>(this.ccsl, fromKey, toKey, fromInclusive, toInclusive);
    return new BaseCCSMap<>(subCCSL, keySerde, valueSerde, keyClass);
  }

  @Override
  public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    ICCSList<K> subCCSL =
        new SubCompactedConcurrentSkipList<>(this.ccsl, null, toKey, false, inclusive);
    return new BaseCCSMap<>(subCCSL, keySerde, valueSerde, keyClass);
  }

  @Override
  public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    ICCSList<K> subCCSL =
        new SubCompactedConcurrentSkipList<>(this.ccsl, fromKey, null, inclusive, false);
    return new BaseCCSMap<>(subCCSL, keySerde, valueSerde, keyClass);
  }
}
