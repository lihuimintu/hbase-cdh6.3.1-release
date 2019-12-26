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
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * serializer and Deserializer.
 * Implementation class needs thread-safety
 */
@InterfaceAudience.Private
public interface ISerde<T> {

  /**
   * write to ByteBuffer
   *
   * @param obj         key/value
   * @param destination the destination ByteBuffer of Chunk
   * @param offset      the destination ByteBuffer's offset
   * @param len         data len
   * @throws SerdeException SerdeException
   */
  void serialize(T obj, ByteBuffer destination, int offset, int len) throws SerdeException;

  /**
   * the len of key or value which will be writing to Chunk.
   *
   * @param obj key/value
   * @return the total len
   * @throws SerdeException SerdeException
   */
  int getSerializedSize(T obj) throws SerdeException;

  /**
   * read ByteBuffer
   *
   * @param bb the ByteBuffer of Chunk
   * @return the obj of key or value.
   * @throws SerdeException SerdeException
   */
  T deserialize(ByteBuffer bb, int offset, int len) throws SerdeException;

}
