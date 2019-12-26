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
package org.apache.hadoop.hbase.regionserver.skiplist.hbase;

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.regionserver.skiplist.core.ByteBufferUtils;
import org.apache.hadoop.hbase.regionserver.skiplist.core.ISerde;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * HBase Cell Serde
 */
@InterfaceAudience.Private
public class CellSerde implements ISerde<Cell> {
  public static final CellSerde INSTANCE = new CellSerde();

  @Override
  public int getSerializedSize(Cell cell) throws SerdeException {
    // Normally all Cell impls within Server will be of type ExtendedCell. Just considering the
    // other case also. The data fragments within Cell is copied into buf as in KeyValue
    // serialization format only.

    // Long is for seqId，We need save the Seq on the CCSMap.
    // we need true for total all data len.
    return KeyValueUtil.getSerializedSize(cell, true) + Long.BYTES;
  }

  @Override
  public void serialize(Cell cell, ByteBuffer destination, int offset, int len)
      throws SerdeException {
    //seqId is the first.
    ByteBufferUtils.putLong(destination, offset, cell.getSequenceId());

    if (cell instanceof ExtendedCell) {
      ((ExtendedCell) cell).write(destination, offset + Long.BYTES);
    } else {
      // Normally all Cell impls within Server will be of type ExtendedCell. Just considering the
      // other case also. The data fragments within Cell is copied into buf as in KeyValue
      // serialization format only.
      KeyValueUtil.appendTo(cell, destination, offset + Long.BYTES, true);
    }
  }

  @Override
  public Cell deserialize(ByteBuffer bb, int offset, int len) throws SerdeException {
    long seqId = ByteBufferUtils.toLong(bb, offset);
    return new ByteBufferKeyValue(bb, offset + Long.BYTES, len - Long.BYTES, seqId);
  }

}
