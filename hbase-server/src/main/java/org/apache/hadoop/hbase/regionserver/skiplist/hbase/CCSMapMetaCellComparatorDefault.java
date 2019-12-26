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

import static org.apache.hadoop.hbase.CellComparatorImpl.META_COMPARATOR;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.skiplist.core.INodeComparator;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * For HBase meta.
 * CCSMapMetaCellComparatorDefault will autoBoxing the Cell.
 */
@InterfaceAudience.Private
public class CCSMapMetaCellComparatorDefault implements INodeComparator<Cell> {

  public static final CCSMapMetaCellComparatorDefault INSTNANCE =
      new CCSMapMetaCellComparatorDefault(META_COMPARATOR);

  private final CellComparator metaCellComparator;

  public CCSMapMetaCellComparatorDefault(CellComparator metaCellComparator) {
    this.metaCellComparator = metaCellComparator;
  }

  @Override
  public int compareTo(ByteBuffer left, int leftOffset, int leftLen, ByteBuffer right,
      int rightOffset, int rightLen) {
    try {
      Cell leftCell = CellSerde.INSTANCE.deserialize(left, leftOffset, leftLen);
      Cell rightCell = CellSerde.INSTANCE.deserialize(right, rightOffset, rightLen);
      return this.compareTo(leftCell, rightCell);
    } catch (SerdeException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int compareTo(Cell left, ByteBuffer right, int rightOffset, int rightLen) {
    try {
      Cell rightCell = CellSerde.INSTANCE.deserialize(right, rightOffset, rightLen);
      return this.compareTo(left, rightCell);
    } catch (SerdeException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int compareTo(ByteBuffer left, int leftOffset, int leftLen, Cell right) {
    try {
      Cell leftCell = CellSerde.INSTANCE.deserialize(left, leftOffset, leftLen);
      return this.compareTo(leftCell, right);
    } catch (SerdeException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int compareTo(Cell left, Cell right) {
    return this.metaCellComparator.compare(left, right);
  }

}
