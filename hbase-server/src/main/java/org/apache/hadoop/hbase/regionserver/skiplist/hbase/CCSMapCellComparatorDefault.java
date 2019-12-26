/**
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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.regionserver.skiplist.core.INodeComparator;
import org.apache.hadoop.hbase.regionserver.skiplist.exception.SerdeException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * CCSMapCellComparatorDefault will autoBoxing the Cell.
 * it's slower than CCSMapCellComparatorDirectly
 */
@InterfaceAudience.Private
public class CCSMapCellComparatorDefault implements INodeComparator<Cell> {

  public static final CCSMapCellComparatorDefault INSTNANCE =
      new CCSMapCellComparatorDefault(CellComparatorImpl.COMPARATOR);

  private final CellComparator cellComparator;

  public CCSMapCellComparatorDefault(CellComparator cellComparator) {
    this.cellComparator = cellComparator;
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
    return this.cellComparator.compare(left, right);
  }

}
