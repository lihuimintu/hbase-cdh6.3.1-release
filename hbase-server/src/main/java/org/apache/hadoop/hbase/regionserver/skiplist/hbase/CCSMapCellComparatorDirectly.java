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

import static org.apache.hadoop.hbase.CellComparatorImpl.COMPARATOR;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.skiplist.core.ByteBufferUtils;
import org.apache.hadoop.hbase.regionserver.skiplist.core.INodeComparator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.primitives.Longs;

/**
 * CCSMapCellComparatorDirectly compare the ByteBuffer directly, avoid autoboxing the Cell
 */
@InterfaceAudience.Private
public class CCSMapCellComparatorDirectly implements INodeComparator<Cell> {

  public static final CCSMapCellComparatorDirectly INSTNANCE =
      new CCSMapCellComparatorDirectly(COMPARATOR);

  private final CellComparator cellComparator;

  public CCSMapCellComparatorDirectly(CellComparator cellComparator) {
    this.cellComparator = cellComparator;
  }

  @Override
  public int compareTo(ByteBuffer left, int leftOffset, int leftLen, ByteBuffer right,
      int rightOffset, int rightLen) {

    int rowOffsetBase = KeyValue.ROW_OFFSET + Long.BYTES;
    int rowDataOffsetBase = KeyValue.ROW_LENGTH_SIZE + rowOffsetBase;
    int leftRowDataOffset = leftOffset + rowDataOffsetBase;
    int rightRowdataOffset = rightOffset + rowDataOffsetBase;
    int leftRowDataLeght = ByteBufferUtils.toShort(left, leftOffset + rowOffsetBase);
    int rightRowDataLeght = ByteBufferUtils.toShort(right, rightOffset + rowOffsetBase);
    int c;
    //compare row
    c = ByteBufferUtils
        .compareTo(left, leftRowDataOffset, leftRowDataLeght, right, rightRowdataOffset,
            rightRowDataLeght);
    if (c != 0) {
      return c;
    }

    //compare without row

    int lFamOffset = leftRowDataOffset + leftRowDataLeght;
    int rFamOffset = rightRowdataOffset + rightRowDataLeght;

    int lFamLength = ByteBufferUtils.toByte(left, lFamOffset);
    int rFamLength = ByteBufferUtils.toByte(right, rFamOffset);

    int lKeyLen = ByteBufferUtils.toInt(left, leftOffset + Long.BYTES);
    int rKeyLen = ByteBufferUtils.toInt(right, rightOffset + Long.BYTES);

    int lQualLength = lKeyLen - KeyValue.KEY_INFRASTRUCTURE_SIZE - leftRowDataLeght - lFamLength;
    int rQualLength = rKeyLen - KeyValue.KEY_INFRASTRUCTURE_SIZE - rightRowDataLeght - rFamLength;

    int lTypeByte = -999;
    if (lFamLength + lQualLength == 0) {
      lTypeByte = ByteBufferUtils.toByte(left, leftOffset + lKeyLen + rowOffsetBase - 1);
      if (lTypeByte == KeyValue.Type.Minimum.getCode()) {
        // left is "bigger", i.e. it appears later in the sorted order
        return 1;
      }
    }
    int rTypeByte = -999;
    if (rFamLength + rQualLength == 0) {
      rTypeByte = ByteBufferUtils.toByte(right, rightOffset + rKeyLen + rowOffsetBase - 1);
      if (rTypeByte == KeyValue.Type.Minimum.getCode()) {
        return -1;
      }
    }

    // comparing column family
    c = ByteBufferUtils
        .compareTo(left, lFamOffset + Byte.BYTES, lFamLength, right, rFamOffset + Byte.BYTES,
            rFamLength);
    if (c != 0) {
      return c;
    }
    // Compare qualifier
    c = ByteBufferUtils.compareTo(left, lFamOffset + Byte.BYTES + lFamLength, lQualLength, right,
        rFamOffset + Byte.BYTES + rFamLength, rQualLength);
    if (c != 0) {
      return c;
    }

    //compare TimeStamp
    long leftTS = ByteBufferUtils
        .toLong(left, leftOffset + rowOffsetBase + lKeyLen - KeyValue.TIMESTAMP_TYPE_SIZE);
    long rightTS = ByteBufferUtils
        .toLong(right, rightOffset + rowOffsetBase + rKeyLen - KeyValue.TIMESTAMP_TYPE_SIZE);

    c = Longs.compare(rightTS, leftTS);
    if (c != 0) {
      return c;
    }

    //compare type
    if (lTypeByte == -999) {
      lTypeByte = ByteBufferUtils.toByte(left, leftOffset + lKeyLen + rowOffsetBase - 1);
    }

    if (rTypeByte == -999) {
      rTypeByte = ByteBufferUtils.toByte(right, rightOffset + rKeyLen + rowOffsetBase - 1);
    }

    c = (0xff & rTypeByte) - (0xff & lTypeByte);
    if (c != 0) {
      return c;
    }

    //compare sequenceId
    //always compare sequenceID
    long leftSequenceId = ByteBufferUtils.toLong(left, leftOffset);
    long rightSequenceId = ByteBufferUtils.toLong(right, rightOffset);
    return Longs.compare(rightSequenceId, leftSequenceId);

  }

  @Override
  public int compareTo(Cell left, ByteBuffer right, int rightOffset, int rightLen) {
    int result = this.compareTo(right, rightOffset, rightLen, left);
    return result < 0 ? 1 : result == 0 ? 0 : -1;
  }

  @Override
  public int compareTo(ByteBuffer left, int leftOffset, int leftLen, Cell right) {
    int rowOffsetBase = KeyValue.ROW_OFFSET + Long.BYTES;
    int rowDataOffsetBase = KeyValue.ROW_LENGTH_SIZE + rowOffsetBase;
    int leftRowDataOffset = leftOffset + rowDataOffsetBase;
    int leftRowDataLeght = ByteBufferUtils.toShort(left, leftOffset + rowOffsetBase);
    int c;
    //compare row

    if (right instanceof ByteBufferExtendedCell) {
      c = ByteBufferUtils.compareTo(left, leftRowDataOffset, leftRowDataLeght,
          ((ByteBufferExtendedCell) right).getRowByteBuffer(),
          ((ByteBufferExtendedCell) right).getRowPosition(), right.getRowLength());
    } else {
      c = ByteBufferUtils.compareTo(left, leftRowDataOffset, leftRowDataLeght, right.getRowArray(),
          right.getRowOffset(), right.getRowLength());
    }

    if (c != 0) {
      return c;
    }

    //compare without row

    int lFamOffset = leftRowDataOffset + leftRowDataLeght;

    int lFamLength = ByteBufferUtils.toByte(left, lFamOffset);
    int rFamLength = right.getFamilyLength();

    int lKeyLen = ByteBufferUtils.toInt(left, leftOffset + Long.BYTES);

    int lQualLength = lKeyLen - KeyValue.KEY_INFRASTRUCTURE_SIZE - leftRowDataLeght - lFamLength;
    int rQualLength = right.getQualifierLength();

    int lTypeByte = -999;
    if (lFamLength + lQualLength == 0) {
      lTypeByte = ByteBufferUtils.toByte(left, leftOffset + lKeyLen + rowOffsetBase - 1);
      if (lTypeByte == KeyValue.Type.Minimum.getCode()) {
        // left is "bigger", i.e. it appears later in the sorted order
        return 1;
      }
    }
    int rTypeByte = right.getTypeByte();
    if (rFamLength + rQualLength == 0) {
      if (rTypeByte == KeyValue.Type.Minimum.getCode()) {
        return -1;
      }
    }

    // comparing column family

    if (right instanceof ByteBufferExtendedCell) {
      c = ByteBufferUtils.compareTo(left, lFamOffset + Byte.BYTES, lFamLength,
          ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) right).getFamilyPosition(), right.getFamilyLength());
    } else {
      c = ByteBufferUtils
          .compareTo(left, lFamOffset + Byte.BYTES, lFamLength, right.getFamilyArray(),
              right.getFamilyOffset(), right.getFamilyLength());
    }

    if (c != 0) {
      return c;
    }
    // Compare qualifier
    if (right instanceof ByteBufferExtendedCell) {
      c = ByteBufferUtils.compareTo(left, lFamOffset + Byte.BYTES + lFamLength, lQualLength,
          ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) right).getQualifierPosition(), right.getQualifierLength());
    } else {
      c = ByteBufferUtils.compareTo(left, lFamOffset + Byte.BYTES + lFamLength, lQualLength,
          right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
    }

    if (c != 0) {
      return c;
    }

    //compare TimeStamp
    long leftTS = ByteBufferUtils
        .toLong(left, leftOffset + rowOffsetBase + lKeyLen - KeyValue.TIMESTAMP_TYPE_SIZE);
    long rightTS = right.getTimestamp();

    c = Longs.compare(rightTS, leftTS);
    if (c != 0) {
      return c;
    }

    //compare type
    if (lTypeByte == -999) {
      lTypeByte = ByteBufferUtils.toByte(left, leftOffset + lKeyLen + rowOffsetBase - 1);
    }

    c = (0xff & rTypeByte) - (0xff & lTypeByte);
    if (c != 0) {
      return c;
    }

    //compare sequenceId
    //always compare sequenceID
    long leftSequenceId = ByteBufferUtils.toLong(left, leftOffset);
    long rightSequenceId = right.getSequenceId();
    return Longs.compare(rightSequenceId, leftSequenceId);

  }

  @Override
  public int compareTo(Cell left, Cell right) {
    return this.cellComparator.compare(left, right);
  }
}
