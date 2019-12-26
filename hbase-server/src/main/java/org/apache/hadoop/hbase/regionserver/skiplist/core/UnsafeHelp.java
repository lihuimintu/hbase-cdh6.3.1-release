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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.yetus.audience.InterfaceAudience;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

@InterfaceAudience.Private
public final class UnsafeHelp {

  // Unsafe mechanics
  static final Unsafe UNSAFE;

  /**
   * The offset to the first element in a byte array.
   */
  public static final long BYTE_ARRAY_BASE_OFFSET;

  static final boolean littleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  static {
    try {
      UNSAFE = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return f.get(null);
          } catch (Throwable e) {
            throw new Error(e);
          }
        }
      });
      BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  private UnsafeHelp() {
  }

  public static boolean compareAndSetLong(ByteBuffer buf, int offset, long expected, long update) {
    if (littleEndian) {
      expected = Long.reverseBytes(expected);
      update = Long.reverseBytes(update);
    }
    if (buf.isDirect()) {
      return UNSAFE
          .compareAndSwapLong(null, ((DirectBuffer) buf).address() + offset, expected, update);
    }
    return UNSAFE
        .compareAndSwapLong(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset,
            expected, update);
  }

  public static boolean compareAndSetInt(ByteBuffer buf, int offset, int expected, int update) {
    if (littleEndian) {
      expected = Integer.reverseBytes(expected);
      update = Integer.reverseBytes(update);
    }
    if (buf.isDirect()) {
      return UNSAFE
          .compareAndSwapInt(null, ((DirectBuffer) buf).address() + offset, expected, update);
    }
    return UNSAFE
        .compareAndSwapInt(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset,
            expected, update);
  }

}
