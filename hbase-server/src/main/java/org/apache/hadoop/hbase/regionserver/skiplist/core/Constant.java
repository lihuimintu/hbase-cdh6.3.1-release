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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class Constant {
  private Constant() {
  }

  public static final int NO_INDEX = 0;
  public static final long NIL_NODE_ID = 0;
  public static final long INDEX_NOT_INITIAL = -2;
  public static final long SKIP_LIST_HEAD_NODE = -1;
  public final static int MAX_LEVEL = 255;
  public final static int DEFAULT_LEVEL = 32;
  public final static long FOUR_BYTES_MARK = 0xFFFFFFFFL;
  public final static int BYTE_MARK = 0xFF;
  public final static int NEXT_NODE_START_OFFSET = Integer.BYTES * 2;
  public final static int KEYVALUE_INFRASTRUCTURE_SIZE = Integer.BYTES * 2;
  public final static int LEVEL_INDEX_START_OFFSET = NEXT_NODE_START_OFFSET + Long.BYTES;
}
