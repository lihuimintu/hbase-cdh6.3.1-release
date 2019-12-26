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

/**
 * the LevelIndexHeader is small(normally 2K)，no much GC because objects are very few. so we only
 * provide OnHeapIndexHeader first.
 */
@InterfaceAudience.Private
public interface ILevelIndexHeader {
  /**
   * @return cur max level
   */
  int getLevel();

  /**
   * update cur max level
   *
   * @param level the dst level
   */
  void updateLevel(int level);

  /**
   * cas update the next Node for level layer on this levelIndex
   *
   * @param level        which layer
   * @param expectedNode the expected nodeId
   * @param newNode      the new nodeId
   * @return true if update success, otherwise false.
   */
  boolean casUpdateLevelNextNodeId(int level, long expectedNode, long newNode);

  /**
   * Get the next node of the specified node on the corresponding layer，or {@code NIL_NODE_ID}
   *
   * @param level skip list layer
   * @return nodeId, or {@code NIL_NODE_ID}
   */
  long getLevelNextNodeId(int level);
}
