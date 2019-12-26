/**
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

import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.BYTE_MARK;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.DEFAULT_LEVEL;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.MAX_LEVEL;
import static org.apache.hadoop.hbase.regionserver.skiplist.core.Constant.NIL_NODE_ID;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Index include the level skip index and link index
 * not thread safe
 */
@InterfaceAudience.Private
public class OnHeapIndexHeader implements ILevelIndexHeader {

  //max level is 255.
  private volatile int level;
  //level Index Head
  private volatile AtomicLong[] levelNodeNext = new AtomicLong[DEFAULT_LEVEL];
  //Link Node Head
  private final AtomicLong nodeNext = new AtomicLong(NIL_NODE_ID);

  public OnHeapIndexHeader() {
    this.level = 1;
    initLevelIndex(this.levelNodeNext);
  }

  private void initLevelIndex(AtomicLong[] levelIndex) {
    for (int i = 0; i < levelIndex.length; i++) {
      levelIndex[i] = new AtomicLong(NIL_NODE_ID);
    }
  }

  @Override
  public int getLevel() {
    return level & BYTE_MARK;
  }

  @VisibleForTesting
  public List<Long> getLevelNodeNext() {
    return Arrays.stream(levelNodeNext).map(AtomicLong::get).collect(Collectors.toList());
  }

  /**
   * not thread safe
   */
  @Override
  public void updateLevel(int dstLevel) {
    Preconditions
        .checkArgument(dstLevel <= MAX_LEVEL, "dst level>" + MAX_LEVEL + ", It's unbelievable.");
    Preconditions.checkArgument(dstLevel >= 1, "dst level less than 1.");
    Preconditions.checkArgument(dstLevel <= this.level + 1,
        "updateLevel can't skip. now level=" + this.level + " ,dst level=" + dstLevel);

    //default Level is 32, support 4G number of objects，it's rare to expand
    if (levelNodeNext.length < dstLevel) {
      AtomicLong[] tmpLevelIndex = new AtomicLong[this.levelNodeNext.length * 2];
      initLevelIndex(tmpLevelIndex);
      System.arraycopy(this.levelNodeNext, 0, tmpLevelIndex, 0, this.levelNodeNext.length);
      this.levelNodeNext = tmpLevelIndex;
    }
    if (this.level < dstLevel) {
      this.level = dstLevel & BYTE_MARK;
    }
  }

  @Override
  public long getLevelNextNodeId(int level) {
    Preconditions.checkArgument(level < this.levelNodeNext.length, "invalid overflow level");
    if (level == 0) {
      return this.nodeNext.get();
    }
    return this.levelNodeNext[level - 1].get();
  }

  @Override
  public boolean casUpdateLevelNextNodeId(int level, long expectedNode, long newNode) {
    Preconditions.checkArgument(level < this.levelNodeNext.length, "invalid overflow level");
    if (level == 0) {
      return this.nodeNext.compareAndSet(expectedNode, newNode);
    }
    AtomicLong levelNext = levelNodeNext[level - 1];
    return levelNext.compareAndSet(expectedNode, newNode);
  }
}
