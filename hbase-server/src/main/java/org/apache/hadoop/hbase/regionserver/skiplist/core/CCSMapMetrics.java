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
 * TODO Metrics
 */
@InterfaceAudience.Private
public final class CCSMapMetrics {
  /**
   * 大于Chunk size的分配请求数
   */

  /**
   * 碎片率
   */

  /**
   * 同一个Key的写入擦除率
   * 后写入的value大于之前的，重新分配了Chunk来写入，造成该数据被擦除
   */

  /**
   * 同一个Key的写入覆盖成功率
   * 后写入的value小于之前的，直接覆盖写了，但可能留下了碎片（浪费）
   */

  /**
   * Level/Node数
   */

  /**
   * 写入histogram
   */

  /**
   * 读取histogram
   */

  /**
   * Allocator的相关Metrics
   */
}
