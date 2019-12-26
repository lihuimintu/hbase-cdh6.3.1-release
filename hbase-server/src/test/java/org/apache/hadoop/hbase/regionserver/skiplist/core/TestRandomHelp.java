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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRandomHelp {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRandomHelp.class);

  @Test
  public void testNormal() throws Exception {
    Random seedGenerator = new Random();
    int randomSeed = seedGenerator.nextInt() | 0x0100;

    int level;
    Map<Integer, AtomicInteger> allLevels = new ConcurrentHashMap<>();
    int loop = 5000000;
    for (int i = 0; i < loop; i++) {

      int x = randomSeed;
      x ^= x << 13;
      x ^= x >>> 17;
      x ^= x << 5;
      randomSeed = x;

      if ((x & 0x8001) == 0) { // test highest and lowest bits
        level = 1;
        x >>>= 1;
        while ((x & 1) != 0) {
          ++level;
          x >>>= 1;
        }

        if (!allLevels.containsKey(level)) {
          allLevels.putIfAbsent(level, new AtomicInteger(0));
        }
        allLevels.get(level).incrementAndGet();

      }
    }

    List<Integer> allLevelList = new ArrayList<>(allLevels.keySet());
    Collections.sort(allLevelList);

    int totalInd = 0;
    int totalLevel = 0;
    int totalSize = 0;
    for (Integer id : allLevelList) {
      int c = allLevels.get(id).get();
      totalInd += c;
      totalLevel += id * c;
      System.out.println(id + "  ===> " + c);
      totalSize += c * 8;
    }
    System.out.println(" totalInd  ===> " + totalInd);
    System.out.println(" totalLevel  ===> " + totalLevel);
    System.out.println(" loop* 26/100  ===> " + (loop / 100 * 26));
    System.out.println(" loop* 24/100  ===> " + (loop / 100 * 24));

    Assert.assertTrue(totalInd >= loop / 100 * 24);
    Assert.assertTrue(totalInd <= loop / 100 * 26);

    System.out.println("jdk index mem: " + totalLevel * 40);
    System.out.println("ccsmap index mem: " + totalSize);
  }
}