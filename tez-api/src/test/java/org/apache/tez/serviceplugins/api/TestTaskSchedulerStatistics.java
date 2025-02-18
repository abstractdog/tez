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
package org.apache.tez.serviceplugins.api;

import org.apache.hadoop.util.Time;
import org.apache.tez.serviceplugins.api.TaskSchedulerStatistics.TaskRequestData;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.counters.DAGCounter;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestTaskSchedulerStatistics {

  @Test
  public void testTrackRequestPendingTimeAggregate() {
    TaskSchedulerStatistics stats = new TaskSchedulerStatistics();
    TaskRequestData request1 = () -> Time.now() - 100;

    stats.trackRequestPendingTime(request1);

    Assert.assertTrue(stats.sumPendingTime > 0);
    Assert.assertTrue(stats.maxPendingTime > 0);
    Assert.assertEquals(1, stats.pendingTaskRequestSamples);
    Assert.assertEquals(0, stats.averagePendingTime);

    stats.aggregate();

    Assert.assertTrue(stats.averagePendingTime > 0);
  }

  @Test
  public void testAddStatistics() {
    TaskSchedulerStatistics stats = new TaskSchedulerStatistics();
    TaskSchedulerStatistics otherStats = new TaskSchedulerStatistics();
    TaskRequestData request1 = () -> Time.now() - 300;
    TaskRequestData request2 = () -> Time.now() - 400;

    stats.trackRequestPendingTime(request1);
    stats.aggregate();

    int sumPendingTimeBeforeAdd = stats.sumPendingTime;

    otherStats.trackRequestPendingTime(request2);
    stats.add(otherStats);

    Assert.assertEquals(2, stats.pendingTaskRequestSamples);
    Assert.assertTrue(stats.sumPendingTime > sumPendingTimeBeforeAdd);
  }

  @Test
  public void testGetCounters() {
    TaskSchedulerStatistics stats = new TaskSchedulerStatistics();
    TaskRequestData request = () -> Time.now() - 500;
    stats.trackRequestPendingTime(request);
    stats.aggregate();

    TezCounters counters = stats.getCounters();

    assertTrue(counters.findCounter(DAGCounter.TASK_SCHEDULER_MAX_PENDING_TIME_MS).getValue() >= 0);
    assertTrue(counters.findCounter(DAGCounter.TASK_SCHEDULER_SUM_PENDING_TIME_MS).getValue() >= 0);
    assertTrue(counters.findCounter(DAGCounter.TASK_SCHEDULER_AVG_PENDING_TIME_MS).getValue() >= 0);
  }

  @Test
  public void testClear() {
    TaskSchedulerStatistics stats = new TaskSchedulerStatistics();
    TaskRequestData request = () -> Time.now() - 100;
    stats.trackRequestPendingTime(request);
    stats.aggregate();
    stats.clear();

    Assert.assertEquals(0, stats.maxPendingTime);
    Assert.assertEquals(0, stats.sumPendingTime);
    Assert.assertEquals(0, stats.averagePendingTime);
    Assert.assertEquals(0, stats.pendingTaskRequestSamples);
  }

  @Test
  public void testTrackRequestPendingTimeWithNullRequest() {
    TaskSchedulerStatistics stats = new TaskSchedulerStatistics();
    stats.trackRequestPendingTime(null);

    Assert.assertEquals(0, stats.maxPendingTime);
  }
}
