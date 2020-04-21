/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.tez.dag.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A ReentrantReadWriteLock which maintains some data about lock contention.
 */
public class InstrumentedReentrantReadWriteLock extends ReentrantReadWriteLock {
  private static final long serialVersionUID = 1L;

  private final InstrumentedReentrantReadWriteLock.InstrumentedReadLock readerLock;
  private final InstrumentedReentrantReadWriteLock.InstrumentedWriteLock writerLock;

  public InstrumentedReentrantReadWriteLock() {
    this(false);
  }

  public InstrumentedReentrantReadWriteLock(boolean fair) {
    super(fair);
    readerLock = new InstrumentedReadLock(this);
    writerLock = new InstrumentedWriteLock(this);
  }

  public InstrumentedReentrantReadWriteLock.InstrumentedWriteLock writeLock() {
    return writerLock;
  }

  public InstrumentedReentrantReadWriteLock.InstrumentedReadLock readLock() {
    return readerLock;
  }

  public Map<String, Object> getStatistics() {
    Map<String, Object> statistics = new HashMap<>();
    // number of lock occurrences
    statistics.put("READ_COUNT_LOCK_ACQUIRED_BY_LOCK_METHOD", readerLock.lockBlockingTimes.size());
    statistics.put("WRITE_COUNT_LOCK_ACQUIRED_BY_LOCK_METHOD", writerLock.lockBlockingTimes.size());

    // average time waiting for the lock
    statistics.put("READ_AVG_LOCK_BLOCKED_TIMES_MS",
        readerLock.lockBlockingTimes.stream().mapToInt(l -> l.intValue()).average().getAsDouble()
            / 1000);
    statistics.put("WRITE_AVG_LOCK_BLOCKED_TIMES_MS",
        writerLock.lockBlockingTimes.stream().mapToInt(l -> l.intValue()).average().getAsDouble()
            / 1000);

    // max time waiting for the lock
    statistics.put("READ_MAX_LOCK_BLOCKED_TIME_MS",
        (double) readerLock.lockBlockingTimes.stream().mapToInt(l -> l.intValue()).max().getAsInt()
            / 1000);
    statistics.put("WRITE_MAX_LOCK_BLOCKED_TIME_MS",
        (double) writerLock.lockBlockingTimes.stream().mapToInt(l -> l.intValue()).max().getAsInt()
            / 1000);

    // sum time waiting for the lock
    statistics.put("READ_SUM_LOCK_BLOCKED_TIMES_MS",
        (double) readerLock.lockBlockingTimes.stream().mapToInt(l -> l.intValue()).sum() / 1000);
    statistics.put("WRITE_SUM_LOCK_BLOCKED_TIME_MS",
        (double) writerLock.lockBlockingTimes.stream().mapToInt(l -> l.intValue()).sum() / 1000);
    return statistics;
  }

  public static class InstrumentedReadLock extends ReentrantReadWriteLock.ReadLock {
    private static final long serialVersionUID = 1L;

    private List<Long> lockBlockingTimes = new ArrayList<>();

    protected InstrumentedReadLock(ReentrantReadWriteLock lock) {
      super(lock);
    }

    @Override
    public void lock() {
      long nanos = System.nanoTime();
      super.lock();
      lockBlockingTimes.add(System.nanoTime() - nanos);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      super.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
      return super.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      return super.tryLock(time, unit);
    }

    @Override
    public void unlock() {
      super.unlock();
    }

    @Override
    public Condition newCondition() {
      return super.newCondition();
    }
  }

  public static class InstrumentedWriteLock extends ReentrantReadWriteLock.WriteLock {
    private static final long serialVersionUID = 1L;

    private List<Long> lockBlockingTimes = new ArrayList<>();

    protected InstrumentedWriteLock(ReentrantReadWriteLock lock) {
      super(lock);
    }

    @Override
    public void lock() {
      long nanos = System.nanoTime();
      super.lock();
      lockBlockingTimes.add(System.nanoTime() - nanos);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      super.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
      return super.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      return super.tryLock(time, unit);
    }

    @Override
    public void unlock() {
      super.unlock();
    }

    @Override
    public Condition newCondition() {
      return super.newCondition();
    }
  }
}
