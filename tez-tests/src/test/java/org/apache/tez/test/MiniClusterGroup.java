/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenience class for unit tests, which wraps a dfs and mini tez cluster and takes care of
 * overriding incoming conf properties with optimized ones (for tests). User can still override
 * optimized conf by changing the Configuration object returned by getConfig() before
 * starting the cluster with start().
 */
public class MiniClusterGroup {
  private static final Logger LOG = LoggerFactory.getLogger(MiniClusterGroup.class);
  private static final AtomicInteger index = new AtomicInteger(-1);
  private MiniTezCluster miniTezCluster = null;
  private MiniDFSCluster dfsCluster = null;
  private FileSystem remoteFs = null;
  private String clusterName;
  private Configuration initialConf;
  private boolean started = false;

  public MiniClusterGroup() {
    this(new Configuration(), "MiniClusterGroup" + "_" + index.incrementAndGet());
  }

  public MiniClusterGroup(Configuration conf) {
    this(conf, "MiniClusterGroup" + "_" + index.incrementAndGet());
  }

  public MiniClusterGroup(Configuration conf, String name) {
    this.clusterName = name;
    this.initialConf = conf;

    initialConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    initialConf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, 0);
    initialConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY, 1000);
    initialConf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
    initialConf.setLong(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 0l);
    initialConf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 0l);
    initialConf.setLong(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 1);

    String testRootDir = "target" + Path.SEPARATOR + clusterName + "-tmpDir";
    initialConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testRootDir);
  }

  public MiniClusterGroup start() {
    LOG.info("Starting mini clusters");
    try {
      dfsCluster = new MiniDFSCluster.Builder(initialConf).numDataNodes(1).storagesPerDatanode(1)
          .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
    miniTezCluster = new MiniTezCluster(clusterName, 1, 1, 1);
    Configuration miniTezconf = new Configuration(initialConf);
    miniTezconf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
    miniTezCluster.init(miniTezconf);
    miniTezCluster.start();

    started = true;
    return this;
  }

  public Configuration getConfig() {
    return started ? miniTezCluster.getConfig() : initialConf;
  }

  public FileSystem getFs() {
    return remoteFs;
  }

  public void stop() {
    if (miniTezCluster != null) {
      try {
        LOG.info("Stopping MiniTezCluster");
        miniTezCluster.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (dfsCluster != null) {
      try {
        LOG.info("Stopping DFSCluster");
        dfsCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
