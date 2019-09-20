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

package org.apache.tez.dag.library.edgemanager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.examples.HashJoinExample;
import org.apache.tez.examples.JoinDataGen;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.apache.tez.test.MiniTezCluster;
import org.apache.tez.test.TestTezJobs;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TestBroadcastEdgeManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestBroadcastEdgeManager.class);

  protected static MiniTezCluster mrrTezCluster;
  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestBroadcastEdgeManager.class.getName()
      + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).racks(null)
          .build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (mrrTezCluster == null) {
      mrrTezCluster = new MiniTezCluster(TestTezJobs.class.getName(), 1, 1, 1);
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      conf.setLong(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 500);
      mrrTezCluster.init(conf);
      mrrTezCluster.start();
    }

  }

  @AfterClass
  public static void tearDown() {
    if (mrrTezCluster != null) {
      mrrTezCluster.stop();
      mrrTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
    // TODO Add cleanup code.
  }
  
  @Test
  public void testDAGWithBroadcastEdgeDataLimit() throws Exception {

    Path testDir = new Path("/tmp/testDAGWithBroadcastEdgeDataLimit");
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    remoteFs.mkdirs(stagingDirPath);
    remoteFs.mkdirs(testDir);

    Path dataPath1 = new Path(testDir, "inPath1");
    Path dataPath2 = new Path(testDir, "inPath2");
    Path expectedOutputPath = new Path(testDir, "expectedOutputPath");
    Path outPath = new Path(testDir, "outPath");

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());

    TezClient tezSession = null;
    try {
      tezSession = TezClient.create("HashJoinExampleSession", tezConf, true);
      tezSession.start();

      JoinDataGen dataGen = new JoinDataGen();
      String[] dataGenArgs = new String[] { "-counter", dataPath1.toString(), "1048576",
          dataPath2.toString(), "32", expectedOutputPath.toString(), "2" };
      assertEquals(0, dataGen.run(tezConf, dataGenArgs, tezSession));

      HashJoinExample joinExample = new HashJoinExample();
      String[] args = new String[] { dataPath1.toString(), dataPath2.toString(), "1",
          outPath.toString(), "doBroadcast" };

      assertEquals(0, joinExample.run(tezConf, args, tezSession));
    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }
}
