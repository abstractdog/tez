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
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.apache.tez.test.MiniTezCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TestBroadcastEdgeManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestBroadcastEdgeManager.class);

  private static Configuration conf = new Configuration();
  private static MiniTezCluster miniTezCluster;
  private static String TEST_ROOT_DIR =
      "target" + Path.SEPARATOR + TestBroadcastEdgeManager.class.getName() + "-tmpDir";
  protected static MiniDFSCluster dfsCluster;

  private static TezClient tezSession = null;
  private static TezConfiguration tezConf;

  @BeforeClass
  public static void beforeClass() throws Exception {
    LOG.info("Starting mini clusters");
    FileSystem remoteFs = null;
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
    if (miniTezCluster == null) {
      miniTezCluster = new MiniTezCluster(TestBroadcastEdgeManager.class.getName(), 3, 1, 1);
      Configuration miniTezconf = new Configuration(conf);
      miniTezconf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      miniTezCluster.init(miniTezconf);
      miniTezCluster.start();

      Path remoteStagingDir = remoteFs
          .makeQualified(new Path(TEST_ROOT_DIR, String.valueOf(new Random().nextInt(100000))));
      TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);

      tezConf = new TezConfiguration(miniTezCluster.getConfig());
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

      tezSession = TezClient.create("TestBroadcastEdgeManager", tezConf, true);
      tezSession.start();
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    LOG.info("Stopping mini clusters");
    if (tezSession != null) {
      tezSession.stop();
    }
    if (miniTezCluster != null) {
      miniTezCluster.stop();
      miniTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  public static class InputGenProcessor extends SimpleProcessor {

    final int bytesToGenerate;

    public InputGenProcessor(ProcessorContext context) {
      super(context);
      bytesToGenerate = context.getUserPayload().getPayload().getInt(0);
    }

    @Override
    public void run() throws Exception {
      Random random = new Random();
      Preconditions.checkArgument(getOutputs().size() == 1);
      LogicalOutput out = getOutputs().values().iterator().next();
      if (out instanceof UnorderedKVOutput) {
        UnorderedKVOutput output = (UnorderedKVOutput) out;
        KeyValueWriter kvWriter = output.getWriter();
        int approxNumInts = bytesToGenerate / 6;
        for (int i = 0; i < approxNumInts; i++) {
          kvWriter.write(NullWritable.get(), new IntWritable(random.nextInt()));
        }
      }
    }
  }

  public static class InputFetchProcessor extends SimpleProcessor {
    public InputFetchProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(inputs.size() == 1);
      KeyValueReader broadcastKvReader =
          (KeyValueReader) getInputs().values().iterator().next().getReader();
      long sum = 0;
      int count = 0;
      while (broadcastKvReader.next()) {
        count++;
        sum += ((IntWritable) broadcastKvReader.getCurrentValue()).get();
      }
      System.err.println("Count = " + getContext().getTaskIndex() + " * " + count + ", Sum=" + sum);
    }
  }

  @Test
  public void testDAGWithBroadcastEdgeDataLimit() throws Exception {
    ByteBuffer payload = ByteBuffer.allocate(4);
    payload.putInt(0, 4);

    Vertex broadcastVertex = Vertex.create("DataGen", ProcessorDescriptor
        .create(InputGenProcessor.class.getName()).setUserPayload(UserPayload.create(payload)), 1);
    Vertex fetchVertex = Vertex.create("FetchVertex",
        ProcessorDescriptor.create(InputFetchProcessor.class.getName()), 1);
    UnorderedKVEdgeConfig edgeConf =
        UnorderedKVEdgeConfig.newBuilder(NullWritable.class.getName(), IntWritable.class.getName())
            .setCompression(false, null, null).build();

    DAG dag = DAG.create("BroadcastLoadGen");
    dag.addVertex(broadcastVertex).addVertex(fetchVertex).addEdge(
        Edge.create(broadcastVertex, fetchVertex, edgeConf.createDefaultBroadcastEdgeProperty()));

    runDag(dag);
  }

  private void runDag(DAG dag) throws Exception {
    tezSession.waitTillReady();
    DAGClient dagClient = tezSession.submitDAG(dag);
    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for dag to complete. Sleeping for 500ms." + " DAG name: " + dag.getName()
          + " DAG appContext: " + dagClient.getExecutionContext() + " Current state: "
          + dagStatus.getState());
      Thread.sleep(100);
      dagStatus = dagClient.getDAGStatus(null);
    }
  }
}
