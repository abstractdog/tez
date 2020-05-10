package org.apache.tez.test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenience class for unit tests, which wraps a dfs and mini tez cluster.
 */
public class MiniClusterGroup {
  private static final Logger LOG = LoggerFactory.getLogger(MiniClusterGroup.class);
  private static final AtomicInteger index = new AtomicInteger(-1);
  private MiniTezCluster miniTezCluster = null;
  private MiniDFSCluster dfsCluster = null;
  private FileSystem remoteFs = null;
  private String clusterName;
  private Configuration initialConf;

  public MiniClusterGroup() {
    this(new Configuration(), "MiniClusterGroup" + "_" + index.incrementAndGet());
  }

  public MiniClusterGroup(Configuration conf) {
    this(conf, "MiniClusterGroup" + "_" + index.incrementAndGet());
  }

  public MiniClusterGroup(Configuration conf, String name) {
    this.clusterName = name;
    this.initialConf = conf;

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

    return this;
  }

  public Configuration getConfig() {
    return miniTezCluster.getConfig();
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
