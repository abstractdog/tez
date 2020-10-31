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
package org.apache.tez.dag.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEvent;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleans intermediate shuffle data in FS based shuffle implementation.
 * For FS based shuffle, all shuffle data is put in this folder in FileSystem:
 *   <fs_prefix>/<application_id>/<dag_id>/xxx
 *
 * FsBasedShuffleDataCleaner does two things:
 * 1) When DAGAppMaster is stopped, it cleans the
 * <fs_prefix>/<application_id> folder.
 *
 * 2) When a DAG is finished, cleans the <fs_prefix>/<application_id>/<dag_id>
 * folder.
 */
public class FsBasedShuffleDataCleaner extends AbstractService
    implements EventHandler<DAGAppMasterEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(FsBasedShuffleDataCleaner.class);
  private String appId;
  private Path appPath;
  private FileSystem fs;

  public FsBasedShuffleDataCleaner(AppContext context) {
    super(FsBasedShuffleDataCleaner.class.toString());
    appId = context.getApplicationID().toString();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    String root = TezCommonUtils.getTezFSBasedShufflePath(conf);
    appPath = new Path(root, appId);
    LOG.info("Application shuffle folder: " + appPath.toString());
    fs = appPath.getFileSystem(getConfig());
  }

  @Override
  public void handle(DAGAppMasterEvent event) {
    if(event.getType() == DAGAppMasterEventType.DAG_FINISHED) {
      //DAG finished
      assert event instanceof DAGAppMasterEventDAGFinished;
      DAGAppMasterEventDAGFinished finishEvt =
          (DAGAppMasterEventDAGFinished) event;
      String strId = "" + finishEvt.getDAGId().getId();
      deleteFolderIgnoreErrors(new Path(appPath, strId));
    }
  }

  private void deleteFolderIgnoreErrors(Path path) {
    try {
      fs.delete(path, true);
    } catch (IOException ie) {
      LOG.error("IOException in clean shuffle dir: " + path.toString(), ie);
    }
  }
}
