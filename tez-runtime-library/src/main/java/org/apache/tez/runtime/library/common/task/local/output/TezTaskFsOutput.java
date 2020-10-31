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
package org.apache.tez.runtime.library.common.task.local.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.runtime.api.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezTaskFsOutput extends TezTaskOutputFiles {
  private static final Logger LOG = LoggerFactory.getLogger(TezTaskFsOutput.class);
  private static String outputLocation = null;
  private Path attemptOutput = null;
  // Under DAG folder, how many sub folders to create to put attempt folders in.
  // This is to avoid a flatten folder structure that contains 100K task attempts.
  private static final int NUM_DAG_SUB_FOLDERS = 1000;

  public TezTaskFsOutput(Configuration conf, TaskContext context, int dagID) {
    super(conf, context, dagID);
    LOG.info("Using FileSystem based shuffle output");
    attemptOutput = getOutputPrefix(conf, context);
  }

  public static Path getOutputPrefix(Configuration conf, TaskContext context) {
    if (outputLocation == null) {
      outputLocation = TezCommonUtils.getTezFSBasedShufflePath(conf);
    }
    String attemptId = context.getUniqueIdentifier();
    String hashCode = String.format("%08d", (attemptId.hashCode() % NUM_DAG_SUB_FOLDERS));
    String subPath = context.getApplicationId().toString() + Path.SEPARATOR
        + context.getDagIdentifier() + Path.SEPARATOR + hashCode + Path.SEPARATOR + attemptId;
    return new Path(outputLocation, subPath);
  }

  @Override
  public Path getOutputFileForWrite(long size) throws IOException {
    return new Path(attemptOutput, "file.out");
  }

  @Override
  public Path getOutputFileForWrite() throws IOException {
    return new Path(attemptOutput, "file.out");
  }

  @Override
  public Path getOutputFileForWriteInVolume(Path existing) {
    return new Path(attemptOutput, "file.out");
  }

  @Override
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    return new Path(attemptOutput, "file.out.index");
  }

  @Override
  public Path getOutputIndexFileForWriteInVolume(Path existing) {
    return new Path(attemptOutput, "file.out.index");
  }

}
