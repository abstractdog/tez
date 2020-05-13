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
package org.apache.tez.runtime.library.common.sort.impl;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Test;

public class TestTezSpillRecord {

  @Test
  public void testNonGroupReadableFileEnsuredUmask077() throws Exception {
    testPermission("077", (short) 00600, (short) 00640);
  }

  @Test
  public void testNonGroupReadableFileNotEnsuredUmask022() throws Exception {
    testPermission("022", (short) 00600, (short) 00600);
  }

  @Test
  public void testExpectedPermissionIsNotChangedUmask077() throws Exception {
    testPermission("077", (short) 00640, (short) 00640);
  }

  @Test
  public void testExpectedPermissionIsNotChangedUmask022() throws Exception {
    testPermission("022", (short) 00640, (short) 00640);
  }

  @Test
  public void testAllReadableFileAdjustedUmask077() throws Exception {
    testPermission("077", (short) 00666, (short) 00640);
  }

  @Test
  public void testAllReadableFileIsNotAdjustedUmask022() throws Exception {
    testPermission("022", (short) 00666, (short) 00666);
  }

  private void testPermission(String umask, short originalPermission, short finalPermission)
      throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, umask);

    Path path = new Path(File.createTempFile("spill_umask", null).getAbsolutePath());

    FileSystem fs = path.getFileSystem(conf);
    fs.setPermission(path, FsPermission.createImmutable(originalPermission));

    Assert.assertEquals(FsPermission.createImmutable(originalPermission),
        fs.getFileStatus(path).getPermission());

    TezSpillRecord.ensureSpillFilePermissions(path, conf, path.getFileSystem(conf));

    Assert.assertEquals(FsPermission.createImmutable(finalPermission),
        fs.getFileStatus(path).getPermission());
  }
}
