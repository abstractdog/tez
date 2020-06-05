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
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test class for TezSpillRecord.
 */
public class TestTezSpillRecord {

  @Before
  public void beforeTest() throws Exception {
    FileSystem fs = new Path(File.createTempFile("spill_umask", null).getAbsolutePath())
        .getFileSystem(new Configuration());
    System.out.println("before test fs umask: " + FsPermission.getUMask(fs.getConf()));
  }

  @After
  public void afterTest() throws Exception {
    FileSystem fs = new Path(File.createTempFile("spill_umask", null).getAbsolutePath())
        .getFileSystem(new Configuration());
    System.out.println("after test fs umask: " + FsPermission.getUMask(fs.getConf()));
  }

  /*
   * Typical usecase, restrictive umask found, so ensureSpillFilePermissions will take care of group
   * readability.
   */
  @Test
  public void testNonGroupReadableFileEnsuredUmask077() throws Exception {
    testPermission("077", true, (short) 00600, (short) 00640);
  }

  /*
   * This scenario doesn't seem to be valid. However 600 final permission might not be readable for
   * shuffling because of lack of group readability, 600 initial permission is not supposed to
   * happen in case of 022 umask.
   */
  @Test
  public void testNonGroupReadableFileNotEnsuredUmask022() throws Exception {
    testPermission("022", false, (short) 00600, (short) 00600);
  }

  /*
   * Here ensureSpillFilePermissions is supposed to touch but not supposed to change already correct
   * 640 permission.
   */
  @Test
  public void testExpectedPermissionIsNotChangedUmask077() throws Exception {
    testPermission("077", true, (short) 00640, (short) 00640);
  }

  /*
   * In this case ensureSpillFilePermissions is not supposed to touch permission because of
   * permissive umask (022).
   */
  @Test
  public void testExpectedPermissionIsNotChangedUmask022() throws Exception {
    testPermission("022", false, (short) 00640, (short) 00640);
  }

  /*
   * Here ensureSpillFilePermissions will set the correct 640 permission because of restrictive
   * umask (077).
   */
  @Test
  public void testAllReadableFileAdjustedUmask077() throws Exception {
    testPermission("077", true, (short) 00666, (short) 00640);
  }

  /*
   * In this case ensureSpillFilePermissions is not supposed to touch permission because of
   * permissive umask (022).
   */
  @Test
  public void testAllReadableFileIsNotAdjustedUmask022() throws Exception {
    testPermission("022", false, (short) 00666, (short) 00666);
  }

  private void testPermission(String umask, boolean expectedAdjusted, short originalPermission,
      short finalPermission) throws Exception {
    System.out.println(umask);

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, umask);

    Path path = new Path(File.createTempFile("spill_umask", null).getAbsolutePath());

    FileSystem fs = path.getFileSystem(conf);
    fs.setPermission(path, FsPermission.createImmutable(originalPermission));
    System.out.println("conf perm: " + FsPermission.getUMask(conf));
    System.out.println("fs.getConf perm: " + FsPermission.getUMask(fs.getConf()));

    Assert.assertEquals(FsPermission.createImmutable(originalPermission),
        fs.getFileStatus(path).getPermission());

    // if umask is restrictive (077), ensureSpillFilePermissions will adjust output file's
    // permission
    boolean adjusted = TezSpillRecord.ensureSpillFilePermissions(path, fs);

    //Assert.assertEquals(expectedAdjusted, adjusted);
    Assert.assertEquals(FsPermission.createImmutable(finalPermission),
        fs.getFileStatus(path).getPermission());
  }
}
