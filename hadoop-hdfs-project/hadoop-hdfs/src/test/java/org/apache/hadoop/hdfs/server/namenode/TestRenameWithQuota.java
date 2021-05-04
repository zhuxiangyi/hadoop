/*
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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test rename QuotaCount.
 */
public class TestRenameWithQuota {

  static final short NUM_DATANODES = 1;

  private static final Path ROOT = new Path("/");
  private static final Path QUOTA1 = new Path("/quota1");
  private static final Path QUOTA2 = new Path("/quota2");
  private static final Path NO_QUOTA1 = new Path("/noQuota1");
  private static final Path NO_QUOTA2 = new Path("/noQuota2");
  private static final Path FILE0 = new Path("file0");

  private static final int FILE_SIZE = 1024;
  private static final int NAMESPACE_QUOTA = 1024 * 1024;
  private static final int SPACE_QUOTA = 1024 * 1024;
  private static final int DFS_REPLICATION = 1;
  private static final int DEFAULT_BLOCK_SIZE = 2048;

  private MiniDFSCluster cluster;
  private DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_REPLICATION_KEY, DFS_REPLICATION);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  public void clear() throws IOException {
    for (FileStatus fileStatus : hdfs.listStatus(ROOT)) {
      if (hdfs.exists(fileStatus.getPath())) {
        hdfs.delete(fileStatus.getPath(), true);
      }
    }
  }


  @Test
  public void testRenameNoQuotaToQuota() throws Exception {
    // mkdir dir
    clear();
    hdfs.mkdirs(NO_QUOTA1);
    hdfs.mkdirs(QUOTA1);
    hdfs.setQuota(QUOTA1, NAMESPACE_QUOTA, SPACE_QUOTA);

    DFSTestUtil.createFile(hdfs, new Path(NO_QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to quota
    rename(new Path(NO_QUOTA1, FILE0), new Path(QUOTA1, FILE0),
        false, true, Options.Rename.NONE);
    assertEquals(hdfs.getQuotaUsage(QUOTA1).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(ROOT).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).getFileAndDirectoryCount(), 2);
    assertEquals(hdfs.getQuotaUsage(ROOT).getFileAndDirectoryCount(), 4);

    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(NO_QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to quota
    rename(new Path(NO_QUOTA1, FILE0), new Path(QUOTA1, FILE0),
        false, true, Options.Rename.OVERWRITE);
    assertEquals(hdfs.getQuotaUsage(QUOTA1).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(ROOT).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).getFileAndDirectoryCount(), 2);
    assertEquals(hdfs.getQuotaUsage(ROOT).getFileAndDirectoryCount(), 4);
  }

  @Test
  public void testRenameQuotaToNoQuota() throws Exception {
    // mkdir dir
    clear();
    hdfs.mkdirs(NO_QUOTA1);
    hdfs.mkdirs(QUOTA1);
    hdfs.setQuota(QUOTA1, NAMESPACE_QUOTA, SPACE_QUOTA);

    DFSTestUtil.createFile(hdfs, new Path(QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // mv quota to noQuota
    rename(new Path(QUOTA1, FILE0), new Path(NO_QUOTA1, FILE0),
        false, true, Options.Rename.NONE);
    assertEquals(hdfs.getQuotaUsage(QUOTA1).getSpaceConsumed(), 0);
    assertEquals(hdfs.getQuotaUsage(ROOT).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).getFileAndDirectoryCount(), 1);
    assertEquals(hdfs.getQuotaUsage(ROOT).getFileAndDirectoryCount(), 4);
    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // mv quota to noQuota
    rename(new Path(QUOTA1, FILE0), new Path(NO_QUOTA1, FILE0),
        false, true, Options.Rename.OVERWRITE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).getSpaceConsumed(), 0);
    assertEquals(hdfs.getQuotaUsage(ROOT).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).getFileAndDirectoryCount(), 1);
    assertEquals(hdfs.getQuotaUsage(ROOT).getFileAndDirectoryCount(), 4);

  }

  @Test
  public void testRenameNoQuotaToNoQuota() throws Exception {
    // mkdir dir
    clear();
    hdfs.mkdirs(NO_QUOTA1);
    hdfs.mkdirs(NO_QUOTA2);

    DFSTestUtil.createFile(hdfs, new Path(NO_QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);

    // noQuota mv to noQuota
    rename(new Path(NO_QUOTA1, FILE0), new Path(NO_QUOTA2, FILE0),
        false, true, Options.Rename.NONE);

    assertEquals(hdfs.getQuotaUsage(ROOT).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(ROOT).getFileAndDirectoryCount(), 4);
    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(NO_QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to Quota
    rename(new Path(NO_QUOTA1, FILE0), new Path(NO_QUOTA2, FILE0),
        false, true, Options.Rename.OVERWRITE);
    assert hdfs.exists(new Path(NO_QUOTA2, FILE0));
    assertFalse(hdfs.exists(new Path(NO_QUOTA1, FILE0)));
    assertEquals(hdfs.getQuotaUsage(ROOT).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(ROOT).getFileAndDirectoryCount(), 4);
  }

  @Test
  public void testRenameQuotaToQuota() throws Exception {
    // mkdir dir
    clear();
    hdfs.mkdirs(QUOTA1);
    hdfs.mkdirs(QUOTA2);
    hdfs.setQuota(QUOTA1, NAMESPACE_QUOTA, SPACE_QUOTA);
    hdfs.setQuota(QUOTA2, NAMESPACE_QUOTA, SPACE_QUOTA);

    DFSTestUtil.createFile(hdfs, new Path(QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to quota
    rename(new Path(QUOTA1, FILE0), new Path(QUOTA2, FILE0),
        false, true, Options.Rename.NONE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).getSpaceConsumed(), 0);
    assertEquals(hdfs.getQuotaUsage(QUOTA2).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(ROOT).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).getFileAndDirectoryCount(), 1);
    assertEquals(hdfs.getQuotaUsage(QUOTA2).getFileAndDirectoryCount(), 2);
    assertEquals(hdfs.getQuotaUsage(ROOT).getFileAndDirectoryCount(), 4);

    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to quota
    rename(new Path(QUOTA1, FILE0), new Path(QUOTA2, FILE0),
        false, true, Options.Rename.OVERWRITE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).getSpaceConsumed(), 0);
    assertEquals(hdfs.getQuotaUsage(QUOTA2).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(ROOT).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).getFileAndDirectoryCount(), 1);
    assertEquals(hdfs.getQuotaUsage(QUOTA2).getFileAndDirectoryCount(), 2);
    assertEquals(hdfs.getQuotaUsage(ROOT).getFileAndDirectoryCount(), 4);
  }

  @Test
  public void testRenameStorageType() throws Exception {
    clear();
    hdfs.mkdirs(QUOTA1);
    hdfs.mkdirs(QUOTA2);
    hdfs.setStoragePolicy(QUOTA1, HdfsConstants.HOT_STORAGE_POLICY_NAME);
    hdfs.setStoragePolicy(QUOTA2, HdfsConstants.ALLSSD_STORAGE_POLICY_NAME);
    hdfs.setQuotaByStorageType(QUOTA1, StorageType.DISK, SPACE_QUOTA);
    hdfs.setQuotaByStorageType(QUOTA2, StorageType.SSD, SPACE_QUOTA);

    DFSTestUtil.createFile(hdfs, new Path(QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).
        getTypeConsumed(StorageType.DISK), FILE_SIZE);
    rename(new Path(QUOTA1, FILE0), new Path(QUOTA2, FILE0),
        false, true, Options.Rename.NONE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).
        getTypeConsumed(StorageType.DISK), 0);
    assertEquals(hdfs.getQuotaUsage(QUOTA2).
        getTypeConsumed(StorageType.SSD), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(ROOT).
        getTypeConsumed(StorageType.DISK), 0);
    assertEquals(hdfs.getQuotaUsage(ROOT).
        getTypeConsumed(StorageType.SSD), FILE_SIZE);

    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(QUOTA1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).
        getTypeConsumed(StorageType.DISK), FILE_SIZE);

    rename(new Path(QUOTA1, FILE0), new Path(QUOTA2, FILE0),
        false, true, Options.Rename.OVERWRITE);

    assertEquals(hdfs.getQuotaUsage(QUOTA1).
        getTypeConsumed(StorageType.DISK), 0);
    assertEquals(hdfs.getQuotaUsage(QUOTA2).
        getTypeConsumed(StorageType.SSD), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(ROOT).
        getTypeConsumed(StorageType.DISK), 0);
    assertEquals(hdfs.getQuotaUsage(ROOT).
        getTypeConsumed(StorageType.SSD), FILE_SIZE);
  }

  @Test
  public void testSnapshotRename() throws Exception {

    final Path parent = new Path("/parent");

    hdfs.mkdirs(parent);
    hdfs.setQuota(parent, NAMESPACE_QUOTA, SPACE_QUOTA);
    final Path sub1 = new Path(parent, "sub1");
    hdfs.mkdirs(sub1);

    DFSTestUtil.createFile(hdfs, new Path(sub1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);

    hdfs.allowSnapshot(parent);
    hdfs.createSnapshot(parent, "s0");

    final Path sub2 = new Path(parent, "sub2");
    hdfs.mkdirs(sub2);
    assertEquals(hdfs.getQuotaUsage(parent).getFileAndDirectoryCount(), 4);
    // mv /parent/sub1/FILE0 to /parent/sub2/FILE0
    rename(new Path(sub1, FILE0), new Path(sub2, FILE0),
        false, true, Options.Rename.NONE);

    assertEquals(hdfs.getQuotaUsage(parent).getFileAndDirectoryCount(), 5);
    // rename overwrite
    DFSTestUtil.createFile(hdfs, new Path(sub1, FILE0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    rename(new Path(sub1, FILE0), new Path(sub2, FILE0),
        false, true, Options.Rename.OVERWRITE);
    assertEquals(hdfs.getQuotaUsage(parent).getFileAndDirectoryCount(), 5);
  }

  public void rename(
      Path src, Path dst, boolean srcExists, boolean dstExists,
      Options.Rename... options) throws IOException {
    try {
      hdfs.rename(src, dst, options);
    } finally {
      Assert.assertEquals("Source exists", srcExists, hdfs.exists(src));
      Assert.assertEquals("Destination exists", dstExists, hdfs.exists(dst));
    }
  }
}
