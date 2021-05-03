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

  private static final Path root = new Path("/");
  private static final Path quota1 = new Path("/quota1");
  private static final Path quota2 = new Path("/quota2");
  private static final Path noQuota1 = new Path("/noQuota1");
  private static final Path noQuota2 = new Path("/noQuota2");
  private static final Path file0 = new Path("file0");

  private static final int FILE_SIZE = 1024;
  private static final int NAMESPACE_QUOTA = 1024 * 1024;
  private static final int SPACE_QUOTA = 1024 * 1024;
  private static final int DFS_REPLICATION = 1;
  private static final int DEFAULT_BLOCK_SIZE = 2048;

  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_REPLICATION_KEY, DFS_REPLICATION);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
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
    for (FileStatus fileStatus : hdfs.listStatus(root)) {
      if (hdfs.exists(fileStatus.getPath())) {
        hdfs.delete(fileStatus.getPath(), true);
      }
    }
  }


  @Test
  public void testRenameNoQuotaToQuota() throws Exception {
    // mkdir dir
    clear();
    hdfs.mkdirs(noQuota1);
    hdfs.mkdirs(quota1);
    hdfs.setQuota(quota1, NAMESPACE_QUOTA, SPACE_QUOTA);

    DFSTestUtil.createFile(hdfs, new Path(noQuota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to quota
    rename(new Path(noQuota1, file0), new Path(quota1, file0),
        false, true, Options.Rename.NONE);
    assertEquals(hdfs.getQuotaUsage(quota1).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(root).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(quota1).getFileAndDirectoryCount(), 2);
    assertEquals(hdfs.getQuotaUsage(root).getFileAndDirectoryCount(), 4);

    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(noQuota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to quota
    rename(new Path(noQuota1, file0), new Path(quota1, file0),
        false, true, Options.Rename.OVERWRITE);
    assertEquals(hdfs.getQuotaUsage(quota1).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(root).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(quota1).getFileAndDirectoryCount(), 2);
    assertEquals(hdfs.getQuotaUsage(root).getFileAndDirectoryCount(), 4);
  }

  @Test
  public void testRenameQuotaToNoQuota() throws Exception {
    // mkdir dir
    clear();
    hdfs.mkdirs(noQuota1);
    hdfs.mkdirs(quota1);
    hdfs.setQuota(quota1, NAMESPACE_QUOTA, SPACE_QUOTA);

    DFSTestUtil.createFile(hdfs, new Path(quota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // mv quota to noQuota
    rename(new Path(quota1, file0), new Path(noQuota1, file0),
        false, true, Options.Rename.NONE);
    assertEquals(hdfs.getQuotaUsage(quota1).getSpaceConsumed(), 0);
    assertEquals(hdfs.getQuotaUsage(root).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(quota1).getFileAndDirectoryCount(), 1);
    assertEquals(hdfs.getQuotaUsage(root).getFileAndDirectoryCount(), 4);
    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(quota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // mv quota to noQuota
    rename(new Path(quota1, file0), new Path(noQuota1, file0),
        false, true, Options.Rename.OVERWRITE);

    assertEquals(hdfs.getQuotaUsage(quota1).getSpaceConsumed(), 0);
    assertEquals(hdfs.getQuotaUsage(root).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(quota1).getFileAndDirectoryCount(), 1);
    assertEquals(hdfs.getQuotaUsage(root).getFileAndDirectoryCount(), 4);

  }

  @Test
  public void testRenameNoQuotaToNoQuota() throws Exception {
    // mkdir dir
    clear();
    hdfs.mkdirs(noQuota1);
    hdfs.mkdirs(noQuota2);

    DFSTestUtil.createFile(hdfs, new Path(noQuota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);

    // noQuota mv to noQuota
    rename(new Path(noQuota1, file0), new Path(noQuota2, file0),
        false, true, Options.Rename.NONE);

    assertEquals(hdfs.getQuotaUsage(root).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(root).getFileAndDirectoryCount(), 4);
    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(noQuota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to Quota
    rename(new Path(noQuota1, file0), new Path(noQuota2, file0),
        false, true, Options.Rename.OVERWRITE);
    assert hdfs.exists(new Path(noQuota2, file0));
    assertFalse(hdfs.exists(new Path(noQuota1, file0)));
    assertEquals(hdfs.getQuotaUsage(root).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(root).getFileAndDirectoryCount(), 4);
  }

  @Test
  public void testRenameQuotaToQuota() throws Exception {
    // mkdir dir
    clear();
    hdfs.mkdirs(quota1);
    hdfs.mkdirs(quota2);
    hdfs.setQuota(quota1, NAMESPACE_QUOTA, SPACE_QUOTA);
    hdfs.setQuota(quota2, NAMESPACE_QUOTA, SPACE_QUOTA);

    DFSTestUtil.createFile(hdfs, new Path(quota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to quota
    rename(new Path(quota1, file0), new Path(quota2, file0),
        false, true, Options.Rename.NONE);

    assertEquals(hdfs.getQuotaUsage(quota1).getSpaceConsumed(), 0);
    assertEquals(hdfs.getQuotaUsage(quota2).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(root).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(quota1).getFileAndDirectoryCount(), 1);
    assertEquals(hdfs.getQuotaUsage(quota2).getFileAndDirectoryCount(), 2);
    assertEquals(hdfs.getQuotaUsage(root).getFileAndDirectoryCount(), 4);

    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(quota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    // noQuota mv to quota
    rename(new Path(quota1, file0), new Path(quota2, file0),
        false, true, Options.Rename.OVERWRITE);

    assertEquals(hdfs.getQuotaUsage(quota1).getSpaceConsumed(), 0);
    assertEquals(hdfs.getQuotaUsage(quota2).getSpaceConsumed(), FILE_SIZE);
    assertEquals(hdfs.getQuotaUsage(root).getSpaceConsumed(), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(quota1).getFileAndDirectoryCount(), 1);
    assertEquals(hdfs.getQuotaUsage(quota2).getFileAndDirectoryCount(), 2);
    assertEquals(hdfs.getQuotaUsage(root).getFileAndDirectoryCount(), 4);
  }

  @Test
  public void testRenameStorageType() throws Exception {
    clear();
    hdfs.mkdirs(quota1);
    hdfs.mkdirs(quota2);
    hdfs.setStoragePolicy(quota1, HdfsConstants.HOT_STORAGE_POLICY_NAME);
    hdfs.setStoragePolicy(quota2, HdfsConstants.ALLSSD_STORAGE_POLICY_NAME);
    hdfs.setQuotaByStorageType(quota1, StorageType.DISK, SPACE_QUOTA);
    hdfs.setQuotaByStorageType(quota2, StorageType.SSD, SPACE_QUOTA);

    DFSTestUtil.createFile(hdfs, new Path(quota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);

    assertEquals(hdfs.getQuotaUsage(quota1).
        getTypeConsumed(StorageType.DISK), FILE_SIZE);
    rename(new Path(quota1, file0), new Path(quota2, file0),
        false, true, Options.Rename.NONE);

    assertEquals(hdfs.getQuotaUsage(quota1).
        getTypeConsumed(StorageType.DISK), 0);
    assertEquals(hdfs.getQuotaUsage(quota2).
        getTypeConsumed(StorageType.SSD), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(root).
        getTypeConsumed(StorageType.DISK), 0);
    assertEquals(hdfs.getQuotaUsage(root).
        getTypeConsumed(StorageType.SSD), FILE_SIZE);

    // rename over write
    DFSTestUtil.createFile(hdfs, new Path(quota1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);

    assertEquals(hdfs.getQuotaUsage(quota1).
        getTypeConsumed(StorageType.DISK), FILE_SIZE);

    rename(new Path(quota1, file0), new Path(quota2, file0),
        false, true, Options.Rename.OVERWRITE);

    assertEquals(hdfs.getQuotaUsage(quota1).
        getTypeConsumed(StorageType.DISK), 0);
    assertEquals(hdfs.getQuotaUsage(quota2).
        getTypeConsumed(StorageType.SSD), FILE_SIZE);

    assertEquals(hdfs.getQuotaUsage(root).
        getTypeConsumed(StorageType.DISK), 0);
    assertEquals(hdfs.getQuotaUsage(root).
        getTypeConsumed(StorageType.SSD), FILE_SIZE);
  }

  @Test
  public void testSnapshotRename() throws Exception {

    final Path parent = new Path("/parent");

    hdfs.mkdirs(parent);
    hdfs.setQuota(parent, NAMESPACE_QUOTA, SPACE_QUOTA);
    final Path sub1 = new Path(parent, "sub1");
    hdfs.mkdirs(sub1);

    DFSTestUtil.createFile(hdfs, new Path(sub1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);

    hdfs.allowSnapshot(parent);
    hdfs.createSnapshot(parent, "s0");

    final Path sub2 = new Path(parent, "sub2");
    hdfs.mkdirs(sub2);
    assertEquals(hdfs.getQuotaUsage(parent).getFileAndDirectoryCount(), 4);
    // mv /parent/sub1/file0 to /parent/sub2/file0
    rename(new Path(sub1, file0), new Path(sub2, file0),
        false, true, Options.Rename.NONE);

    assertEquals(hdfs.getQuotaUsage(parent).getFileAndDirectoryCount(), 5);
    // rename overwrite
    DFSTestUtil.createFile(hdfs, new Path(sub1, file0),
        FILE_SIZE, (short) DFS_REPLICATION, 0);
    rename(new Path(sub1, file0), new Path(sub2, file0),
        false, true, Options.Rename.OVERWRITE);
    assertEquals(hdfs.getQuotaUsage(parent).getFileAndDirectoryCount(), 5);
  }

  public void rename(Path src, Path dst, boolean srcExists,
                     boolean dstExists, Options.Rename... options) throws IOException {
    try {
      hdfs.rename(src, dst, options);
    } finally {
      Assert.assertEquals("Source exists", srcExists, hdfs.exists(src));
      Assert.assertEquals("Destination exists", dstExists, hdfs.exists(dst));
    }
  }
}
