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

package com.chinamobile.bcbsp.comm;

import com.chinamobile.bcbsp.comm.io.util.MessageBytePoolPerPartition;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Disk manager is used to manager the messages disk catch.
 * Every partition has its messages catch file on disk.
 */
public class DiskManager {
  /** class logger. */
  public static final Log LOG = LogFactory.getLog(DiskManager.class);
  /** the directory of worker for disk catch. */
  private static String workerDir = "/tmp/bcbsp";
  /** counter list of partitions. */
  private static int[] counter;
  
  /**
   * Constructor of DiskManager.
   * @param baseDir
   * @param jobId
   * @param partitionId
   * @param partitionBucketNum
   */
  public DiskManager(String baseDir, String jobId, int partitionId,
      int partitionBucketNum) {
    String tmpString = baseDir == null ? this.workerDir : baseDir;
    this.workerDir = tmpString + "/" + jobId + "/" + "Partition" + "-"
        + partitionId;
    this.workerDir = this.workerDir + "/" + "Messages";
    LOG.info("<Disk Directory For Storing Messages>---  " + this.workerDir);
    initialize(partitionBucketNum);
  }
  
  /**
   * Constructor of DiskManager.
   * @param dir
   * @param partitionBucketNum
   */
  public DiskManager(String dir, int partitionBucketNum) {
    this(partitionBucketNum);
    this.workerDir = dir;
  }
  
  /**
   * Constructor of DiskManager.
   * @param partitionBucketNum
   */
  public DiskManager(int partitionBucketNum) {
    // Noting To Do. Default BASE_DIR
    initialize(partitionBucketNum);
  }
  
  /** Initialize the Disk Manager with some parameters. */
  private void initialize(int partitionBucketNum) {
    this.counter = new int[partitionBucketNum];
    for (int i = 0; i < partitionBucketNum; i++) {
      this.counter[i] = 0;
    }
    File f = new File(this.workerDir);
    if (!f.exists()) {
      f.mkdirs();
    }
  }
  
  /**
   * Save messages of every partition on disk.
   * @param msgList
   * @param superStep
   * @param srcPartitionDstBucket
   * @throws IOException e
   */
  public void processMessagesSave(ArrayList<IMessage> msgList, int superStep,
      int srcPartitionDstBucket) throws IOException {
    int count = msgList.size();
    int bucket = PartitionRule.getBucket(srcPartitionDstBucket);
    int srcPartition = PartitionRule.getPartition(srcPartitionDstBucket);
    String fileName = "srcPartition-" + srcPartition + "_" + "counter" + "-"
        + counter[srcPartitionDstBucket]++ + "_" + "count" + "-" + count;
    String tmpPath = this.workerDir + "/" + "superstep-" + superStep + "/"
        + "bucket-" + bucket;
    File tmpFile = new File(tmpPath);
    if (!tmpFile.exists()) {
      tmpFile.mkdirs();
    }
    String url = new File(tmpFile, fileName).toString();
    // SpillingDiskOutputBuffer writeBuffer = new SpillingDiskOutputBuffer(url);
    FileOutputStream fot = new FileOutputStream(new File(url));
    BufferedOutputStream bos = new BufferedOutputStream(fot,
        MetaDataOfMessage.MESSAGE_IO_BYYES);
    DataOutputStream dos = new DataOutputStream(bos);
    for (int i = 0; i < count; i++) {
      msgList.remove(0).write(dos);
    }
    dos.close();
  }
  
  /**
   * Load All The Messages In The Specified Bucket(Multiple Files).
   * @param msgMap
   * @param superStep
   * @param bucket
   * @throws IOException e
   */
  public void processMessageLoad(HashMap<String, ArrayList<IMessage>> msgMap,
      int superStep, int bucket) throws IOException {
    String tmpPath = this.workerDir + "/" + "superstep-" + superStep + "/"
        + "bucket-" + bucket;
    File bucketFile = new File(tmpPath);
    if (!bucketFile.exists()) {
      LOG.warn("<Bucket> (" + bucket + ") Is Not Exists While Loading From It");
      return;
    }
    FileInputStream fis;
    BufferedInputStream bis;
    DataInputStream dis;
    IMessage m;
    ArrayList<IMessage> mList;
    File[] listFiles = bucketFile.listFiles();
    String[] fileName;
    int count;
    for (int i = 0; i < listFiles.length; i++) {
      fileName = listFiles[i].getName().split("-");
      count = Integer.parseInt(fileName[fileName.length - 1]);
      fis = new FileInputStream(listFiles[i]);
      bis = new BufferedInputStream(fis, MetaDataOfMessage.MESSAGE_IO_BYYES);
      dis = new DataInputStream(bis);
      for (int j = 0; j < count; j++) {
        m = CommunicationFactory.createBspMessage();
        m.readFields(dis);
        // LOG.info("<><><><><>"+m.getDstVertexID()+"<><><><><>"+new
        // String(m.getData()));
        mList = msgMap.get(m.getDstVertexID());
        if (mList == null) {
          mList = new ArrayList<IMessage>();
        }
        mList.add(m);
        msgMap.put(m.getDstVertexID(), mList);
      }
      dis.close();
    }
  }
  
  // ========================================================================
  /**
   * Save messages of every partition on disk for byte array version.
   * @param messages MessageBytePoolPerPartition
   * @param superStep int
   * @param srcPartitionDstBucket int
   * @throws IOException e
   */
  public void processMessagesSave(MessageBytePoolPerPartition messages,
      int superStep, int srcPartitionDstBucket) throws IOException {
    // LOG.info("####TAG1: "+srcPartitionDstBucket
    // +" ###TAG2: "+msgList.size());
    int count = messages.getMsgCount();
    int bucket = PartitionRule.getBucket(srcPartitionDstBucket);
    int srcPartition = PartitionRule.getPartition(srcPartitionDstBucket);
    String fileName = "srcPartition-" + srcPartition + "_" + "counter" + "-"
        + counter[srcPartitionDstBucket]++ + "_" + "count" + "-" + count;
    String tmpPath = this.workerDir + "/" + "superstep-" + superStep + "/"
        + "bucket-" + bucket;
    File tmpFile = new File(tmpPath);
    if (!tmpFile.exists()) {
      tmpFile.mkdirs();
    }
    String url = new File(tmpFile, fileName).toString();
    // SpillingDiskOutputBuffer writeBuffer = new SpillingDiskOutputBuffer(url);
    FileOutputStream fot = new FileOutputStream(new File(url));
    BufferedOutputStream bos = new BufferedOutputStream(fot,
        MetaDataOfMessage.MESSAGE_IO_BYYES);
    DataOutputStream dos = new DataOutputStream(bos);
    messages.write(dos);
    dos.close();
  }
  
  /**
   * Get The Bucket Files In File Array.
   * @param superStep int
   * @param bucket int
   * @return File[]
   */
  public File[] preLoadFile(int superStep, int bucket) {
    String tmpPath = this.workerDir + "/" + "superstep-" + superStep + "/"
        + "bucket-" + bucket;
    File bucketFile = new File(tmpPath);
    if (!bucketFile.exists()) {
      LOG.warn("<Bucket> (" + bucket + ") Is Not Exists While Loading From It");
      return null;
    }
    File[] listFiles = bucketFile.listFiles();
    return listFiles;
  }
  
  /**
   *  Note Messages Coming Must Be Reinitialized. File Is Legal By Default.
   * @param messages MessageBytePoolPerPartition
   * @param f File
   * @throws IOException e
   */
  public void processMessageLoadFile(MessageBytePoolPerPartition messages,
      File f) throws IOException {
    FileInputStream fis;
    BufferedInputStream bis;
    DataInputStream dis;
    fis = new FileInputStream(f);
    bis = new BufferedInputStream(fis, MetaDataOfMessage.MESSAGE_IO_BYYES);
    dis = new DataInputStream(bis);
    messages.readFields(dis);
    dis.close();
  }

  public static String getWorkerDir() {
    return workerDir;
  }

  public static void setWorkerDir(String workerDir) {
    DiskManager.workerDir = workerDir;
  }
}
