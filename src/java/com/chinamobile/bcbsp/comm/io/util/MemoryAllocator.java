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

package com.chinamobile.bcbsp.comm.io.util;

import com.chinamobile.bcbsp.comm.MetaDataOfMessage;
import com.chinamobile.bcbsp.graph.MetaDataOfGraph;
import com.chinamobile.bcbsp.util.BSPJob;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import org.apache.commons.logging.Log;

/**
 * 
 * 
 * */
public class MemoryAllocator {
  /** the handle of BSP job. */
  private BSPJob job;
  // Note Basic Memory Parameters
  /** the used memory of JVM. */
  private long used = 0;
  /** committed memory size of JVM. */
  private long committed = 0;
  /** max heap memory size of JVM. */
  private long maxHeapSize = 0;
  /** remain memory size of JVM. */
  private long remain = 0;
  /** adjusted remain memory to voiding JVM object extending usage. */
  private long remainAdjusted = 0;
  
  /**
   * Constructor of MemoryAllocator.
   * @param job
   */
  public MemoryAllocator(BSPJob job) {
    this.job = job;
    MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = this.job.getHashBucketNumber();
    refreshMemoryStatus();
    initializeDataUnit();
    remainAdjusted = (long) (remain * 0.8);
  }
  
  /** Refresh memory status with current JVM status. */
  private void refreshMemoryStatus() {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
    used = memoryUsage.getUsed();
    committed = memoryUsage.getCommitted();
    maxHeapSize = memoryUsage.getMax();
    remain = maxHeapSize - used;
  }
  
  /** Show the information of memory used. */
  public void PrintMemoryInfo(Log LOG) {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
    long usedNow = memoryUsage.getUsed();
    long committedNow = memoryUsage.getCommitted();
    long maxHeapSizeNow = memoryUsage.getMax();
    long remainNow = maxHeapSize - used;
    LOG.info("Memory Usage **********************************************");
    LOG.info("Memory  Max ****************** " + maxHeapSizeNow);
    LOG.info("Memory Commit **************** " + committedNow
        + "-----Percents: " + committedNow * 1.0f / maxHeapSizeNow);
    LOG.info("Memory Used ****************** " + usedNow + "-----Percents: "
        + usedNow * 1.0f / maxHeapSizeNow);
    LOG.info("Memory Remain **************** " + remainNow + "-----Percents: "
        + remainNow * 1.0f / maxHeapSizeNow);
  }
  
  /**
   * Initialize The Data Unit Size for user-Defined memory partition.
   */
  private void initializeDataUnit() {
    MetaDataOfGraph.BYTESIZE_PER_VERTEX = 8;
    MetaDataOfGraph.BYTESIZE_PER_EDGE = 4;
    MetaDataOfMessage.BYTESIZE_PER_MESSAGE = 8;
  }
  
  /**
   * Set up the graph data load buffer size.
   * Make sure that hash number is initialized.
   */
  public void setupBeforeLoadGraph(Log LOG) {
    long byteSizeForGraphLoad = remainAdjusted
        / MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER;
    MetaDataOfGraph.BCBSP_GRAPH_LOAD_INIT = (int) (byteSizeForGraphLoad
        / MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER / 2);
    LOG.info("Graph Load Buffer Size **************** "
        + MetaDataOfGraph.BCBSP_GRAPH_LOAD_INIT);
  }
  
  /**
   * Utilized in computation recycle.
   * Make sure that graph data is ready for Item Ratio Computation.
   * @param superstep
   */
  public void setupOnEachSuperstep(int superstep, Log LOG) {
    if (superstep > 0) {
      return;
    }
    // Note Combine And None-Combine. Bucked-To-Be-Processing Bytes Are
    // Different.
    int maxBucketCount = MetaDataOfGraph.
        VERTEX_NUM_PERBUCKET[MetaDataOfGraph.BCBSP_MAX_BUCKET_INDEX];
    // Accurate Evaluation.
    long messageObjesSize = maxBucketCount
        * (8 + 4 + MetaDataOfMessage.BYTESIZE_PER_MESSAGE);
    // Note Contain The Incomed Bucket Message For Memory Deployment.
    long leftBytes = this.remainAdjusted - messageObjesSize;
    if (leftBytes <= 0) {
      LOG.info("[ShutDown]Memory Space Is Too Small "
          + "Except The One To Hold The Bucketed Message");
      System.exit(-1);
    } else if (leftBytes <= this.remainAdjusted / 5) {
      LOG.info("Memory Space For Local Computing Is "
          + "A Little Small [Less Than 1/5Total]");
    }
    long sendMessageCacheBytes = leftBytes / 8;
    leftBytes = leftBytes - sendMessageCacheBytes;
    // Note Graph Data Is In Memory One Bucket One Time.
    // Note Set Up The Ceil Bucket Cache Threshold(Bytes).
    // Note IO Buffer Unit Is 10000 To 10000(Bytes)[65535]
    int unitSizeThreshold = 65535;
    // Note Graph Is Vertex And Edges.
    long graphIOBytes = (long) unitSizeThreshold
        * MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER * 2;
    // Note
    long revMessageIOBytes = (long) unitSizeThreshold
        * MetaDataOfMessage.PARTITIONBUCKET_NUMBER;
    // Note
    leftBytes -= (graphIOBytes + revMessageIOBytes + sendMessageCacheBytes);
    if (leftBytes <= 0) {
      LOG.info("[ShutDown]Memory Space Is Too Small "
          + "To Cache The Bucketed Message");
      System.exit(-1);
    } else if (leftBytes <= this.remainAdjusted / 10) {
      LOG.info("Memory Space For Local Computing Is"
          + " A Little Small [Less Than 1/10Total]");
    }
    MetaDataOfGraph.BCBSP_GRAPH_LOAD_READSIZE = unitSizeThreshold;
    MetaDataOfGraph.BCBSP_GRAPH_LOAD_WRITESIZE = unitSizeThreshold;
    MetaDataOfMessage.MESSAGE_IO_BYYES = unitSizeThreshold;
    MetaDataOfMessage.MESSAGE_SEND_BUFFER_THRESHOLD =
        (int) (sendMessageCacheBytes / MetaDataOfMessage.
            PARTITIONBUCKET_NUMBER);
    MetaDataOfMessage.MESSAGE_RECEIVED_BUFFER_THRESHOLD = (int) (leftBytes
        / MetaDataOfMessage.PARTITIONBUCKET_NUMBER / 2);
    LOG.info("Memory Deploy **********************************************");
    LOG.info("Memory Processed Message Bucket Bytes   *************** "
        + messageObjesSize);
    LOG.info("Memory Message Send Cache Bytes         **************** "
        + sendMessageCacheBytes + "***** SizePerUnit: "
        + MetaDataOfMessage.MESSAGE_SEND_BUFFER_THRESHOLD);
    LOG.info("Memory Graph Data IO Bytes              **************** "
        + graphIOBytes);
    LOG.info("Memory Message Data IO Bytes            **************** "
        + revMessageIOBytes);
    LOG.info("Memory Message Rev Cached Bytes         **************** "
        + leftBytes + "****SizePerUnit: "
        + MetaDataOfMessage.MESSAGE_RECEIVED_BUFFER_THRESHOLD);
  }
}
