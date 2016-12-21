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

package com.chinamobile.bcbsp.graph;

import org.apache.commons.logging.Log;

/**
 * Meta Data of Graphdata.
 * @author Liu Jinpeng
 */
public class MetaDataOfGraph {
  /** Directory For All Jobs. */
  public static String BCBSP_DISKGRAPH_BASEDIR = "/tmp/bcbsp1/";
  /** Directory For One Job StaffPartition. */
  public static String BCBSP_DISKGRAPH_WORKDIR;
  /** hash number of graph data bucket. */
  public static int BCBSP_DISKGRAPH_HASHNUMBER = 1;
  /** vertex number of per graph data bucket. */
  public static int BCBSP_GRAPH_VERTEXNUM = 0;
  /** edge number of graph data. */
  public static int BCBSP_GRAPH_EDGENUM = 0;
  /** vertex number of graph data. */
  public static int BCBSP_GRAPH_LOAD_INIT = -1;
  /** size of writing loading graph data. */
  public static int BCBSP_GRAPH_LOAD_WRITESIZE = 65535;
  /** size of reading loading graph data. */
  public static int BCBSP_GRAPH_LOAD_READSIZE = 65535;
  /** vertex number of per graph data bucket. */
  public static int[] VERTEX_NUM_PERBUCKET;
  /** edge number of per graph data bucket. */
  public static int[] EDGE_NUM_PERBUCKET;
  // Note Data For Byte Compute
  /** byte size of vertex. */
  public static byte BYTESIZE_PER_VERTEX = 0;
  /** byte size of edge. */
  public static byte BYTESIZE_PER_EDGE = 0;
  /** maximum of bucket index. */
  public static short BCBSP_MAX_BUCKET_INDEX = 0;
  
  /** Initialize the statistics of every bucket. */
  public static void initStatisticsPerBucket() {
    VERTEX_NUM_PERBUCKET = new int[BCBSP_DISKGRAPH_HASHNUMBER];
    EDGE_NUM_PERBUCKET = new int[BCBSP_DISKGRAPH_HASHNUMBER];
    for (int i = 0; i < BCBSP_DISKGRAPH_HASHNUMBER; i++) {
      VERTEX_NUM_PERBUCKET[i] = EDGE_NUM_PERBUCKET[i] = 0;
    }
  }
  
  /** Initialize the maximum of bucket index. */
  private static void initializeMaxBucket() {
    short index = 0;
    for (short i = 0; i < BCBSP_DISKGRAPH_HASHNUMBER; i++) {
      index = VERTEX_NUM_PERBUCKET[i] > VERTEX_NUM_PERBUCKET[index] ? i : index;
    }
    BCBSP_MAX_BUCKET_INDEX = index;
  }
  
  /** Compute the statistics of per bucket. */
  public static void StatisticsPerBucket() {
    VERTEX_NUM_PERBUCKET = new int[BCBSP_DISKGRAPH_HASHNUMBER];
    EDGE_NUM_PERBUCKET = new int[BCBSP_DISKGRAPH_HASHNUMBER];
    for (int i = 0; i < BCBSP_DISKGRAPH_HASHNUMBER; i++) {
      VERTEX_NUM_PERBUCKET[i] = EDGE_NUM_PERBUCKET[i] = 0;
    }
  }
  
  /** Show the statistics of per bucket. */
  public static void LogStatisticsPerBucket(Log LOG) {
    for (int i = 0; i < BCBSP_DISKGRAPH_HASHNUMBER; i++) {
      LOG.info("<Bucket - " + i + "> Vertices Is :" + VERTEX_NUM_PERBUCKET[i]
          + "_____ " + "Edges Is : " + EDGE_NUM_PERBUCKET[i]);
    }
    initializeMaxBucket();
    LOG.info("[The Max Size Bucket]  " + BCBSP_MAX_BUCKET_INDEX
        + " [Bucket Size] " + VERTEX_NUM_PERBUCKET[BCBSP_MAX_BUCKET_INDEX]);
  }
  
  /**
   * Compute the local partition rule.
   * @param dstVertexID
   * @return int
   */
  public static int localPartitionRule(String dstVertexID) {
    int hashBucketNumber = BCBSP_DISKGRAPH_HASHNUMBER;
    int hashCode = dstVertexID.hashCode();
    int hashIndex = hashCode % hashBucketNumber; // bucket index
    hashIndex = (hashIndex < 0 ? hashIndex + hashBucketNumber : hashIndex);
    return hashIndex;
  }
}
