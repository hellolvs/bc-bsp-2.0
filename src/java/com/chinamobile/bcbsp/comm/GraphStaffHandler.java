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

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContext;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.ml.BSPPeer;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;

/**
 * Handler Interface For GraphManager Handling.
 * @author Liu Jinpeng
 */
public interface GraphStaffHandler {
  /**
   * Process vertices of a partition.
   * @param v
   *        Vertex
   * @param bsp
   *        BSP
   * @param job
   *        BSPJob
   * @param superStepCounter
   *        int
   * @param context
   *        BSPStaffContext
   * @param activeFlag
   *        boolean
   * @throws IOException
   *         e
   */
  void vertexProcessing(Vertex v, BSP bsp, BSPJob job,
      int superStepCounter, BSPStaffContext context, boolean activeFlag)
      throws IOException;
  
  /**
   * Preprocess of a bucket.
   * @param bucket int
   * @param superstep int
   */
  void preBucket(int bucket, int superstep);
  
  //biyahui added
  /**
   * load message by bucket
   * @param bucket
   * @param job
   * @param context
   */
  public void preBucketForRecovery(int bucket,BSPJob job,BSPStaffContext context);
  /**
   * Save the result of vertex.
   * @param v Vertex
   * @param output RecordWriter
   * @throws IOException e
   * @throws InterruptedException e
   */
  void saveResultOfVertex(Vertex v, RecordWriter output)
      throws IOException, InterruptedException;
  
  /**
   * Progress the partition for machine learning app.
   * @param peer
   * @param bsp
   * @param job
   * @param superStepCounter
   * @param context
   * @param activeFlag
   */
  void peerProcessing(BSPPeer peer , BSP bsp, BSPJob job, int superStepCounter, BSPStaffContext context, boolean activeFlag );

}
