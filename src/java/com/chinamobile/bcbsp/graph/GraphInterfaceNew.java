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

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContext;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.GraphStaffHandler;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Graph data manage interface.
 */
public interface GraphInterfaceNew {
  
  /**
   * Graph data manage interface. It offers operations of vertices.
   * @param graphStaffHandler
   *        GraphStaffHandler
   * @param bsp
   *        BSP
   * @param job
   *        BSPJob
   * @param superStepCounter
   *        int
   * @param context
   *        BSPStaffContext
   * @throws IOException
   *         e
   */
  void processingByBucket(GraphStaffHandler graphStaffHandler, BSP bsp,
      BSPJob job, int superStepCounter, BSPStaffContext context)
      throws IOException;
  
  /**
   * Save the result of all vertices.
   * @param graphStaffHandler
   *        GraphStaffHandler
   * @param output
   *        RecordWriter
   * @throws IOException
   *         e
   * @throws InterruptedException
   *         e
   */
  void saveAllVertices(GraphStaffHandler graphStaffHandler, RecordWriter output)
      throws IOException, InterruptedException;
  
  /**
   * Save the result of all vertices for checkpoint.
   * @param output
   *        RecordWriter
   * @throws IOException
   *         e
   * @throws InterruptedException
   *         e
   */
  void saveAllVertices(RecordWriter output) throws IOException,
      InterruptedException;
/**
 * get all the vertex message saved for messagecheckpoint
 * @param output
 * @return
 * @throws IOException
 * @throws InterruptedException
 */ 
void getAllVertex(GraphStaffHandler graphStaffHandler, CommunicatorInterface communicator,
		RecordWriter output) throws IOException,
		InterruptedException;
}
