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

package com.chinamobile.bcbsp.bspstaff;



import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.ml.BSPPeer;
import com.chinamobile.bcbsp.util.BSPJob;

import java.util.Iterator;

/**
 * BSPStaffContextInterface.java This is a context for prcessing one vertex. All
 * methods defined in this interface can be used by users in {@link BSP}.
 * @author WangZhigang, changed by Bai Qiushi
 * @version 1.0
 */

public interface BSPStaffContextInterface {

  /**
   * Get the vertex.
   * @return The vertex
   */
  @SuppressWarnings({"rawtypes"})
  Vertex getVertex();

  /**
   * Update the vertex.
   * @param vertex User defined vertex
   */
  @SuppressWarnings("unchecked")
  void updateVertex(Vertex vertex);

  /**
   * Get the number of outgoing edges.
   * @return  the number of outgoing edges
   */
  int getOutgoingEdgesNum();

  /**
   * Get outgoing edges.
   * @return The outgoing edges.
   */
  Iterator<Edge> getOutgoingEdges();

  /**
   * Remove one outgoing edge. If the edge does not exist, return false.
   * @param edge The edge user defined.
   * @return <code>true</code> if the operation is successfull, else
   *         <code>false</code>.
   */
  @SuppressWarnings("unchecked")
  boolean removeEdge(Edge edge);

  /**
   * Update one outgoing edge. If the edge exists, update it, else add it.
   * @param edge the edge of vertex.
   * @return <code>true</code> if the operation is successfull, else
   *         <code>false</code>.
   */
  @SuppressWarnings("unchecked")
  boolean updateEdge(Edge edge);

  /**
   * Get the current superstep counter.
   * @return the current super step counter.
   */
  int getCurrentSuperStepCounter();

  /**
   * Get the BSP Job Configuration.
   * @return the BSP Job Configuration
   */
  BSPJob getJobConf();

  /**
   * User interface to send a message in the compute function.
   * @param msg The message in system
   */
  void send(IMessage msg);

  // Note For Compatible Consideration.
  /**
   * User interface to send BSP message.
   * @param msg BSPMessage
   */
  void send(BSPMessage msg);

  /**
   * User interface to get an aggregate value aggregated from the previous super
   * step.
   * @param name AggregateValue name
   * @return the AggregateValue of name
   */
  @SuppressWarnings({"rawtypes"})
  AggregateValue getAggregateValue(String name);

  /**
   * User interface to set current head node into inactive state.
   */
  void voltToHalt();

  /**
   * User interface to get the message.
   * @return The message in system*/
  @SuppressWarnings("rawtypes")
  IMessage getMessage();

  /**
   * User interface to set the communicator.
   * @param handler the communicatorInterface type communicator.
   */
  void setCommHandler(CommunicatorInterface handler);
  /**
   * added for test
   */
  CommunicatorInterface getCommunicator();
  
  /**
   * Update the peer for machine learning compute.
   * @param peer
   */  
  void updatePeer(BSPPeer peer);
  

  /**
   * Get the Current peer for this superstep compute.
   * @return
   */
  BSPPeer getCurrentPeer();
}
