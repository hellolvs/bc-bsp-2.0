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

package com.chinamobile.bcbsp.pipes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.Communicator;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.IMessage;

/**
 * The interface for the messages that can come up from the child. All of these
 * calls are asynchronous and return before the message has been processed.
 */
public interface UpwardProtocol {

  /**
   * Get message from c++ client and send.
   * @param msg
   *        the string to display to the user
   * @throws IOException e
   */
  void sendNewMessage(String msg) throws IOException;

  /**
   * Get Aggregate value from c++ and set to handler.
   * @param aggregateValue
   *        string like "aggregator name : aggregate value"
   * @throws IOException e
   */
  void setAgregateValue(String aggregateValue) throws IOException;

  /**
   * Report that the Current superStep has finished successfully.
   * @throws IOException e
   */
  void currentSuperStepDone() throws IOException;

  /**
   * Report that the Current superStep or more likely communication failed.
   * @param e throws exception
   */
  void failed(Throwable e);

  /**
   * According vertex id to get new Message from java.
   * @param vertexId the id of the vertex
   * @return the message queue
   * @throws IOException e
   */
  ConcurrentLinkedQueue<IMessage> getMessage(String vertexId)
      throws IOException;

  // /**
  // * According key to get partition id from c++
  // */
  // int getPartitionId(int id) throws IOException;
  /**Set the communicator.
   * @param communicator the communciator*/
  void setCommunicator(CommunicatorInterface communicator);

  /**Open the HDFS.*/
  void openDFS();

  /**Save result.
   * @param vertexId the id of vertex
   * @param vertexValue the value of vertex
   * @param edges the edges of vertex
   * @return whether save result succeed*/
  boolean saveResult(String vertexId, String vertexValue, String[] edges);

  /**Save result.
   * @param vertexEdge the vertex edge list
   * @return whether save result succeed*/
  boolean saveResult(ArrayList<String> vertexEdge);

  /**Save result.
   * @param vertex the vertex
   * @return whether save result succeed*/
  boolean saveResult(String vertex);

  /**Close the HDFS.
   * @throws IOException e
   * @throws InterruptedException e*/
  void closeDFS() throws IOException, InterruptedException;
}
