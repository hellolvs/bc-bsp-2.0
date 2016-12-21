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


import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The interface for Message Queues, which manages the message queues.
 */
public interface MessageQueuesInterface {
  
  /**
   * Add a message into the incoming message queue with the destination vertexID
   * for index.
   * @param dstVertexID String
   * @param msg IMessage
   */
  void incomeAMessage(String dstVertexID, IMessage msg);
  
  /**
   * Add a message into the outgoing message queue with the destination
   * "WorkerManagerName:Port" for index.
   * @param outgoingIndex String
   * @param msg IMessage
   */
  void outgoAMessage(String outgoingIndex, IMessage msg);
  
  /**
   * Remove and return the incomed message queue for dstVertexID as the
   * destination.
   * @param dstVertexID String
   * @return ConcurrentLinkedQueue<BSPMessage>
   */
  ConcurrentLinkedQueue<IMessage> removeIncomedQueue(String dstVertexID);
  
  /**
   * Get the current longest outgoing queue's index. If the outgoing queues are
   * all empty, return null.
   * @return String
   */
  String getMaxOutgoingQueueIndex();
  
  /**
   * Get the next outgoing queue's index. This method will be used for traversal
   * of the outgoing queues' map looply. If the outgoing queues are all empty,
   * return null.
   * @return String
   * @throws Exception
   *         e
   */
  String getNextOutgoingQueueIndex() throws Exception;
  
  /**
   * Remove and return the outgoing message queue for index as the queue's
   * index.
   * @param index String
   * @return ConcurrentLinkedQueue<BSPMessage>
   */
  ConcurrentLinkedQueue<IMessage> removeOutgoingQueue(String index);
  
  /**
   * Get the current longest incoming queue's index. If the incoming queues are
   * all empty, return -1.
   * @return int
   */
  String getMaxIncomingQueueIndex();
  
  /**
   * Remove and return the incoming message queue for dstVertexID as the queue's
   * index.
   * @param dstVertexID
   *        String
   * @return ConcurrentLinkedQueue<BSPMessage>
   */
  ConcurrentLinkedQueue<IMessage> removeIncomingQueue(String dstVertexID);
  
  /**
   * Get the size of incoming queues.
   * @param dstVertexID
   *        String
   * @return int
   */
  int getIncomingQueueSize(String dstVertexID);
  
  /**
   * Get the size of outgoing queues.
   * @param index
   *        String
   * @return int
   */
  int getOutgoingQueueSize(String index);
  
  /** Exchange the incomeQueues. */
  void exchangeIncomeQueues();
  
  /** Clear all message queues. */
  void clearAllQueues();
  
  /** Clear outgoing message queues. */
  void clearOutgoingQueues();
  
  /** Clear incoming message queues. */
  void clearIncomingQueues();
  
  /** Clear incomed message queues. */
  void clearIncomedQueues();
  
  /**
   * Get the size of outgoing queues.
   * @return int
   */
  int getOutgoingQueuesSize();
  
  /**
   * Get the size of incoming queues.
   * @return int
   */
  int getIncomingQueuesSize();
  
  /**
   * Get the size of incomed queues.
   * @return int
   */
  int getIncomedQueuesSize();
  
  /** Show the information of memory. */
  void showMemoryInfo();
  
  /* Zhicheng Liu added */
  /**
   * Get message method.
   * @return IMessage
   */
  IMessage getAMessage();
  
}
