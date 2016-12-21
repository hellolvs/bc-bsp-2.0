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

import com.chinamobile.bcbsp.comm.io.util.WritableBSPMessages;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The interface for Message Queues, which manages the message queues.
 */
public interface MessageManagerInterface {
  
  /**
   * Load message bucket.
   * @param bucket
   *        int
   * @param superStep
   *        int
   */
  void loadBucketMessage(int bucket, int superStep);
  
  /**
   * Add a message into the incoming message queue with the destination vertexID
   * for index.
   * @param srcPartitionDstBucket
   *        int
   * @param msg
   *        IMessage
   * @param superstep
   *        int
   */
  void incomeAMessage(int srcPartitionDstBucket, IMessage msg, int superstep);
  
  /**
   * Add a message into the incoming message queue with the destination vertexID
   * for index.
   * @param srcPartitionDstBucket
   *        int
   * @param msg
   *        WritableBSPMessages
   * @param superstep
   *        int
   */
  void incomeAMessage(int srcPartitionDstBucket, WritableBSPMessages msg,
      int superstep);
  
  /**
   * Add a message into the outgoing message queue with the destination
   * "WorkerManagerName:Port" for index.
   * @param dstPartitionBucket
   *        int
   * @param msg
   *        IMessage
   * @return int
   */
  int outgoAMessage(int dstPartitionBucket, IMessage msg);
  
  /**
   * Remove and return the incomed message queue for dstVertexID as the
   * destination.
   * @param dstVertexID
   *        String
   * @return ConcurrentLinkedQueue<BSPMessage>
   */
  ArrayList<IMessage> removeIncomedQueue(String dstVertexID);
  
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
   * @param destPartitionBucket
   *        int
   * @return ConcurrentLinkedQueue<BSPMessage>
   */
  WritableBSPMessages removeOutgoingQueue(int destPartitionBucket);
  
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
   * Get the incoming queue's size.
   * @param dstVertexID String
   * @return int
   */
  int getIncomingQueueSize(String dstVertexID);
  
  /**
   * Get the outgoing queue's size.
   * @param dstPartitionBucket int
   * @return int
   */
  int getOutgoingQueueSize(int dstPartitionBucket);
  
  /**
   * Get the outgoing queue's size.
   */
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
   * Get the outgoing queue's size.
   * @return int
   */
  int getOutgoingQueuesSize();
  
  /**
   * Get the incoming queue's size.
   * @return int
   */
  int getIncomingQueuesSize();
  
  /**
   * Get the incomed queue's size.
   * @return int
   */
  int getIncomedQueuesSize();
  
  /** Show the information of memeory.*/
  void showMemoryInfo();

  void loadBucketMessageNew(int bucket, int superStep);
}
