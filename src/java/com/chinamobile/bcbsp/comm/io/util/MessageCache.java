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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.comm.CommunicationFactory;

/**
 * Message cache for byte array.
 * @author Liu Jinpeng
 */
public class MessageCache {
  /** Class logger. */
  private static final Log LOG = LogFactory.getLog(MessageCache.class);
  /** handle of message store as byte array. */
  private MessageBytePoolPerPartition[] msgBytesPoolHandlers;
  
  /**
   *  Constructor of MessageCache.
   * @param partitionNum
   */
  public MessageCache(int partitionNum) {
    initialize(partitionNum);
  }
  
  /**
   *  Initialize Before Utilization.
   * @param partitionNum
   */
  public void initialize(int partitionNum) {
    msgBytesPoolHandlers = new MessageBytePoolPerPartition[partitionNum];
    for (int i = 0; i < partitionNum; i++) {
      msgBytesPoolHandlers[i] = createData();
    }
  }
  
  /**
   * Create the message byte thread pool of per partition.
   * @return MessageBytePoolPerPartition
   */
  private MessageBytePoolPerPartition createData() {
    return CommunicationFactory.createMsgBytesPoolPerPartition();
  }
  
  /**
   * Remove The Specified BytePool And Update The Array List.
   * @param partitionId
   * @return MessageBytePoolPerPartition
   */
  public MessageBytePoolPerPartition removeMsgBytePool(int partitionId) {
    MessageBytePoolPerPartition tmp = msgBytesPoolHandlers[partitionId];
    msgBytesPoolHandlers[partitionId] = createData();
    return tmp;
  }
  
  /**
   * Get method of MessageBytePoolPerPartition handle as partition ID.
   * @param partitionId
   * @return MessageBytePoolPerPartition
   */
  public MessageBytePoolPerPartition getBytePoolPerPartition(int partitionId) {
    return this.msgBytesPoolHandlers[partitionId];
  }
  
  /**
   * Get the total counter for numbers of all MessageBytePoolPerPartitions.
   * @return int
   */
  public int getTotalCount() {
    int count = 0;
    for (int i = 0; i < msgBytesPoolHandlers.length; i++) {
      count += msgBytesPoolHandlers[i].getMsgCount();
    }
    return count;
  }
  
  /**
   * Get the total counter for sizes of all MessageBytePoolPerPartitions.
   * @return int
   */
  public int getTotalSize() {
    int count = 0;
    for (int i = 0; i < msgBytesPoolHandlers.length; i++) {
      count += msgBytesPoolHandlers[i].getMsgSize();
    }
    return count;
  }

  /**
   * Get method of MessageBytePoolPerPartition list.
   * @return MessageBytePoolPerPartition[]
   */
  public MessageBytePoolPerPartition[] getMsgBytesPoolHandlers() {
    return msgBytesPoolHandlers;
  }
  
}
