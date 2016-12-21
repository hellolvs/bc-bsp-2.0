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

import com.chinamobile.bcbsp.comm.IMessage;

import java.util.ArrayList;

/**
 * Message storage Interface for current iteration.
 * @author Liujinpeng
 */
public interface IMessageStoreProcessed {
  
  /**
   * Message From BytesPool To Object Organization.BytePool Is Ready.
   * @param partitionId
   *        int
   * @param superstep
   *        int
   */
  void preLoadMessages(int partitionId, int superstep);
  /*Biyahui added*/
  void preLoadMessagesNew(int partitionId, int superstep);
  
  /**
   * Add a byte array message into message iterator and change it into object.
   * @param messages
   *        MessageBytePoolPerPartition
   */
  void addMsgBytePoolToMsgObjects(MessageBytePoolPerPartition messages);
  
  /**
   * Remove message from one vertex.
   * @param vertex String
   * @param partiId int
   * @return ArrayList<IMessage>
   */
  ArrayList<IMessage> removeMessagesForOneVertex(String vertex, int partiId);
  
  /** Clean the message store. */
  void clean();
  
  /**
   * For Update The ByteArray Message For Next Superstep In Exchage Procedure.
   * @param msgStore MessageStore
   */
  void refreshMessageBytePools(MessageStore msgStore);

}
