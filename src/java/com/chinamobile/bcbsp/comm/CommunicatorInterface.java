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

import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.router.routeparameter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * CommunicatorInterface.
 * 
 * @author
 * @version
 */
public interface CommunicatorInterface extends CommunicateNInterface {
  
  /**
   * Initialize the communicator.
   * 
   * @param routeparameter
   *        routeparameter
   * @param aPartitionToWorkerManagerNameAndPort
   *        HashMap<Integer, String>
   * @param aGraphData
   *        GraphDataInterface
   */
  void initialize(routeparameter routeparameter,
      HashMap<Integer, String> aPartitionToWorkerManagerNameAndPort,
      GraphDataInterface aGraphData);
  
  /**
   * Initialize the communicator.
   * 
   * @param routeparameter
   *        routeparameter
   * @param aPartitionToWorkerManagerNameAndPort
   *        HashMap<Integer, String>
   * @param edgeSize
   *        Integer
   */
  void initialize(routeparameter routeparameter,
      HashMap<Integer, String> aPartitionToWorkerManagerNameAndPort,
      int edgeSize);
  
  /**
   * Send a M. Messages sent by this method are not guaranteed to be received in
   * a sent order.
   * 
   * @param msg
   *        IMessage
   * @throws IOException
   *         e
   */
  void send(IMessage msg) throws IOException;
  
  /**
   * Send a M to all vertices of it's outgoing edges. Messages sent by this
   * method are not guaranteed to be received in a sent order.
   * 
   * @param msg
   *        IMessage
   * @throws IOException
   *         e
   */
  void sendToAllEdges(IMessage msg) throws IOException;
  
  /**
   * Get the M Iterator filled with messages sent to this vertexID in the last
   * super step.
   * 
   * @param vertexID
   *        String
   * @return The message iterator
   * @throws IOException
   *         e
   */
  Iterator<IMessage> getMessageIterator(String vertexID) throws IOException;
  
  /**
   * Get the M Queue filled with messages sent to this vertexID in the last
   * super step.
   * 
   * @param vertexID
   *        String
   * @return ConcurrentLinkedQueue<IMessage>
   * @throws IOException
   *         e
   */
  ConcurrentLinkedQueue<IMessage> getMessageQueue(String vertexID)
      throws IOException;
  
  /**
   * To start the sender and the receiver of the communicator.
   */
  void start();
  
  /**
   * To begin the sender's and the receiver's tasks for the super step.
   * 
   * @param superStepCount
   *        Integer
   */
  void begin(int superStepCount);
  
  /**
   * To notify the sender and the receiver to complete.
   */
  void complete();
  
  /**
   * To notify the communicator that there are no more messages for sending for
   * this super step.
   */
  void noMoreMessagesForSending();
  
  /**
   * To ask the communicator if the sending process has finished sending all of
   * the messages in the outgoing queue of the communicator.
   * 
   * @return true if sending has finished, otherwise false.
   */
  boolean isSendingOver();
  
  /**
   * To notify the communicator that there are no more messages for receving for
   * this super step.
   */
  void noMoreMessagesForReceiving();
  
  /**
   * To ask the communicator if the receiving process has finished receiving all
   * of the remaining messages from the incoming queue for it.
   * 
   * @return true if receiving has finished, otherwise false.
   */
  boolean isReceivingOver();
  
  /**
   * To exchange the incoming and incomed queues for each super step.
   */
  void exchangeIncomeQueues();
  
  /**
   * Get the outgoing queues' size.
   * 
   * @return int outgoing queues' size
   */
  int getOutgoingQueuesSize();
  
  /**
   * Get the incoming queues' size.
   * 
   * @return int incoming queues' size
   */
  int getIncomingQueuesSize();
  
  /**
   * Get the incomed queues' size.
   * 
   * @return int incomed queues' size
   */
  int getIncomedQueuesSize();
  
  /**
   * To clear all the queues.
   */
  void clearAllQueues();
  
  /**
   * To clear the outgoing queues.
   */
  void clearOutgoingQueues();
  
  /**
   * Set the incomed queues' size.
   * 
   * @param value
   *        HashMap<Integer, String>
   */
  void setPartitionToWorkerManagerNamePort(HashMap<Integer, String> value);
  
  /**
   * To clear the incoming queues.
   */
  void clearIncomingQueues();
  
  /**
   * To clear the incomed queues.
   */
  void clearIncomedQueues();
  
  // add by chen
  /**
   * Get outgoing message counter for a super step.
   * 
   * @return long outgoMessageCounter long
   */
  long getOutgoMessageCounter();
  
  /**
   * Get received message counter for a super step.
   * 
   * @return receivedMessageCounter long
   */
  long getReceivedMessageCounter();
  
  /**
   * Get received message bytes counter for a super step.
   * 
   * @return receivedMessageBytesCounter long
   */
  long getReceivedMessageBytesCounter();
  
  /**
   * Get outgoing message bytes counter for a super step.
   * 
   * @return outgoMessageBytesCounter long
   */
  long getOutgoMessageBytesCounter();
  
  /**
   * Get outgoing message counter for a super step after combine.
   * 
   * @return long outgoMessageBytesCounter
   */
  long getCombineOutgoMessageCounter();
  
  /**
   * Get outgoing message bytes counter for a super step after combine.
   * 
   * @return long combineOutgoMessageBytesCounter
   */
  long getCombineOutgoMessageBytesCounter();
  
  /**
   * The counter of send messages.
   * 
   * @return long sendMessageCounter.
   */
  long getSendMessageCounter();
  
  /**
   * Set method for staffId just for test.
   * 
   * @param staffId
   *        String
   */
  void setStaffId(String staffId);
  
  // Zhicheng Liu added
  /**
   * Check message for staff migrate.
   * 
   * @return IMessage
   */
  IMessage checkAMessage();
  
  /**
   * Recovery for the staff migrate.
   * 
   * @param incomedMessages
   *        Map<String, LinkedList<IMessage>>
   */
  void recoveryForMigrate(Map<String, LinkedList<IMessage>> incomedMessages);
  
  /*Biyahui added*/
  void recoveryForFault(Map<String, LinkedList<IMessage>> incomedMessages);
  
  /**
   * get the migrated messages to continue compute
   * 
   * @param valueOf
   * @return
   */
  ConcurrentLinkedQueue<IMessage> getMigrateMessageQueue(String valueOf);
  
  /*Biyahui added*/
  ConcurrentLinkedQueue<IMessage> getRecoveryMessageQueue(String valueOf);
  
  /**
   * added for test
   * 
   * @return
   */
  HashMap<Integer, String> getpartitionToWorkerManagerNameAndPort();
  
  /**
   * Send message to Partition ID.
   * 
   * @param msg
   * @throws IOException
   */
  void sendPartition(IMessage msg) throws IOException;
}
