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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.comm.MessageQueuesForDisk.BucketMeta;
import com.chinamobile.bcbsp.comm.io.util.MessageBytePoolPerPartition;
import com.chinamobile.bcbsp.comm.io.util.MessageSendCache;
import com.chinamobile.bcbsp.comm.io.util.MessageStore;
import com.chinamobile.bcbsp.comm.io.util.MessageStoreListProcessed;
import com.chinamobile.bcbsp.comm.io.util.MessageStoreProcessed;
import com.chinamobile.bcbsp.comm.io.util.WritableBSPMessages;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * Message queue change message object into byte array.
 * @author Liu Jinpeng-Delopment Version
 */

public class MessageQueuesNewByteArray implements MessageManagerInterface {
  /** class logger. */
  private static final Log LOG = LogFactory
      .getLog(MessageQueuesNewByteArray.class);
  /** Data Structure for storing messages to send dest site * bucketsNum. */
  private MessageSendCache msgSendCache;
  /**
   * Data Structure for storing messages that to be processed in this superstep
   * contents is like destination partition * bucketsNum.
   */
  private MessageStore currentMsgStore;
  /**
   * Data Structure for storing messages receiving from other sites and to be
   * processed in next superstep.
   */
  private MessageStore nextMsgStore;
  /** the handle of message disk manager. */
  private DiskManager diskManager;
  /** manager Message Data In Bytes. */
  private MessageBytePoolPerPartition singletonBytePartition;
  /** Hash bucket number. */
  private int hashBucketNumber;
  /**
   * Hash buckets of hash map of Incoming Message Queues. Hash indexed by
   * vertexID(int).
   */
  private ArrayList<BucketMeta> incomingQueues;
  /**
   * Hash buckets of hash map of Incomed Message Queues. Hash indexed by
   * vertexID(int).
   */
  private ArrayList<BucketMeta> incomedQueues;
  /**record the staff incomed messageNum*/
  private int incomedMessagesSize = 0;
  
  /**
   * Constructor for new BSP Message Queue.
   * @param job
   * @param partitionID
   */
  
  @SuppressWarnings("unchecked")
  public MessageQueuesNewByteArray(BSPJob job, int partitionID) {
    MetaDataOfMessage.initializeMBLength();
    // note Message Queue For Computing should be just cut into buckets not
    // partition-bucket
    if(job.isReceiveCombinerSetFlag()){
    	currentMsgStore = new MessageStoreListProcessed(MetaDataOfMessage.HASH_NUMBER,job);
        nextMsgStore = new MessageStoreListProcessed(MetaDataOfMessage.PARTITIONBUCKET_NUMBER,job);
    }else{
        currentMsgStore = new MessageStoreListProcessed(MetaDataOfMessage.HASH_NUMBER,job);
        nextMsgStore = new MessageStoreListProcessed(MetaDataOfMessage.PARTITIONBUCKET_NUMBER,job);
    }
//    currentMsgStore = new MessageStoreProcessed(MetaDataOfMessage.HASH_NUMBER);
//    nextMsgStore = new MessageStore(MetaDataOfMessage.PARTITIONBUCKET_NUMBER);
    //this.hashBucketNumber = job.getHashBucketNumber();
    //this.incomingQueues = new ArrayList<BucketMeta>(this.hashBucketNumber);
    //this.incomedQueues = new ArrayList<BucketMeta>(this.hashBucketNumber);
    
    msgSendCache = new MessageSendCache(
        MetaDataOfMessage.PARTITIONBUCKET_NUMBER);
    // Note Default Directory
    this.diskManager = new DiskManager("/tmp/bcbsp1",
        job.getJobID().toString(), partitionID,
        MetaDataOfMessage.PARTITIONBUCKET_NUMBER);
    singletonBytePartition = CommunicationFactory
        .createMsgBytesPoolPerPartition();
    // Note Add By Liu Jinpeng 20140329
    CommunicationFactory.setMessageClass(job.getMessageClass());
  }
  
  @Override
  public int outgoAMessage(int dstPartitionBucket, IMessage msg) {
    int size = this.msgSendCache.addMessage(msg, dstPartitionBucket);
    MetaDataOfMessage.SMBLength[dstPartitionBucket]++;
    // LOG.info("######################[In OutGo ] Size " + size +"[Threshold] "
    // +MetaDataOfMessage.MESSAGE_SEND_BUFFER_THRESHOLD);
    // Note* Need To Modify,
    if (size >= MetaDataOfMessage.MESSAGE_SEND_BUFFER_THRESHOLD) {
      return MetaDataOfMessage.BufferStatus.SPILL;
    }
    return MetaDataOfMessage.BufferStatus.NORMAL;
  }
  
  @Override
  public void incomeAMessage(int srcPartitionDstBucket,
      WritableBSPMessages msg, int superstep) {
    MessageBytePoolPerPartition messages = (MessageBytePoolPerPartition) msg;
    // Note Lots Of Works Is ExChanged To Message Store.
    // LOG.info("[Store Size ]"+
    // this.msgSendCache.getBytePoolPerPartition(srcPartitionDstBucket)
    // .getSize() + "[Bytes Received ] "+messages.getSize() +" [RThreshold] "+
    // MetaDataOfMessage.MESSAGE_RECEIVED_BUFFER_THRESHOLD);
    if ((messages.getSize() + this.msgSendCache.getBytePoolPerPartition(
        srcPartitionDstBucket).getSize()) < MetaDataOfMessage.MESSAGE_RECEIVED_BUFFER_THRESHOLD) {
      this.nextMsgStore.add(messages, srcPartitionDstBucket);
    } else {
      try {
        this.diskManager.processMessagesSave(messages, superstep,
            srcPartitionDstBucket);
      } catch (IOException e) {
        throw new RuntimeException(
            "[MessageQueuesNewByteArray] incomeAMessage exception: ", e);
      }
    }
    // LOG.info("Receive Message Count " + messages.getMsgCount());
    // LOG.info("Receive Message Size " + messages.getMsgSize());
    MetaDataOfMessage.RMBLength[srcPartitionDstBucket] += messages
        .getMsgCount();
  }
  
  @Override
  public void incomeAMessage(int srcPartitionDstBucket, IMessage msg,
      int superstep) {
  }
  
  @Override
  public void exchangeIncomeQueues() {
    // Note Messages Received From Other Partition Statement.
    stateRMBLength();
    // Note Messages Left In Sending Buffer Statement. Error While None-Zero.
    stateSMBLength();
    this.currentMsgStore.clean();
    this.currentMsgStore.refreshMessageBytePools(nextMsgStore);
    MetaDataOfMessage.recreateRMBLength();
    // Note Message Left In Receiving Buffer Statement.Error While None-Zero.
    verifyNetworkedMessages();
  }
  
  /** Verify the local message queues clear. */
  private void verifyNetworkedMessages() {
    int count = MetaDataOfMessage.computeRMBLength();
    if (count > 0) {
      LOG.error("<Networked Messages Do Not Equal Zero When Exchange()>");
    }
  }
  
  /** Show the length of receive message buffer length. */
  private void stateRMBLength() {
    int count = MetaDataOfMessage.computeRMBLength();
    LOG.info("<(STATISTICS)Total Messages In RMB is >   " + count);
  }
  
  /** Show the length of send message buffer length. */
  private void stateSMBLength() {
    int count = MetaDataOfMessage.computeSMBLength();
    LOG.info("<(STATISTICS)Total Messages In SMB is >   " + count);
  }
  
  // Note Get Array list To Send. Unified Interface WritableBSPMessages
  @Override
  public WritableBSPMessages removeOutgoingQueue(int destPartitionBucket) {
    WritableBSPMessages messages = this.msgSendCache
        .removeMsgBytePool(destPartitionBucket);
    MetaDataOfMessage.SMBLength[destPartitionBucket] -= messages.getMsgCount();
    return messages;
  }
  
  // Note Request For The Size Of One DstPartition-DstBucket Structure.Using In
  // Send Remaining Messages Procedure.
  @Override
  public int getOutgoingQueueSize(int dstPartitionBucket) {
    return this.msgSendCache.getBytePoolPerPartition(dstPartitionBucket)
        .getMsgCount();
  }
  
  @Override
  public ArrayList<IMessage> removeIncomedQueue(String dstVertexID) {
    int hashIndex = PartitionRule.localPartitionRule(dstVertexID);
    ArrayList<IMessage> tmp = this.currentMsgStore.removeMessagesForOneVertex(
        dstVertexID, hashIndex);
    return tmp;
    // return this.localQueues[hashIndex].remove(dstVertexID);
  }
  
  @Override
  public void clearOutgoingQueues() {
    // TODO Auto-generated method stub
  }
  
  @Override
  public void clearIncomingQueues() {
    // TODO Auto-generated method stub
  }
  
  @Override
  public void clearIncomedQueues() {
    for (int i = 0; i < MetaDataOfMessage.HASH_NUMBER; i++) {
      this.currentMsgStore.clean();
    }
  }
  
  @Override
  public void showMemoryInfo() {
    // TODO Auto-generated method stub
  }
  
  // Note Messages Is Processed Bucket By Bucket. So All Messages Should Be In
  // Memory While Disk Serves.
  @Override
  public void loadBucketMessage(int bucket, int superStep) {
    this.currentMsgStore.preLoadMessages(bucket, superStep);
    // Note Disk Bucket Process.
    File[] fList = this.diskManager.preLoadFile(superStep, bucket);
    if (fList == null) {
    	//LOG.info(" Feng test File list is empty!");
      return;
    }
    try {
      for (int i = 0; i < fList.length; i++) {
        this.diskManager.processMessageLoadFile(singletonBytePartition,
            fList[i]);
        this.currentMsgStore.addMsgBytePoolToMsgObjects(singletonBytePartition);
        this.singletonBytePartition.reInitialize();
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "[MessageQueuesNewByteArray] loadBucketMessage exception: ", e);
    }
  }
  /*Biyahui added*/
  @Override
  public void loadBucketMessageNew(int bucket, int superStep) {
    this.currentMsgStore.preLoadMessagesNew(bucket, superStep);
    // Note Disk Bucket Process.
    File[] fList = this.diskManager.preLoadFile(superStep, bucket);
    if (fList == null) {
    	//LOG.info(" Feng test File list is empty!");
      return;
    }
    try {
      for (int i = 0; i < fList.length; i++) {
        this.diskManager.processMessageLoadFile(singletonBytePartition,
            fList[i]);
        this.currentMsgStore.addMsgBytePoolToMsgObjects(singletonBytePartition);
        this.singletonBytePartition.reInitialize();
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "[MessageQueuesNewByteArray] loadBucketMessage exception: ", e);
    }
  }
  
  // Note None-Used
  @Override
  public String getMaxOutgoingQueueIndex() {
    return null;
  }
  
  // Note None-Used
  @Override
  public String getNextOutgoingQueueIndex() throws Exception {
    return null;
  }
  
  // Note None-Used
  @Override
  public String getMaxIncomingQueueIndex() {
    // TODO Auto-generated method stub
    return null;
  }
  
  // Note None-Used
  @Override
  public ConcurrentLinkedQueue<IMessage> removeIncomingQueue(String dstVertID) {
    // TODO Auto-generated method stub
    return null;
  }
  
  // Note None-Used
  @Override
  public int getIncomingQueueSize(String dstVertexID) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  // Note None-Used
  @Override
  public void clearAllQueues() {
    // TODO Auto-generated method stub
  }
  
  // Note None-Used
  @Override
  public int getOutgoingQueuesSize() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  // Note None-Used
  @Override
  public int getIncomingQueuesSize() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  // Note None-Used
  @Override
  public int getIncomedQueuesSize() {
    // TODO Auto-generated method stub
	  int size = 0;
	  size = MetaDataOfMessage.computeRMBLength();
	    return size;
  }

  public MessageBytePoolPerPartition getSingletonBytePartition() {
    return singletonBytePartition;
  }
}
