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
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * New version of MessageQueue.
 * @author Liu Jinpeng-Delopment Version
 */
public class MessageQueuesNew implements MessageManagerInterface {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(MessageQueuesNew.class);
  /** Data Structure for storing messages to send dest site * bucketsNum. */
  private ArrayList<IMessage>[] networkedQueues;
  /**
   * Data Structure for storing messages that to be processed in this superstep
   * contents is like destination partition * bucketsNum.
   */
  private HashMap<String, ArrayList<IMessage>>[] localQueues;
  /**
   * Data Structure for storing messages receiving from other sites and to be
   * processed in next superstep.
   */
  private ArrayList<IMessage>[] nextForLocalQueues;
  /** the handle of message disk manager. */
  private DiskManager diskManager;
  
  /**
   * Constructor for new BSP Message Queue
   * @param job
   * @param partitionID
   */
  @SuppressWarnings("unchecked")
  public MessageQueuesNew(BSPJob job, int partitionID) {
    MetaDataOfMessage.initializeMBLength();
    // note Message Queue For Computing should be just cut into buckets not
    // partition-bucket
    localQueues = new HashMap[MetaDataOfMessage.HASH_NUMBER];
    nextForLocalQueues =
        new ArrayList[MetaDataOfMessage.PARTITIONBUCKET_NUMBER];
    networkedQueues = new ArrayList[MetaDataOfMessage.PARTITIONBUCKET_NUMBER];
    for (int i = 0; i < MetaDataOfMessage.PARTITIONBUCKET_NUMBER; i++) {
      nextForLocalQueues[i] = new ArrayList<IMessage>();
      networkedQueues[i] = new ArrayList<IMessage>();
    }
    for (int i = 0; i < MetaDataOfMessage.HASH_NUMBER; i++) {
      localQueues[i] = new HashMap<String, ArrayList<IMessage>>();
    }
    // Note Default Directory
    this.diskManager = new DiskManager("/tmp/bcbsp1",
        job.getJobID().toString(), partitionID,
        MetaDataOfMessage.PARTITIONBUCKET_NUMBER);
  }
  
  @Override
  public int outgoAMessage(int dstPartitionBucket, IMessage msg) {
    this.networkedQueues[dstPartitionBucket].add(msg);
    MetaDataOfMessage.SMBLength[dstPartitionBucket]++;
    if (MetaDataOfMessage.SMBLength[dstPartitionBucket] >=
        MetaDataOfMessage.MESSAGE_SEND_BUFFER_THRESHOLD) {
      return MetaDataOfMessage.BufferStatus.SPILL;
    }
    return MetaDataOfMessage.BufferStatus.NORMAL;
  }
  
  @Override
  public void incomeAMessage(int srcPartitionDstBucket,
      WritableBSPMessages msg, int superstep) {
    ArrayList<IMessage> msgList = ((BSPMessagesPack) msg).getPack();
    int count = msgList.size();
    // if(MetaDataOfMessage.RMBLength[srcPartitionDstBucket]<
    // MetaDataOfMessage.MESSAGE_RECEIVED_BUFFER_THRESHOLD)
    // if(MetaDataOfMessage.RMBLength[srcPartitionDstBucket]< 3000)
    // LOG.info("TAGTAG1");
    int remain = MetaDataOfMessage.MESSAGE_RECEIVED_BUFFER_THRESHOLD
        - MetaDataOfMessage.RMBLength[srcPartitionDstBucket];
    int counter = 0;
    while (count-- > 0 && remain-- > 0) {
      counter++;
      incomeAMessage(srcPartitionDstBucket, msgList.remove(0), superstep);
    }
    // Note count Is -1 Afer The Recycle Before.
    count++;
    MetaDataOfMessage.RMBLength[srcPartitionDstBucket] += counter;
    if (count == 0) {
      return;
    }
    // Note Write Disk Procedure. Named Like SpilledData.
    // LOG.info("######### "+count +"$$$$$$$$ " +msgList.size());
    try {
      this.diskManager.processMessagesSave(msgList, superstep,
          srcPartitionDstBucket);
    } catch (IOException e) {
      throw new RuntimeException("[MessageQueuesNew] incomeAMessage exception:", e);
    }
    MetaDataOfMessage.RMBLength[srcPartitionDstBucket] += count;
  }
  
  @Override
  public void incomeAMessage(int srcPartitionDstBucket, IMessage msg,
      int superstep) {
    this.nextForLocalQueues[srcPartitionDstBucket].add(msg);
  }
  
  @Override
  public void exchangeIncomeQueues() {
    // Note Messages Received From Other Partition Statement.
    stateRMBLength();
    // Note Messages Left In Sending Buffer Statement. Error While None-Zero.
    stateSMBLength();
    clearLocalHashMap();
    buildLocalHashMap();
    MetaDataOfMessage.recreateRMBLength();
    // Note Message Left In Receiving Buffer Statement.Error While None-Zero.
    verifyNetworkedMessages();
  }
  
  /** Clear the local message queues. */
  private void clearLocalHashMap() {
    for (int i = 0; i < MetaDataOfMessage.HASH_NUMBER; i++) {
      this.localQueues[i].clear();
    }
  }
  
  /** Build the local message queues. */
  private void buildLocalHashMap() {
    int partitionBucket = 0;
    IMessage m = null;
    ArrayList<IMessage> mList = null;
    for (int i = 0; i < MetaDataOfMessage.HASH_NUMBER; i++) {
      for (int j = 0; j < MetaDataOfMessage.PARTITION_NUMBER; j++) {
        partitionBucket = j * MetaDataOfMessage.HASH_NUMBER + i;
        while (!this.nextForLocalQueues[partitionBucket].isEmpty()) {
          m = this.nextForLocalQueues[partitionBucket].remove(0);
          mList = this.localQueues[i].get(m.getDstVertexID());
          if (mList == null) {
            mList = new ArrayList<IMessage>();
            this.localQueues[i].put(m.getDstVertexID(), mList);
          }
          mList.add(m);
        }
      }
    }
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
    LOG.info("<(STATISTICS)Total Messages In RMB is >" + count);
  }
  
  /** Show the length of send message buffer length. */
  private void stateSMBLength() {
    int count = MetaDataOfMessage.computeSMBLength();
    LOG.info("<(STATISTICS)Total Messages In SMB is >" + count);
  }
  
  // Note Get Array list To Send
  @Override
  public WritableBSPMessages removeOutgoingQueue(int destPartitionBucket) {
    ArrayList<IMessage> mList = this.networkedQueues[destPartitionBucket];
    this.networkedQueues[destPartitionBucket] = new ArrayList<IMessage>();
    MetaDataOfMessage.SMBLength[destPartitionBucket] -= mList.size();
    BSPMessagesPack bspMP = new BSPMessagesPack();
    bspMP.setPack(mList);
    return bspMP;
  }
  
  // Note Request For The Size Of One DstPartition-DstBucket Structure.Using In
  // Send Remaining Messages Procedure.
  @Override
  public int getOutgoingQueueSize(int dstPartitionBucket) {
    return MetaDataOfMessage.SMBLength[dstPartitionBucket];
  }
  
  @Override
  public ArrayList<IMessage> removeIncomedQueue(String dstVertexID) {
    int hashIndex = PartitionRule.localPartitionRule(dstVertexID);
    ArrayList<IMessage> tmp = this.localQueues[hashIndex].remove(dstVertexID);
    if (tmp == null){
      tmp = new ArrayList<IMessage>();
    }
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
      localQueues[i].clear();
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
    try {
      this.diskManager.processMessageLoad(this.localQueues[bucket], superStep,
          bucket);
    } catch (IOException e) {
      throw new RuntimeException("[MessageQueuesNew] loadBucketMessage exception:", e);
    }
  }
  //Biyahui added
	public void loadBucketMessageNew(int bucket, int superStep) {
		try {
			this.diskManager.processMessageLoad(this.localQueues[bucket],
					superStep, bucket);
		} catch (IOException e) {
			throw new RuntimeException(
					"[MessageQueuesNew] loadBucketMessage exception:", e);
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
    return 0;
  }
  
}
