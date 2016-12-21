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

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.comm.io.util.WritableBSPMessages;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.router.route;
import com.chinamobile.bcbsp.router.routeparameter;
import com.chinamobile.bcbsp.router.simplerouteFactory;
import com.chinamobile.bcbsp.rpc.RPCCommunicationProtocol;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * RPC Communicator. The communication tool with RPC for BSP. It manages the
 * outgoing and incoming queues of each staff.
 */
public class RPCCommunicator implements CommunicatorInterface,
    RPCCommunicationProtocol {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(RPCCommunicator.class);
  /** default message pack size. */
  private static final int MESSAGE_PACK_SIZE = 1000;
  /** default max producer number pack. */
  private static final int MAX_PRODUCER_NUM = 20;
  /** combine threshold. */
  private static final int COMBINE_THRESOLD = 8;
  /** compute combine threshold first parameter. */
  private static final double CON_FIR_COMBINE_THRESOLD = 0.4;
  /** compute combine threshold second parameter. */
  private static final double CON_SEC_COMBINE_THRESOLD = 10;
  /** BSP Job ID. */
  private BSPJobID jobID = null;
  /** BSP Job. */
  private BSPJob job = null;
  /** edge size of partition. */
  private int edgeSize = 1;
  /** partition ID. */
  private int partitionID = 0;
  /** Graph data interface. */
  private GraphDataInterface graphData = null;
  /** route parameter of Communicator. */
  private routeparameter routeparameter = null;
  /** route table. */
  private route route = null;
  /** communicate sender. */
  private RPCSender sender = null;
  /** communicate receiver. */
  private RPCReceiver receiver = null;
  /** Route Table: HashID---PartitionID. */
  private HashMap<Integer, Integer> hashBucketToPartition = null;
  /** Route Table: PartitionID---WorkerManagerNameAndPort(HostName:Port). */
  private HashMap<Integer, String> partitionToWorkerManagerNameAndPort = null;
  /** The message queues manager. */
  private MessageQueuesInterface messageQueues = null;
  /** partitioner interface. */
  private Partitioner<Text> partitioner;
  /** Sending message counter for a super step. */
  private long sendMessageCounter;
  /** Outgoing message counter for a super step. */
  private long outgoMessageCounter;
  /** RPC server. */
  private Server server;
  /** Outgoing message counter for a super step after combine. */
  private long combineOutgoMessageCounter;
  /** Outgoing message bytes counter for a super step after combine. */
  private long combineOutgoMessageBytesCounter;
  /** Received message counter for a super step. */
  private volatile long receivedMessageCounter;
  /** Received message bytes counter for a super step. */
  private volatile long receivedMessageBytesCounter;
  /** Outgoing message bytes counter for a super step. */
  private long outgoMessageBytesCounter;
  /** Staff ID of Job (for test). */
  private String staffId;
  /** BSP staff. */
  // Note Add Finally For RPCServer Setup And Close.Tmprory.20140313
  private BSPStaff bspStaff;
  
  /**
   * Constructor of RPCCommunicator.
   * @param jobID
   * @param job
   * @param partitionID
   * @param partitioner
   */
  public RPCCommunicator(BSPJobID jobID, BSPJob job, int partitionID,
      Partitioner<Text> partitioner) {
    this.jobID = jobID;
    this.job = job;
    this.partitionID = partitionID;
    this.partitioner = partitioner;
    this.sendMessageCounter = 0;
    this.outgoMessageCounter = 0;
    int version = job.getMessageQueuesVersion();
    // Note Add 20140329
    CommunicationFactory.setMessageClass(BSPMessage.class);
    if (version == job.MEMORY_VERSION) {
      this.messageQueues = new MessageQueuesForMem();
    } else if (version == job.DISK_VERSION) {
      this.messageQueues = new MessageQueuesForDisk(job, partitionID);
    } else if (version == job.BDB_VERSION) {
      this.messageQueues = new MessageQueuesForBDB(job, partitionID);
    }
  }
  
  @Override
  public void initialize(routeparameter routeparameter,
      HashMap<Integer, String> aPartitionToWorkerManagerNameAndPort,
      GraphDataInterface aGraphData) {
    this.routeparameter = routeparameter;
    this.partitionToWorkerManagerNameAndPort = aPartitionToWorkerManagerNameAndPort;
    this.graphData = aGraphData;
    this.edgeSize = this.graphData.getEdgeSize();
  }
  
  @Override
  public void initialize(routeparameter routeparameter,
      HashMap<Integer, String> aPartitionToWorkerManagerNameAndPort,
      int edgeSize) {
    // TODO Auto-generated method stub
    this.routeparameter = routeparameter;
    this.partitionToWorkerManagerNameAndPort = aPartitionToWorkerManagerNameAndPort;
    this.edgeSize = edgeSize;
  }
  
  /** Set method of BSPJobID. */
  public void setBSPJobID(BSPJobID jobID) {
    this.jobID = jobID;
  }
  
  /**
   * Get method of BSPJobID.
   * @return BSPJobID
   */
  public BSPJobID getBSPJobID() {
    return this.jobID;
  }
  
  /**
   * Get method of BSPJob.
   * @return BSPJob
   */
  public BSPJob getJob() {
    return job;
  }
  
  /** Set method of BSPJob. */
  public void setJob(BSPJob job) {
    this.job = job;
  }
  
  /**
   * Set method of partitionID.
   * @return partitionID
   */
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }
  
  /**
   * Get method of partitionID.
   * @return int
   */
  public int getPartitionID() {
    return this.partitionID;
  }
  
  /**
   * Get method of messageQueues.
   * @return MessageQueuesInterface
   */
  public MessageQueuesInterface getMessageQueues() {
    return this.messageQueues;
  }
  
  /**
   * Get the dstPartitionID from the vertexID.
   * @param vertexID
   * @return dstPartitionID
   */
  @SuppressWarnings("unused")
  private int getDstPartitionID(int vertexID) {
    int dstPartitionID = 0;
    for (Entry<Integer, Integer> e : this.hashBucketToPartition.entrySet()) {
      List<Integer> rangeList = new ArrayList<Integer>(e.getValue());
      if ((vertexID <= rangeList.get(1)) && (vertexID >= rangeList.get(0))) {
        dstPartitionID = e.getKey(); // destination partition id
        break;
      }
    }
    return dstPartitionID;
  }
  
  /**
   * Get the dstWorkerManagerName from the dstPartitionID.
   * @param dstPartitionID
   * @return dstWorkerManagerName
   */
  private String getDstWorkerManagerNameAndPort(int dstPartitionID) {
    String dstWorkerManagerNameAndPort = null;
    dstWorkerManagerNameAndPort = this.partitionToWorkerManagerNameAndPort
        .get(dstPartitionID);
    return dstWorkerManagerNameAndPort;
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public void send(IMessage msg) throws IOException {
    Text vertexID = new Text(msg.getDstVertexID());
    this.route = simplerouteFactory.createroute(this.routeparameter);
    int dstPartitionID = route.getpartitionID(vertexID);
    // The destination partition is just in this staff.
    if (dstPartitionID == this.partitionID) {
      String dstVertexID = msg.getDstVertexID();
      messageQueues.incomeAMessage(dstVertexID, msg);
    } else {
      String dstWorkerManagerNameAndPort = this
          .getDstWorkerManagerNameAndPort(dstPartitionID);
      String outgoingIndex = dstWorkerManagerNameAndPort;
      messageQueues.outgoAMessage(outgoingIndex, msg);
      this.outgoMessageCounter++;
      this.outgoMessageBytesCounter += msg.size();
    }
    this.sendMessageCounter++;
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public void sendToAllEdges(IMessage msg) throws IOException {
    // TODO Auto-generated method stub
  }
  
  @Override
  public Iterator<IMessage> getMessageIterator(String vertexID)
      throws IOException {
    ConcurrentLinkedQueue<IMessage> incomedQueue = messageQueues
        .removeIncomedQueue(vertexID);
    Iterator<IMessage> iterator = incomedQueue.iterator();
    return iterator;
  }
  
  @Override
  public ConcurrentLinkedQueue<IMessage> getMessageQueue(String vertexID)
      throws IOException {
    return messageQueues.removeIncomedQueue(vertexID);
  }
  
  @Override
  public void start() {
    // allocate RPC Sender
    this.sender = new RPCSender(this);
    this.sender
        .setCombineThreshold((int) (edgeSize * CON_FIR_COMBINE_THRESOLD / CON_SEC_COMBINE_THRESOLD));
    this.sender
        .setSendThreshold((int) (edgeSize * CON_FIR_COMBINE_THRESOLD / CON_SEC_COMBINE_THRESOLD));
    this.sender.setMessagePackSize(MESSAGE_PACK_SIZE);
    this.sender.setMaxProducerNum(MAX_PRODUCER_NUM);
    this.sender.start();
    // Allocate RPC Receiver
    this.receiver = new RPCReceiver(this);
    this.receiver.setCombineThreshold(COMBINE_THRESOLD);
  }
  
  @Override
  public void begin(int superStepCount) {
    this.receiver.setSuperStepCounter(superStepCount);
    this.sender.setStaffId(this.staffId);
    this.sender.begin(superStepCount);
  }
  
  @Override
  public void complete() {
    LOG.info("[RPCComm] execute complete ");
    this.sender.complete();
    // Add For Finish RPC Service Server.
    this.stopRPCSever();
    
  }
  
  @Override
  public void noMoreMessagesForSending() {
    this.sender.setNoMoreMessagesFlag(true);
  }
  
  @Override
  public boolean isSendingOver() {
    boolean result = false;
    if (this.sender.isOver()) {
      result = true;
    }
    return result;
  }
  
  @Override
  public void noMoreMessagesForReceiving() {
    this.receivedMessageBytesCounter = this.receiver.getMessageBytesCount();
    this.receivedMessageCounter = this.receiver.getMessageCount();
    this.receiver.recordMsgNum();
  }
  
  @Override
  public boolean isReceivingOver() {
    return this.receiver.isReceiveOver();
  }
  
  @Override
  public void exchangeIncomeQueues() {
    // messageQueues.showMemoryInfo();
    messageQueues.exchangeIncomeQueues();
    LOG.info("[Communicator] has sent " + this.sendMessageCounter
        + " messages totally.");
    LOG.info("[Communicator] has outgo " + this.outgoMessageCounter
        + " messages totally.");
    this.receiver.resetCounters();
    this.outgoMessageBytesCounter = 0;
    this.sendMessageCounter = 0;
    this.outgoMessageCounter = 0;
  }
  
  @Override
  public int getOutgoingQueuesSize() {
    return messageQueues.getOutgoingQueuesSize();
  }
  
  @Override
  public int getIncomingQueuesSize() {
    return messageQueues.getIncomingQueuesSize();
  }
  
  @Override
  public int getIncomedQueuesSize() {
    return messageQueues.getIncomedQueuesSize();
  }
  
  @Override
  public void clearAllQueues() {
    messageQueues.clearAllQueues();
    
  }
  
  @Override
  public void clearOutgoingQueues() {
    messageQueues.clearOutgoingQueues();
  }
  
  @Override
  public void setPartitionToWorkerManagerNamePort(HashMap<Integer, String> value) {
    this.partitionToWorkerManagerNameAndPort = value;
  }
  
  @Override
  public void clearIncomingQueues() {
    messageQueues.clearIncomingQueues();
  }
  
  @Override
  public void clearIncomedQueues() {
    messageQueues.clearIncomedQueues();
  }
  
  /**
   * Get method of combinerSetFlag.
   * @return boolean
   */
  public boolean isSendCombineSetFlag() {
    return this.job.isCombinerSetFlag();
  }
  
  /**
   * Get method of receiveCombineSetFlag.
   * @return boolean
   */
  public boolean isReceiveCombineSetFlag() {
    return this.job.isReceiveCombinerSetFlag();
  }
  
  public Combiner getCombiner() {
    if (this.job != null) {
      return (Combiner) ReflectionUtils.newInstance(
          job.getConf().getClass(Constants.USER_BC_BSP_JOB_COMBINER_CLASS,
              Combiner.class), job.getConf());
    } else {
      return null;
    }
  }
  
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return this.protocolVersion;
  }
  
  @Override
  public int sendPackedMessage(BSPMessagesPack packedMessages) {
    return this.receiver.receivePackedMessage(packedMessages);
  }
  
  @Override
  public int sendPackedMessage(BSPMessagesPack packedMessages, String str) {
    return this.receiver.receivePackedMessage(packedMessages, str);
  }
  
  @Override
  public int sendUnpackedMessage(IMessage messages) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public long getReceivedMessageCounter() {
    return receivedMessageCounter;
  }
  
  /** Set method of receivedMessageCounter. */
  public void setReceivedMessageCounter(long recievedMessageCounter) {
    this.receivedMessageCounter = recievedMessageCounter;
  }
  
  @Override
  public long getReceivedMessageBytesCounter() {
    return receivedMessageBytesCounter;
  }
  
  /** Set method of receivedMessageBytesCounter. */
  public void setReceivedMessageBytesCounter(long receivedMessageBytesCounter) {
    this.receivedMessageBytesCounter = receivedMessageBytesCounter;
  }
  
  @Override
  public long getOutgoMessageBytesCounter() {
    return outgoMessageBytesCounter;
  }
  
  /** Set method of outgoMessageBytesCounter. */
  public void setOutgoMessageBytesCounter(long outgoMessageBytesCounter) {
    this.outgoMessageBytesCounter = outgoMessageBytesCounter;
  }
  
  @Override
  public long getOutgoMessageCounter() {
    return outgoMessageCounter;
  }
  
  /** Set method of outgoMessageCounter. */
  public void setOutgoMessageCounter(long outgoMessageCounter) {
    this.outgoMessageCounter = outgoMessageCounter;
  }
  
  @Override
  public long getCombineOutgoMessageCounter() {
    return combineOutgoMessageCounter;
  }
  
  /** Set method of combineOutgoMessageCounter. */
  public void setCombineOutgoMessageCounter(long combineOutgoMessageCounter) {
    this.combineOutgoMessageCounter = combineOutgoMessageCounter;
  }
  
  @Override
  public long getCombineOutgoMessageBytesCounter() {
    return combineOutgoMessageBytesCounter;
  }
  
  /** Set method of combineOutgoMessageBytesCounter. */
  public void setCombineOutgoMessageBytesCounter(
      long combineOutgoMessageBytesCounter) {
    this.combineOutgoMessageBytesCounter = combineOutgoMessageBytesCounter;
  }
  
  /**
   * Get method of sendMessageCounter.
   * @return long
   */
  public long getSendMessageCounter() {
    return sendMessageCounter;
  }
  
  /** Set method of sendMessageCounter. */
  public void setSendMessageCounter(long sendMessageCounter) {
    this.sendMessageCounter = sendMessageCounter;
  }
  
  /**
   * Get method of staffId.
   * @return long
   */
  public String getStaffId() {
    return staffId;
  }
  
  /** Set method of staffId. */
  public void setStaffId(String staffId) {
    this.staffId = staffId;
  }
  
  /* Zhicheng Liu added */
  @Override
  public IMessage checkAMessage() {
    return this.messageQueues.getAMessage();
  }
  
  @Override
  public void recoveryForMigrate(
      Map<String, LinkedList<IMessage>> incomedMessages) {
    Iterator<String> it = incomedMessages.keySet().iterator();
    while (it.hasNext()) {
      String vertexID = it.next();
      addMessageForMigrate(vertexID, incomedMessages.get(vertexID));
    }
    messageQueues.exchangeIncomeQueues();
  }
  /*biyahui added*/
  @Override
	public void recoveryForFault(
			Map<String, LinkedList<IMessage>> incomedMessages) {
		Iterator<String> it = incomedMessages.keySet().iterator();
		while (it.hasNext()) {
			String vertexID = it.next();
			addMessageForMigrate(vertexID, incomedMessages.get(vertexID));
		}
		messageQueues.exchangeIncomeQueues();
	}
  
  /** Add message for staff mograte. */
  private void addMessageForMigrate(String vertexID, List<IMessage> messages) {
    Iterator<IMessage> it = messages.iterator();
    while (it.hasNext()) {
      messageQueues.incomeAMessage(vertexID, it.next());
    }
  }
  
  // ==========Note add 20140310 for normalization with the new communication
  
  /**
   * Add To Replace The Method Start And Do Some Reconstruction.
   */
  @Override
  public void start(String hostname, BSPStaff bspStaff) {
    this.bspStaff = bspStaff;
    startServer(hostname);
    start();
  }
  
  /** Start The RPC Server Using Reference Of BSPStaff. */
  public void startServer(String hostName) {
    this.bspStaff.startRPCServer(hostName);
  }
  
  /** Stop The RPC Using BSPStaff. */
  public void stopRPCSever() {
    this.bspStaff.stopRPCSever();
  }
  
  @Override
  public void preBucketMessages(int bucket, int superstep) {
    // TODO Auto-generated method stub
  }

  public void preBucketMessagesNew(int bucket, int superstep) {
	  // TODO Auto-generated method stub
  }
  
  @Override
  public int sendPackedMessage(WritableBSPMessages packedMessages,
      int srcPartition, int dstBucket, int superstep) {
    // TODO Auto-generated method stub
    return 0;
  }
  // =====================================end 20140310=====

@Override
public ConcurrentLinkedQueue<IMessage> getMigrateMessageQueue(String valueOf) {
	// TODO Auto-generated method stub
	return null;
}
/*Biyahui added*/
@Override
public ConcurrentLinkedQueue<IMessage> getRecoveryMessageQueue(String valueOf) {
	// TODO Auto-generated method stub
	return null;
}

@Override
public HashMap<Integer, String> getpartitionToWorkerManagerNameAndPort() {
	// TODO Auto-generated method stub
	return null;
}

public int getEdgeSize() {
  return edgeSize;
}

public HashMap<Integer, String> getPartitionToWorkerManagerNameAndPort() {
  return partitionToWorkerManagerNameAndPort;
}

public routeparameter getRouteparameter() {
  return routeparameter;
}

public void setRouteparameter(routeparameter routeparameter) {
  this.routeparameter = routeparameter;
}

@Override
public void sendPartition(IMessage msg) throws IOException {
  // TODO Auto-generated method stub
  
}
}
