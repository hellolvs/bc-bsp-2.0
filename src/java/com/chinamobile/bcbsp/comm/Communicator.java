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
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.router.route;
import com.chinamobile.bcbsp.router.routeparameter;
import com.chinamobile.bcbsp.router.simplerouteFactory;
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
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Communicator. The communication tool for BSP. It manages the outgoing and
 * incoming queues of each staff.
 */
public class Communicator implements CommunicatorInterface {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(Communicator.class);
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
  /** partition ID. */
  private int partitionID = 0;
  /** edge size of partition. */
  private int edgeSize = 1;
  /** Graph data interface. */
  private GraphDataInterface graphData = null;
  /** route parameter of Communicator. */
  private routeparameter routeparameter = null;
  /** route table. */
  private route route = null;
  /** communicate sender. */
  private Sender sender = null;
  /** communicate receiver. */
  private Receiver receiver = null;
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
  // add by chen
  /** Outgoing message counter for a super step after combine. */
  private long combineOutgoMessageCounter;
  /** Outgoing message bytes counter for a super step after combine. */
  private long combineOutgoMessageBytesCounter;
  /** Received message counter for a super step. */
  private long receivedMessageCounter;
  /** Received message bytes counter for a super step. */
  private long receivedMessageBytesCounter;
  /** Outgoing message bytes counter for a super step after combine. */
  private long outgoMessageBytesCounter;
  /** BSP staff. */
  private BSPStaff bspStaff;
  
  /**
   * Constructor.
   * @param jobID
   * @param job
   * @param partitionID
   * @param partitioner
   */
  public Communicator(BSPJobID jobID, BSPJob job, int partitionID,
      Partitioner<Text> partitioner) {
    this.jobID = jobID;
    this.job = job;
    this.partitionID = partitionID;
    this.partitioner = partitioner;
    this.sendMessageCounter = 0;
    this.outgoMessageCounter = 0;
    // add by chen
    this.outgoMessageBytesCounter = 0;
    // Note Add 20140329
    CommunicationFactory.setMessageClass(BSPMessage.class);
    int version = job.getMessageQueuesVersion();
    if (version == job.MEMORY_VERSION) {
      this.messageQueues = new MessageQueuesForMem();
    } else if (version == job.DISK_VERSION) {
      this.messageQueues = new MessageQueuesForDisk(job, partitionID);
    } else if (version == job.BDB_VERSION) {
      this.messageQueues = new MessageQueuesForBDB(job, partitionID);
    }
  }
  
  /**
   * Initialize the communicator.
   * @param routeparameter
   * @param aPartiToWorkerManagerNameAndPort
   * @param aGraphData
   */
  @Override
  public void initialize(routeparameter routeparameter,
      HashMap<Integer, String> aPartiToWorkerManagerNameAndPort,
      GraphDataInterface aGraphData) {
    this.routeparameter = routeparameter;
    this.partitionToWorkerManagerNameAndPort = aPartiToWorkerManagerNameAndPort;
    this.graphData = aGraphData;
  }
  
  /**
   * Initialize the communicator.
   * @param routeparameter
   * @param aPartiToWorkerManagerNameAndPort
   * @param edgeSize
   */
  @Override
  public void initialize(routeparameter routeparameter,
      HashMap<Integer, String> aPartiToWorkerManagerNameAndPort, int edgeSize) {
    // TODO Auto-generated method stub
    this.routeparameter = routeparameter;
    this.partitionToWorkerManagerNameAndPort = aPartiToWorkerManagerNameAndPort;
    this.edgeSize = edgeSize;
  }
  
  /** set method of BSPJobID. */
  public void setBSPJobID(BSPJobID jobID) {
    this.jobID = jobID;
  }
  
  /** get method of BSPJobID. */
  public BSPJobID getBSPJobID() {
    return this.jobID;
  }
  
  /** get method of BSPJob. */
  public BSPJob getJob() {
    return job;
  }
  
  /** get method of BSPJob. */
  public void setJob(BSPJob job) {
    this.job = job;
  }
  
  /** set method of partitionID. */
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }
  
  /** get method of partitionID. */
  public int getPartitionID() {
    return this.partitionID;
  }
  
  /** get method of MessageQueues. */
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
  
  /** set method of partitionToWorkerManagerNameAndPort. */
  @Override
  public void setPartitionToWorkerManagerNamePort(HashMap<Integer, String> value) {
    this.partitionToWorkerManagerNameAndPort = value;
  }
  
  /*
   * (non-Javadoc)
   * @see
   * com.chinamobile.bcbsp.comm.CommunicatorInterface#GetMessageIterator(java
   * .lang.String)
   */
  @Override
  public Iterator<IMessage> getMessageIterator(String vertexID)
      throws IOException {
    ConcurrentLinkedQueue<IMessage> incomedQueue = messageQueues
        .removeIncomedQueue(vertexID);
    Iterator<IMessage> iterator = incomedQueue.iterator();
    return iterator;
  }
  
  /*
   * (non-Javadoc)
   * @see com.chinamobile.bcbsp.comm.CommunicatorInterface#GetMessageQueue(java
   * .lang.String)
   */
  @Override
  public ConcurrentLinkedQueue<IMessage> getMessageQueue(String vertexID)
      throws IOException {
    return messageQueues.removeIncomedQueue(vertexID);
  }
  
  /*
   * (non-Javadoc)
   * @see
   * com.chinamobile.bcbsp.comm.CommunicatorInterface#Send(java.lang.String,
   * com.chinamobile.bcbsp.bsp.BSPMessage)
   */
  @Override
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
      // add by chen
      this.outgoMessageBytesCounter += msg.size();
      this.outgoMessageCounter++;
    }
    this.sendMessageCounter++;
  }
  
  /** send message to all neighbors. */
  @Override
  public void sendToAllEdges(IMessage msg) throws IOException {
  }
  
  /** clear the message queues. */
  @Override
  public void clearAllQueues() {
    messageQueues.clearAllQueues();
  }
  
  /** clear outgoing message queues. */
  @Override
  public void clearOutgoingQueues() {
    messageQueues.clearOutgoingQueues();
  }
  
  /** clear incoming queues. */
  @Override
  public void clearIncomingQueues() {
    messageQueues.clearIncomingQueues();
  }
  
  /** clear incomed message queues. */
  @Override
  public void clearIncomedQueues() {
    messageQueues.clearIncomedQueues();
  }
  
  @Override
  public void exchangeIncomeQueues() {
    messageQueues.showMemoryInfo();
    messageQueues.exchangeIncomeQueues();
    LOG.info("[Communicator] has sent " + this.sendMessageCounter
        + " messages totally.");
    LOG.info("[Communicator] has outgo " + this.outgoMessageCounter
        + " messages totally.");
    this.sendMessageCounter = 0;
    this.outgoMessageCounter = 0;
    // //add by chen
    this.outgoMessageBytesCounter = 0;
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
  public void start() {
    int edgeSize = this.graphData.getEdgeSize();
    this.sender = new Sender(this);
    this.sender
        .setCombineThreshold((int) (edgeSize * CON_FIR_COMBINE_THRESOLD / CON_SEC_COMBINE_THRESOLD));
    this.sender
        .setSendThreshold((int) (edgeSize * CON_FIR_COMBINE_THRESOLD / CON_SEC_COMBINE_THRESOLD));
    this.sender.setMessagePackSize(MESSAGE_PACK_SIZE);
    this.sender.setMaxProducerNum(MAX_PRODUCER_NUM);
    this.sender.start();
  }
  
  @Override
  public void noMoreMessagesForSending() { // To notify the sender no more
    // messages for sending.
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
  public void noMoreMessagesForReceiving() { // To notify the receiver no more
    // messages for receiving.
    this.receiver.setNoMoreMessagesFlag(true);
  }
  
  @Override
  public boolean isReceivingOver() {
    boolean result = false;
    if (!this.receiver.isAlive()) { // When the receiver is not alive,
      // return true.
      result = true;
    }
    return result;
  }
  
  /** the flag of send combine. */
  public boolean isSendCombineSetFlag() {
    return this.job.isCombinerSetFlag();
  }
  
  /** the flag of receive combiner. */
  public boolean isReceiveCombineSetFlag() {
    return this.job.isReceiveCombinerSetFlag();
  }
  
  /** get combiner class. */
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
  public void begin(int superStepCount) {
    String localhostNameAndPort = this
        .getDstWorkerManagerNameAndPort(this.partitionID);
    String brokerName = localhostNameAndPort.split(":")[0] + "-"
        + this.partitionID;
    this.receiver = new Receiver(this, brokerName);
    this.receiver.setCombineThreshold(COMBINE_THRESOLD);
    this.receiver.start();
    this.sender.begin(superStepCount);
  }
  
  @Override
  public void complete() {
    LOG.info("[ACTIVEMQComm] execute complete ");
    this.sender.complete();
    this.stopRPCSever();
  }
  
  @Override
  public long getReceivedMessageCounter() {
    return receivedMessageCounter;
  }
  
  /** counter of received messages. */
  public void setReceivedMessageCounter(long recievedMessageCounter) {
    this.receivedMessageCounter = recievedMessageCounter;
  }
  
  @Override
  public long getReceivedMessageBytesCounter() {
    return receivedMessageBytesCounter;
  }
  
  /** set method of receivedMessageBytesCounter. */
  public void setReceivedMessageBytesCounter(long receivedMessageBytesCounter) {
    this.receivedMessageBytesCounter = receivedMessageBytesCounter;
  }
  
  @Override
  public long getOutgoMessageBytesCounter() {
    return outgoMessageBytesCounter;
  }
  
  /** set method of outgoMessageBytesCounter. */
  public void setOutgoMessageBytesCounter(long outgoMessageBytesCounter) {
    this.outgoMessageBytesCounter = outgoMessageBytesCounter;
  }
  
  @Override
  public long getOutgoMessageCounter() {
    return outgoMessageCounter;
  }
  
  /** set method of outgoMessageCounter. */
  public void setOutgoMessageCounter(long outgoMessageCounter) {
    this.outgoMessageCounter = outgoMessageCounter;
  }
  
  @Override
  public long getCombineOutgoMessageCounter() {
    return combineOutgoMessageCounter;
  }
  
  /** set method of combineOutgoMessageCounter. */
  public void setCombineOutgoMessageCounter(long combineOutgoMessageCounter) {
    this.combineOutgoMessageCounter = combineOutgoMessageCounter;
  }
  
  @Override
  public long getCombineOutgoMessageBytesCounter() {
    return combineOutgoMessageBytesCounter;
  }
  
  /** set method of combineOutgoMessageBytesCounter. */
  public void setCombineOutgoMessageBytesCounter(
      long combineOutgoMessageBytesCounter) {
    this.combineOutgoMessageBytesCounter = combineOutgoMessageBytesCounter;
  }
  
  @Override
  public long getSendMessageCounter() {
    // TODO Auto-generated method stub
    return this.sendMessageCounter;
  }
  
  @Override
  public void setStaffId(String staffId) {
    // TODO Auto-generated method stub
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
  /*Biyahui added*/
	public void recoveryForFault(
			Map<String, LinkedList<IMessage>> incomedMessages) {
		Iterator<String> it = incomedMessages.keySet().iterator();
		while (it.hasNext()) {
			String vertexID = it.next();
			addMessageForMigrate(vertexID, incomedMessages.get(vertexID));
		}
		messageQueues.exchangeIncomeQueues();
	}
  
  /** add message for the migrate staff. */
  private void addMessageForMigrate(String vertexID, List<IMessage> messages) {
    Iterator<IMessage> it = messages.iterator();
    while (it.hasNext()) {
      messageQueues.incomeAMessage(vertexID, it.next());
    }
  }
  
  // Note Add 2 New Methods 20140313
  @Override
  public void start(String hostname, BSPStaff bspStaff) {
    this.bspStaff = bspStaff;
    startServer(hostname);
    try {
      Thread.currentThread().sleep(5000);
    } catch (InterruptedException e) {
      LOG.error("[Communicator] start excepton: ", e);
      throw new RuntimeException("[Communicator] start excepton:", e);
    }
    start();
  }
  
  /** Note Start The RPC Server Using Reference Of BSPStaff. */
  public void startServer(String hostName) {
    this.bspStaff.startActiveMQBroker(hostName);
  }
  
  /** Note Stop The RPC Using BSPStaff. */
  public void stopRPCSever() {
    this.bspStaff.stopActiveMQBroker();
  }
  
  @Override
  public void preBucketMessages(int bucket, int superstep) {
    // TODO Auto-generated method stub
    
  }
  @Override
  public void preBucketMessagesNew(int bucket, int superstep) {
    // TODO Auto-generated method stub
    
  }

@Override
public ConcurrentLinkedQueue<IMessage> getMigrateMessageQueue(String valueOf) {
	// TODO Auto-generated method stub
	return null;
}
/*Biyahui*/
	public ConcurrentLinkedQueue<IMessage> getRecoveryMessageQueue(String valueOf) {
		return null;
	}

@Override
public HashMap<Integer, String> getpartitionToWorkerManagerNameAndPort() {
	// TODO Auto-generated method stub
	return null;
}

public void setSendMessageCounter(long sendMessageCounter) {
  this.sendMessageCounter = sendMessageCounter;
}

public int getEdgeSize() {
  return edgeSize;
}

public void setEdgeSize(int edgeSize) {
  this.edgeSize = edgeSize;
}

public HashMap<Integer, String> getPartitionToWorkerManagerNameAndPort() {
  return partitionToWorkerManagerNameAndPort;
}

public void setPartitionToWorkerManagerNameAndPort(
    HashMap<Integer, String> partitionToWorkerManagerNameAndPort) {
  this.partitionToWorkerManagerNameAndPort = partitionToWorkerManagerNameAndPort;
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
