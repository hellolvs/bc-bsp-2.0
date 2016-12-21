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
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.comm.MetaDataOfMessage.BufferStatus;
import com.chinamobile.bcbsp.comm.io.util.WritableBSPMessages;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.router.route;
import com.chinamobile.bcbsp.router.routeparameter;
import com.chinamobile.bcbsp.rpc.RPCCommunicationProtocol;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

/**
 * A New Communication Manager That Can Match New Communication Mode And New
 * Message Organization. Do Not Support Balance Hash Partititon Now.
 */
public class CommunicatorNew implements CommunicatorInterface,
    RPCCommunicationProtocol {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(CommunicatorNew.class);
  /** default message sender buffer threshold. */
  private static final int MESSAGE_SEND_BUFFER_THRESHOLD = 5000;
  /** default message received buffer threshold. */
  private static final int MESSAGE_RECEIVED_BUFFER_THRESHOLD = 50000;
  /** BSP Job ID. */
  private BSPJobID jobID = null;
  /** BSP Job. */
  private BSPJob job = null;
  /** partition ID. */
  private int partitionID = 0;
  /** Graph data interface. */
  private GraphDataInterface graphData = null;
  /** route parameter of Communicator. */
  private routeparameter routeparameter = null;
  /** route table. */
  private route route = null;
  /** Route Table: PartitionID---WorkerManagerNameAndPort(HostName:Port). */
  private HashMap<Integer, String> partitionToWorkerManagerNameAndPort = null;
  /** RPC Handlers Manager. */
  private HashMap<Integer, RPCCommunicationProtocol> rpcCommHandlers = null;
  /** Message Manager Interface. */
  private MessageManagerInterface messageManager = null;
  /** current superstep counter. */
  private int superstepCounter;
  /** Send Message Using Thread Service Of JDK. */
  private ExecutorService sendMssgPool = Executors
      .newFixedThreadPool(MetaDataOfMessage.DEFAULT_MESSAGE_THREAD_BASE);
  /** Message send result. */
  private HashMap<Integer, Future<Boolean>> sendMssgResult = new HashMap<Integer, Future<Boolean>>();
  /** handle of BSP staff. */
  private BSPStaff bspStaff;
  /** the partition of message. */
  private Partitioner<Text> partitioner;
  /** singleton model of message. */
  private ConcurrentLinkedQueue<IMessage> singletonMsg = new ConcurrentLinkedQueue<IMessage>();
  /** map of migrate messages */
  private HashMap<String, LinkedList<IMessage>> incomedMigrateMessages = new HashMap<String, LinkedList<IMessage>>();
  /** map of recovery messages*/
  private HashMap<String, LinkedList<IMessage>> incomedRecoveryMessages = new HashMap<String, LinkedList<IMessage>>();
  private ConcurrentLinkedQueue<IMessage> migrateMessages = new ConcurrentLinkedQueue<IMessage>();
  private ConcurrentLinkedQueue<IMessage> recoveryMessages = new ConcurrentLinkedQueue<IMessage>();
  /** Staff ID of Job (for test). */
  private String staffId;
  /** Outgoing queue size */
  private int outoingQueueSize = 0;
  
  /**
   * Constructor of new version Communicator.
   */
  public CommunicatorNew(BSPJobID jobID, BSPJob job, int partitionID,
      Partitioner<Text> partitioner) {
    this.jobID = jobID;
    this.job = job;
    this.partitionID = partitionID;
    MetaDataOfMessage.SENDMSGCOUNTER = 0;
    MetaDataOfMessage.RECEIVEMSGCOUNTER = 0;
    MetaDataOfMessage.NETWORKEDMSGCOUNTER = 0;
    MetaDataOfMessage.NETWORKEDMSGBYTECOUNTER = 0;
    MetaDataOfMessage.RECEIVEMSGBYTESCOUNTER = 0;
    // The important section for message structure
    MetaDataOfMessage.PARTITION_ID = partitionID;
    MetaDataOfMessage.HASH_NUMBER = job.getHashBucketNumber();
    MetaDataOfMessage.PARTITION_NUMBER = job.getNumBspStaff();
    MetaDataOfMessage.PARTITIONBUCKET_NUMBER = MetaDataOfMessage.HASH_NUMBER
        * MetaDataOfMessage.PARTITION_NUMBER;
    // Note: CommunicatorNew Class Is Just Ready For MessageQueueNew Now
    // this.messageManager = new MessageQueuesNew(job, partitionID);
    this.messageManager = new MessageQueuesNewByteArray(job, partitionID);
    // Note Temporary
    this.partitioner = partitioner;
  }
  
  /**
   * Namely Put The Specified Messages Into Message Buffer For Next Local
   * Computing.
   */
  public class SendLocalMessageThread implements Callable<Boolean> {
    /** the partition ID of source vertex. */
    private int srcPartitionId;
    /** the partition ID of destination vertex. */
    private int dstBucketId;
    /** current superstep counter. */
    private int superStepCounter;
    /** interface of BSP message. */
    private WritableBSPMessages localSendMessage;
    
    /**
     * Constructor of SendLocalMessageThread.
     * 
     * @param srcPartitionId
     * @param dstcucketId
     * @param superStepCounter
     * @param localSendMessage
     */
    public SendLocalMessageThread(int srcPartitionId, int dstcucketId,
        int superStepCounter, WritableBSPMessages localSendMessage) {
      this.srcPartitionId = srcPartitionId;
      this.superStepCounter = superStepCounter;
      this.localSendMessage = localSendMessage;
      this.dstBucketId = dstcucketId;
    }
    
    /** Call method of Thread. */
    public Boolean call() {
      int sPartitionDBucket = PartitionRule.getSrcPartitionBucketId(
          this.srcPartitionId, this.dstBucketId);
      messageManager.incomeAMessage(sPartitionDBucket, this.localSendMessage,
          this.superStepCounter);
      return true;
    }
  }
  
  /**
   * SendRemoteMessageThread. Send messages to the destination partition on the
   * remote machine by RPC Server.
   * 
   * @author WangZhigang
   * @version 0.1
   */
  public class SendRemoteMessageThread implements Callable<Boolean> {
    /** the handler of RPC communicator protocol. */
    private RPCCommunicationProtocol handler;
    /** the partition ID of source vertex. */
    private int srcPartitionId;
    /** the partition ID of destination vertex. */
    private int dstBucketId;
    /** current superstep counter. */
    private int superStepCounter;
    /** interface of BSP message. */
    private WritableBSPMessages networkedMessage;
    
    /**
     * Constructor of SendRemoteMessageThread.
     * 
     * @param srcPartitionId
     * @param dstcucketId
     * @param superStepCounter
     * @param localSendMessage
     */
    public SendRemoteMessageThread(int srcPartitionId,
        RPCCommunicationProtocol RPCHandler, int dstBucketId,
        int superStepCounter, WritableBSPMessages networkeredMessage) {
      this.handler = RPCHandler;
      this.srcPartitionId = srcPartitionId;
      this.dstBucketId = dstBucketId;
      this.superStepCounter = superStepCounter;
      this.networkedMessage = networkeredMessage;
    }
    
    /**
     * Call method of Thread.
     * 
     * @return boolean
     */
    public Boolean call() {
      sendPacked();
      return true;
    }
    
    /** Send the messsages as package. */
    public void sendPacked() {
      try {
        this.handler.sendPackedMessage(this.networkedMessage,
            this.srcPartitionId, this.dstBucketId, this.superStepCounter);
      } catch (Exception e) {
        throw new RuntimeException("<SendRemoteMessageThread>", e);
      }
      this.networkedMessage.clearBSPMsgs();
    }
  }
  
  @Override
  public void initialize(routeparameter routeparameter,
      HashMap<Integer, String> aPartitionToWorkerManagerNameAndPort,
      GraphDataInterface aGraphData) {
    this.rpcCommHandlers = new HashMap<Integer, RPCCommunicationProtocol>();
    this.partitionToWorkerManagerNameAndPort = aPartitionToWorkerManagerNameAndPort;
    this.graphData = aGraphData;
  }
  
  /**
   * Get the dstPartitionID from the vertexID.
   * 
   * @param vertexID
   * @return dstPartitionID
   */
  @SuppressWarnings("unused")
  private int getDstPartitionID(int vertexID) {
    return -1;
  }
  
  /**
   * Get the dstWorkerManagerName from the dstPartitionID.
   * 
   * @param dstPartitionID
   * @return dstWorkerManagerName
   */
  private String getDstWorkerManagerNameAndPort(int dstPartitionID) {
    String dstWorkerManagerNameAndPort = null;
    dstWorkerManagerNameAndPort = this.partitionToWorkerManagerNameAndPort
        .get(dstPartitionID);
    return dstWorkerManagerNameAndPort;
  }
  
  // Core Method Or API Which Is Used For Message Passing.
  @Override
  public void send(IMessage msg) throws IOException {
    String vertexID = msg.getDstVertexID();
    int globalPartition = this.partitioner.getPartitionID(new Text(vertexID));
    int bucket = PartitionRule.localPartitionRule(vertexID);
    msg.setDstPartition(globalPartition);
    // The destination partition is just in this staff.
    int dstPartitionBucket = PartitionRule.getDestPartitionBucketId(
        globalPartition, bucket);
    switch (messageManager.outgoAMessage(dstPartitionBucket, msg)) {
    case MetaDataOfMessage.BufferStatus.NORMAL:
    break;
    case MetaDataOfMessage.BufferStatus.SPILL:
      Future<Boolean> result = sendMssgResult.get(dstPartitionBucket);
      if (result != null && !result.isDone()) {
        try {
          result.get();
        } catch (Exception e) {
          throw new RuntimeException("<sendMessage>--<SendThread>", e);
        }
      }
      startSendMessageThread(dstPartitionBucket, this.superstepCounter);
    break;
    default:
      LOG.error("<Controll Message Sending Error>" + "<SuperStep>"
          + "<DestPartition>" + "<Bucket>");
    }
    MetaDataOfMessage.SENDMSGCOUNTER++;
  }
  
  /**
   * Generate And Start A Thread To Send The Messages. Then Add The Specified
   * Thread Into sendMssgPool.
   */
  private void startSendMessageThread(int partitionBucketId,
      int superStepCounter) {
    int dstPartition = PartitionRule.getPartition(partitionBucketId);
    int dstBucket = PartitionRule.getBucket(partitionBucketId);
    WritableBSPMessages writableMessage = this.messageManager
        .removeOutgoingQueue(partitionBucketId);
    Future<Boolean> future;
    if (this.partitionID == dstPartition) {
      // Local send messages: write to local file system straightly.
      future = this.sendMssgPool.submit(new SendLocalMessageThread(
          this.partitionID, dstBucket, superStepCounter, writableMessage));
    } else {
      // Remote send messages: send messages to remote machine by RPC.
      int count = writableMessage.getMsgCount();
      int size = writableMessage.getMsgSize();
      future = this.sendMssgPool.submit(new SendRemoteMessageThread(
          this.partitionID, getRPCHandler(partitionBucketId), dstBucket,
          superStepCounter, writableMessage));
      MetaDataOfMessage.NETWORKEDMSGCOUNTER += count;
      MetaDataOfMessage.NETWORKEDMSGBYTECOUNTER += size;
    }
    this.sendMssgResult.put(partitionBucketId, future);
  }
  
  /**
   * Get Message Sending RPC Handler For Communication.
   * 
   * @param partitionBucket
   * @return RPCCommunicationProtocol
   */
  private RPCCommunicationProtocol getRPCHandler(int partitionBucket) {
    if (this.rpcCommHandlers.get(partitionBucket) != null) {
      return this.rpcCommHandlers.get(partitionBucket);
    }
    int partition = PartitionRule.getPartition(partitionBucket);
    String addrPort = getDstWorkerManagerNameAndPort(partition);// get the dst
                                                                // worker port
                                                                // depend on
                                                                // partion
    String[] tmp = addrPort.split(":");
    String hostname = tmp[0];
    int portNum = Integer.parseInt(tmp[1]);
    try {
      RPCCommunicationProtocol rPCHandler = (RPCCommunicationProtocol) RPC
          .getProxy(RPCCommunicationProtocol.class,
              RPCCommunicationProtocol.protocolVersion, new InetSocketAddress(
                  hostname, portNum), new Configuration());
      // LOG.info("Test")
      this.rpcCommHandlers.put(partitionBucket, rPCHandler);
      return rPCHandler;
    } catch (Exception e) {
      LOG.error("<Request For RPC Handler Error> For " + " <Partition> " + "("
          + partitionBucket + ")");
      throw new RuntimeException("<Request For RPC Handler Error> For "
          + " <Partition> " + "(" + partitionBucket + ")", e);
      
    }
    // return null;
  }
  
  /** Initialize the RPC protocol handle. */
  private void initRPCHandler() throws IOException {
    Iterator<Integer> keyItr = this.partitionToWorkerManagerNameAndPort
        .keySet().iterator();
    int partitionId = 0;
    while (keyItr.hasNext()) {
      partitionId = keyItr.next();
      String addrPort = getDstWorkerManagerNameAndPort(partitionId);
      String[] tmp = addrPort.split(":");
      String hostname = tmp[0];
      int portNum = Integer.parseInt(tmp[1]);
      for (int i = 0; i < MetaDataOfMessage.HASH_NUMBER; i++) {
        RPCCommunicationProtocol rPCHandler = (RPCCommunicationProtocol) RPC
            .waitForProxy(RPCCommunicationProtocol.class,
                RPCCommunicationProtocol.protocolVersion,
                new InetSocketAddress(hostname, portNum), new Configuration());
        this.rpcCommHandlers.put(PartitionRule.getDestPartitionBucketId(
            partitionId, i), rPCHandler);
      }
    }
  }
  
  @Override
  public void sendToAllEdges(IMessage msg) throws IOException {
    // TODO Auto-generated method stub
  }
  
  @Override
  public Iterator<IMessage> getMessageIterator(String vertexID)
      throws IOException {
    ArrayList<IMessage> incomedQueue = messageManager
        .removeIncomedQueue(vertexID);
    if (incomedQueue == null) {
      LOG.info("Vertex message is empty!");
      return null;
    }
    Iterator<IMessage> iterator = incomedQueue.iterator();
    return iterator;
  }
  
  @Override
  public ConcurrentLinkedQueue<IMessage> getMessageQueue(String vertexID)
      throws IOException {
    ArrayList<IMessage> tmpList = messageManager.removeIncomedQueue(vertexID);
    // Note Singleton
    if (tmpList == null) {
      return singletonMsg;
    }
    ConcurrentLinkedQueue<IMessage> tmpQueue = new ConcurrentLinkedQueue<IMessage>();
    while (!tmpList.isEmpty()) {
      tmpQueue.add(tmpList.remove(0));
    }
    tmpList = null;
    return tmpQueue;
  }
  
  @Override
  public void start(String hostname, BSPStaff bspStaff) {
    int edgeSize = this.graphData.getEdgeSize();
    this.bspStaff = bspStaff;
    startServer(hostname);
    // Note Initialize Some Controlling Parameters.
    MetaDataOfMessage.MESSAGE_SEND_BUFFER_THRESHOLD = MESSAGE_SEND_BUFFER_THRESHOLD;
    // Note Control Whether And When To Used Disk Storage.
    MetaDataOfMessage.MESSAGE_RECEIVED_BUFFER_THRESHOLD = MESSAGE_RECEIVED_BUFFER_THRESHOLD;
  }
  
  // Note Do This Action Before Each Super Step. Like Pre-SuperStep
  @Override
  public void begin(int superStepCount) {
    this.superstepCounter = superStepCount;
  }
  
  @Override
  public void complete() {
    close();
    LOG.info("[RPCComm] execute complete ");
  }
  
  // Note Release The Resources
  public void close() {
    stopRPCSever();
    this.rpcCommHandlers.clear();
    this.sendMssgPool.shutdownNow();
    LOG.info("stop thread pool sendMssgPool");
  }
  
  // Note All The Messages In The Send Buffer Should Be Send Over.
  @Override
  public void noMoreMessagesForSending() {
    sendRemainMessages(this.superstepCounter);
  }
  
  public void sendRemainMessages(int superStepCounter) {
    InetSocketAddress dstAddress;
    int dstPartitionBucketId = 0;
    clearSendMssgPool();
    for (int dstPartitionId = 0; dstPartitionId < MetaDataOfMessage.PARTITION_NUMBER; dstPartitionId++) {
      for (int bucketId = 0; bucketId < MetaDataOfMessage.HASH_NUMBER; bucketId++) {
        dstPartitionBucketId = PartitionRule.getDestPartitionBucketId(
            dstPartitionId, bucketId);
        // LOG.info("<sendRemainMessages>--<CheckLength>--<PartitionId>" +
        // dstPartitionId + " <BucketId>" + bucketId);
        if (this.messageManager.getOutgoingQueueSize(dstPartitionBucketId) > 0) {
          startSendMessageThread(dstPartitionBucketId, superStepCounter);
        }
      }
    }
    clearSendMssgPool();
    MetaDataOfMessage.clearSMBLength();
  }
  
  /** Clear the sender message thread pool. */
  private void clearSendMssgPool() {
    try {
      // Iterator it = this.sendMssgResult.keySet().iterator();
      // while(it.hasNext()){
      // LOG.info("clearSendMssgPool Test! "+it.toString());
      // }
      for (Future<Boolean> ele : this.sendMssgResult.values()) {
        if (!ele.isDone()) {
          ele.get();
        }
      }
      this.sendMssgResult.clear();
    } catch (Exception e) {
      LOG.error("<clearSendMessage1>" + e);
      throw new RuntimeException("<clearSendMessage1>", e);
    }
  }
  
  // Note With Using Synchronize Send Control, Remained Messages Should be Send
  // Over Here.
  @Override
  public boolean isSendingOver() {
    return true;
  }
  
  // Note
  @Override
  public void noMoreMessagesForReceiving() {
  }
  
  @Override
  public boolean isReceivingOver() {
    return true;
  }
  
  // Note Between Two SuperStep, Some Information And Structure Should Be
  // Changed Or Updated
  @Override
  public void exchangeIncomeQueues() {
    messageManager.showMemoryInfo();
    // Note The Main Operation Is In The MessageManager Between SuperStep. New
    // Messages Should Be Processed Next SuperStep.
    messageManager.exchangeIncomeQueues();
    LOG.info("[Communicator] has generated " + MetaDataOfMessage.SENDMSGCOUNTER
        + " messages totally.");
    LOG.info("[Communicator] has networked "
        + MetaDataOfMessage.NETWORKEDMSGCOUNTER + " messages totally.");
    setOutgoingQueuesSize(MetaDataOfMessage.SENDMSGCOUNTER + MetaDataOfMessage.NETWORKEDMSGCOUNTER);
    MetaDataOfMessage.SENDMSGCOUNTER = 0;
    MetaDataOfMessage.NETWORKEDMSGCOUNTER = 0;
    MetaDataOfMessage.RECEIVEMSGCOUNTER = 0;
    MetaDataOfMessage.NETWORKEDMSGBYTECOUNTER = 0;
    MetaDataOfMessage.RECEIVEMSGBYTESCOUNTER = 0;
    // Note Add By Liu Jinpeng At 2014-01-19 Just For Testing.
    // System.gc();
  }
  
  private void setOutgoingQueuesSize(long size) {
	  outoingQueueSize = (int) size;
}

@Override
  public int getOutgoingQueuesSize() {
    return  outoingQueueSize;
  }
  
  @Override
  public int getIncomingQueuesSize() {
    return 0;
  }
  
  // Note Return Value Is Some Statistics
  @Override
  public int getIncomedQueuesSize() {
    return messageManager.getIncomedQueuesSize();
  }
  
  @Override
  public void clearAllQueues() {
  }
  
  @Override
  public void clearOutgoingQueues() {
    messageManager.clearOutgoingQueues();
	setOutgoingQueuesSize(0);

  }
  
  @Override
  public void setPartitionToWorkerManagerNamePort(HashMap<Integer, String> value) {
    this.partitionToWorkerManagerNameAndPort = value;
  }
  
  // Note None-Used
  @Override
  public void clearIncomingQueues() {
    messageManager.clearIncomingQueues();
  }
  
  @Override
  public void clearIncomedQueues() {
    messageManager.clearIncomedQueues();
  }
  
  /**
   * Get the flag of sender combiner.
   * 
   * @return boolean
   */
  public boolean isSendCombineSetFlag() {
    return this.job.isCombinerSetFlag();
  }
  
  /**
   * Get the flag of receiver combiner.
   * 
   * @return boolean
   */
  public boolean isReceiveCombineSetFlag() {
    return this.job.isReceiveCombinerSetFlag();
  }
  
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return this.protocolVersion;
  }
  
  @Override
  public int sendUnpackedMessage(IMessage messages) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public int sendPackedMessage(WritableBSPMessages packedMessages,
      int srcPartition, int dstBucket, int superstep) {
    int srcPartitionDstBucket = PartitionRule.getSrcPartitionBucketId(
        srcPartition, dstBucket);
    // Note Error Ever . Force Cast WritableMessages To BSPMessagePack
    this.messageManager.incomeAMessage(srcPartitionDstBucket, packedMessages,
        superstep);
    MetaDataOfMessage.RECEIVEMSGCOUNTER += packedMessages.getMsgCount();
    MetaDataOfMessage.RECEIVEMSGBYTESCOUNTER += packedMessages.getMsgSize();
    return 0;
  }
  
  // Note Start The RPC Server Using Reference Of BSPStaff
  public void startServer(String hostName) {
    this.bspStaff.startRPCServer(hostName);
  }
  
  // Note Stop The RPC Using BSPStaff
  public void stopRPCSever() {
    this.bspStaff.stopRPCSever();
  }
  
  @Override
  public void preBucketMessages(int bucket, int superstep) {
    this.messageManager.loadBucketMessage(bucket, superstep);
  }
  /*Biyahui added*/
  @Override
  public void preBucketMessagesNew(int bucket, int superstep) {
    this.messageManager.loadBucketMessageNew(bucket, superstep);
  }
  
  // Note For Communication Statistics.Used by Chen Changning.
  @Override
  public long getReceivedMessageCounter() {
    return MetaDataOfMessage.RECEIVEMSGCOUNTER;
  }
  
  @Override
  public long getReceivedMessageBytesCounter() {
    return MetaDataOfMessage.RECEIVEMSGBYTESCOUNTER;
  }
  
  @Override
  public long getCombineOutgoMessageCounter() {
    return MetaDataOfMessage.NETWORKEDMSGCOUNTER;
  }
  
  @Override
  public long getCombineOutgoMessageBytesCounter() {
    return MetaDataOfMessage.NETWORKEDMSGBYTECOUNTER;
  }
  
  // None-Used
  @Override
  public int sendPackedMessage(BSPMessagesPack packedMessages) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public int sendPackedMessage(BSPMessagesPack packedMessages, String str) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public void initialize(routeparameter routeparameter,
      HashMap<Integer, String> aPartitionToWorkerManagerNameAndPort,
      int edgeSize) {
    // TODO Auto-generated method stub
  }
  
  @Override
  public void start() {
    // TODO Auto-generated method stub
  }
  
  @Override
  public long getOutgoMessageCounter() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public long getOutgoMessageBytesCounter() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public long getSendMessageCounter() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public void setStaffId(String staffId) {
    // TODO Auto-generated method stub
    this.staffId = staffId;// set communicator staffID
  }
  
  @Override
  public IMessage checkAMessage() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void recoveryForMigrate(
      Map<String, LinkedList<IMessage>> incomedMessages) {
    // TODO Auto-generated method stub
    // Iterator<String> it = incomedMessages.keySet().iterator();
    // while (it.hasNext()) {
    // String vertexID = it.next();
    // addMessageForMigrate(vertexID, incomedMessages.get(vertexID));
    // }
    // messageQueues.exchangeIncomeQueues();
    if (this.incomedMigrateMessages != null) {
      this.incomedMigrateMessages.clear();
    }
    this.incomedMigrateMessages = (HashMap<String, LinkedList<IMessage>>) incomedMessages;
  }
  /*Biyahui added*/
	public void recoveryForFault(
			Map<String, LinkedList<IMessage>> incomedMessages) {
		if (this.incomedRecoveryMessages != null) {
			this.incomedRecoveryMessages.clear();
		}
		this.incomedRecoveryMessages = (HashMap<String, LinkedList<IMessage>>) incomedMessages;
	}
  
  public void setBspStaff(BSPStaff bspStaff) {
    this.bspStaff = bspStaff;
  }
  
  // Note: getter and setter of some local member variable
  /**
   * Set BSP job ID.
   * 
   * @param size
   */
  public void setBSPJobID(BSPJobID jobID) {
    this.jobID = jobID;
  }
  
  /**
   * Get BSP job ID.
   * 
   * @return BSPJobID
   */
  public BSPJobID getBSPJobID() {
    return this.jobID;
  }
  
  /**
   * Get BSP job.
   * 
   * @return BSPJob
   */
  public BSPJob getJob() {
    return job;
  }
  
  /**
   * Set BSP job.
   * 
   * @param job
   */
  public void setJob(BSPJob job) {
    this.job = job;
  }
  
  /**
   * Set partitionID.
   * 
   * @param partitionID
   */
  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }
  
  /**
   * Get partitionID.
   * 
   * @return int
   */
  public int getPartitionID() {
    return this.partitionID;
  }
  
  /**
   * Get messageManager.
   * 
   * @return MessageManagerInterface
   */
  public MessageManagerInterface getMessageQueues() {
    return this.messageManager;
  }
  
  @Override
  public ConcurrentLinkedQueue<IMessage> getMigrateMessageQueue(String valueOf) {
    // TODO Auto-generated method stub
    this.migrateMessages.clear();
    // Iterator<String> it = incomedMessages.keySet().iterator();
    // while (it.hasNext()) {
    // String vertexID = it.next();
    // addMessageForMigrate(vertexID, incomedMessages.get(vertexID));
    // }
    /*Biyahui revised*/
    Iterator<IMessage> msg=null;
    if(this.incomedMigrateMessages.get(valueOf)==null){
    	return singletonMsg;
    }else{
    	msg=this.incomedMigrateMessages.get(valueOf).iterator();
    }
    //Iterator<IMessage> msg = this.incomedMigrateMessages.get(valueOf)
        //.iterator();
    //end revised
    while (msg.hasNext()) {
      this.migrateMessages.add(msg.next());
    }
    return this.migrateMessages;// certain vertex message queue
  }
  /*Biyahui added*/
	public ConcurrentLinkedQueue<IMessage> getRecoveryMessageQueue(String valueOf) {
		this.recoveryMessages.clear();
		Iterator<IMessage> msg = null;
		if (this.incomedRecoveryMessages.get(valueOf) == null) {
			return singletonMsg;
		} else {
			msg = this.incomedRecoveryMessages.get(valueOf).iterator();
		}
		while (msg.hasNext()) {
			this.recoveryMessages.add(msg.next());
		}
		return this.recoveryMessages;
	}
  
  @Override
  public HashMap<Integer, String> getpartitionToWorkerManagerNameAndPort() {
    // TODO Auto-generated method stub
    return this.partitionToWorkerManagerNameAndPort;
  }
  
  /** For JUnit test.*/
  public GraphDataInterface getGraphData() {
    return graphData;
  }
  
  public void setGraphData(GraphDataInterface graphData) {
    this.graphData = graphData;
  }
  
  public void sendPartition(IMessage msg) throws IOException {
    int dstPartitonID = Integer.parseInt(msg.getDstVertexID());
    msg.setDstPartition(dstPartitonID);
    int bucketID = Constants.PEER_COMPUTE_BUCK_ID;
    msg.setMessageId(Constants.DEFAULT_PEER_DST_MESSSAGE_ID);
    // The destination partition is just in this staff.
    switch (messageManager.outgoAMessage(Constants.PEER_COMPUTE_BUCK_ID, msg)) {
    case MetaDataOfMessage.BufferStatus.NORMAL:
    break;
    case MetaDataOfMessage.BufferStatus.SPILL:
      Future<Boolean> result = sendMssgResult.get(bucketID);
      if (result != null && !result.isDone()) {
        try {
          result.get();
        } catch (Exception e) {
          throw new RuntimeException("<sendMessage>--<SendThread>", e);
        }
      }
      startSendMessageThread(bucketID, this.superstepCounter);
    break;
    default:
      LOG.error("<Controll Message Sending Error>" + "<SuperStep>"
          + "<DestPartition>" + "<Bucket>");
    }
    MetaDataOfMessage.SENDMSGCOUNTER++;
  }
}
