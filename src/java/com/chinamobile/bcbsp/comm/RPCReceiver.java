
package com.chinamobile.bcbsp.comm;

import com.chinamobile.bcbsp.api.Combiner;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A receiver with Hadoop RPC belongs to a communicator, for receiving messages
 * and put them into the incoming queues.
 */
public class RPCReceiver {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(RPCReceiver.class);
  /** The RPC communication tool for BSP. */
  private RPCCommunicator rpcComm = null;
  /** message Queues. */
  private MessageQueuesInterface messageQueues = null;
  /** flag of combiner. */
  private boolean combinerFlag = false;
  /** combiner of receiver. */
  private Combiner combiner = null;
  /** combine threshold default is 3. */
  private int combineThreshold = 3;
  /** message counter. */
  private Long messageCount = 0L;
  /** number of received messages. */
  private Long messagesReceived = 0L;
  /** message byte counter. */
  private Long messageBytesCount = 0L;
  /** superstep counter of job. */
  private int superStepCounter = 0;
  /** thread mutex single. */
  private Integer mutex = 0;
  
  /**
   * Constructor of RPCReceiver.
   * @param rpcComm
   */
  public RPCReceiver(RPCCommunicator rpcComm) {
    LOG.info("[Constructor]-RPCReceiver constructed!");
    this.rpcComm = rpcComm;
    this.messageQueues = this.rpcComm.getMessageQueues();
    this.combinerFlag = this.rpcComm.isReceiveCombineSetFlag();
    // Get the combiner from the communicator.
    if (combinerFlag) {
      this.combiner = this.rpcComm.getCombiner();
      this.combineThreshold = this.rpcComm.getJob()
          .getReceiveCombineThreshold();
    }
  }
  
  /**
   * Receive the package of messages.
   * @param packedMessages
   * @return int
   */
  public int receivePackedMessage(BSPMessagesPack packedMessages) {
    synchronized (mutex) {
      messagesReceived++;
      Iterator<IMessage> iter = packedMessages.getPack().iterator();
      IMessage bspMsg = null;
      while (iter.hasNext()) {
        bspMsg = iter.next();
        this.messageCount++;
        this.messageBytesCount += bspMsg.size();
        String vertexID = bspMsg.getDstVertexID();
        this.messageQueues.incomeAMessage(vertexID, bspMsg);
      }
    }
    int maxSize = 0;
    String incomingIndex = null;
    ConcurrentLinkedQueue<IMessage> maxQueue = null;
    // combiner
    if (combinerFlag) {
      maxSize = 0;
      maxQueue = null;
      incomingIndex = this.messageQueues.getMaxIncomingQueueIndex();
      if (incomingIndex != null) {
        maxSize = this.messageQueues.getIncomingQueueSize(incomingIndex);
      }
      // When the longest queue's length reaches the threshold
      if (maxSize >= combineThreshold) {
        // Get the queue out of the map.
        maxQueue = this.messageQueues.removeIncomingQueue(incomingIndex);
        // Combine the max queue into just one message.
        IMessage message = combine(maxQueue);
        this.messageCount++;
        this.messageQueues.incomeAMessage(incomingIndex, message);
      }
    }
    return 0;
  }
  
  /**
   * Receive the package of messages for test.
   * @param packedMessages
   * @param str
   * @return int
   */
  public int receivePackedMessage(BSPMessagesPack packedMessages, String str) {
    synchronized (mutex) {
      messagesReceived++;
      Iterator<IMessage> iter = packedMessages.getPack().iterator();
      IMessage bspMsg = null;
      while (iter.hasNext()) {
        bspMsg = iter.next();
        this.messageCount++;
        this.messageBytesCount += bspMsg.size();
        String vertexID = bspMsg.getDstVertexID();
        this.messageQueues.incomeAMessage(vertexID, bspMsg);
        LOG.info("incomingqueue size is :"
            + this.messageQueues.getIncomingQueuesSize()
            + " this.messageCount=" + this.messageCount);
      }
    }
    int maxSize = 0;
    String incomingIndex = null;
    ConcurrentLinkedQueue<IMessage> maxQueue = null;
    // combiner
    if (combinerFlag) {
      maxSize = 0;
      maxQueue = null;
      incomingIndex = this.messageQueues.getMaxIncomingQueueIndex();
      if (incomingIndex != null) {
        maxSize = this.messageQueues.getIncomingQueueSize(incomingIndex);
      }
      // When the longest queue's length reaches the threshold
      if (maxSize >= combineThreshold) {
        // Get the queue out of the map.
        maxQueue = this.messageQueues.removeIncomingQueue(incomingIndex);
        // Combine the max queue into just one message.
        IMessage message = combine(maxQueue);
        this.messageCount++;
        this.messageQueues.incomeAMessage(incomingIndex, message);
      }
    }
    return 0;
  }
  
  /**
   * Combine the incoming message queue.
   * @param incomingQueue
   * @return IMessage
   */
  private IMessage combine(ConcurrentLinkedQueue<IMessage> incomingQueue) {
    IMessage msg = (IMessage) this.combiner.combine(incomingQueue.iterator());
    return msg;
  }
  
  /**Set method of combineThreshold.*/
  public void setCombineThreshold(int aCombineThreshold) {
    if (this.combineThreshold == 0) {
      this.combineThreshold = aCombineThreshold;
    }
  }
  
  /**Reset the all counters of messages.*/
  public void resetCounters() {
    this.messageCount = 0L;
    this.messageBytesCount = 0L;
    this.messagesReceived = 0L;
  }
  
  /**Set method of the superStep counter. */
  public void setSuperStepCounter(int superStepCount) {
    this.superStepCounter = superStepCount;
  }
  
  /**Show the information of message queues.*/
  public void recordMsgNum() {
    LOG.info("[Messages received] in superStep " + this.superStepCounter
        + this.messageQueues.getIncomingQueuesSize());
    LOG.info("[Messages received] in superStep " + this.superStepCounter
        + "receiver gets " + this.messageCount + " Messages and bytes is "
        + this.messageBytesCount);
  }
  
  /**
   * Get method of messageCount.
   * @return long
   */
  public Long getMessageCount() {
    return messageCount;
  }
  
  /**
   * Get method of messageBytesCount.
   * @return long
   */
  public Long getMessageBytesCount() {
    return messageBytesCount;
  }
  
  /**
   * Get the flag of receive over.
   * @return long
   */
  public boolean isReceiveOver() {
    return true;
  }
}
