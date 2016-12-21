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

import com.chinamobile.bcbsp.api.Combiner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A receiver belongs to a communicator, for receiving messages and put them
 * into the incoming queues.
 */
public class Receiver extends Thread {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(Receiver.class);
  /** The communication tool for BSP. */
  private Communicator communicator = null;
  /** brokerName for this staff to receive messages. */
  private String brokerName;
  /** message Queues. */
  private MessageQueuesInterface messageQueues = null;
  /** the number of consumer. */
  private int consumerNum = 1;
  /** List of consumers. */
  private ArrayList<ConsumerTool> consumers = null;
  /** flag of combiner. */
  private boolean combinerFlag = false;
  /** combiner of receiver. */
  private Combiner combiner = null;
  /** combine threshold default is 3. */
  private int combineThreshold = 3;
  /** flag of no more messages. */
  private boolean noMoreMessagesFlag = false;
  /** message counter. */
  private Long messageCount = 0L;
  /** message bytes counter. */
  // add by chen
  private Long messageBytesCount = 0L;
  
  /**
   * Constructor of Receiver.
   * @param comm
   * @param brokerName
   */
  public Receiver(Communicator comm, String brokerName) {
    this.communicator = comm;
    this.brokerName = brokerName;
    this.combinerFlag = this.communicator.isReceiveCombineSetFlag();
    // Get the combiner from the communicator.
    if (combinerFlag) {
      this.combiner = this.communicator.getCombiner();
      this.combineThreshold = this.communicator.getJob()
          .getReceiveCombineThreshold();
    }
    int usrProducerNum = this.communicator.getJob().getMaxProducerNum();
    int usrConsumerNum = this.communicator.getJob().getMaxConsumerNum();
    if (usrConsumerNum <= usrProducerNum) {
      this.consumerNum = usrConsumerNum;
    }
    this.consumers = new ArrayList<ConsumerTool>();
  }
  
  /**Message counter add 1.*/
  public void addMessageCount(long count) {
    synchronized (this.messageCount) {
      this.messageCount += count;
    }
  }
  
  /**Add message bytes counter.*/
  public void addMessageBytesCount(Long messageBytesCount) {
    this.messageBytesCount += messageBytesCount;
  }
  
  /**Run method of Thread.*/
  public void run() {
    try {
      LOG.info("[Receiver] starts successfully!");
      LOG.info("========== Initialize Receiver ==========");
      if (this.combinerFlag) {
        LOG.info("[Combine Threshold for receiving]= " + this.combineThreshold);
      } else {
        LOG.info("No combiner or combiner turned off for receiving!!!");
      }
      LOG.info("[Consumer Number] = " + this.consumerNum);
      LOG.info("=========================================");
      this.messageQueues = this.communicator.getMessageQueues();
      ConsumerTool consumer = null;
      // 采用这种并发线程不一定真的有效，因为从消息池中取消息所互斥的，然后各个线程放入队列也是互斥，合并操作也是对消息队列
      for (int i = 0; i < this.consumerNum; i++) {
        consumer = new ConsumerTool(this, this.messageQueues, this.brokerName,
            "BSP");
        consumer.start(); // Start the consumer.
        this.consumers.add(consumer);
      }
      int maxSize = 0;
      String incomingIndex = null;
      ConcurrentLinkedQueue<IMessage> maxQueue = null;
   // Wait until the consumerTool has finished, the Receiver will finish.
      while (true) {
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
            this.messageQueues.incomeAMessage(incomingIndex, message);
          }
        } else {
          try { // For no combiner define, just sleep for next check.
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.error("[Receiver] caught", e);
          }
        }
        Iterator<ConsumerTool> itr = consumers.iterator();
        int running = 0;
        while (itr.hasNext()) {
          ConsumerTool thread = itr.next();
          if (thread.isAlive()) {
            running++;
          }
        }
        if (running <= 0) { // All consumers are finished.
          break;
        }
      }
      this.communicator.setReceivedMessageCounter(this.messageCount);
      this.communicator.setReceivedMessageBytesCounter(this.messageBytesCount);
      LOG.info("[Receiver] has received " + this.messageCount
          + " BSPMessages totally!");
      LOG.info("[Receiver] exits.");
    } catch (Exception e) {
      LOG.error("[Receiver] caught: ", e);
    }
  }
  
  /**
   * To tell the consumer tool that there are no more messages for it.
   * @param flag
   */
  public void setNoMoreMessagesFlag(boolean flag) {
    this.noMoreMessagesFlag = flag;
  }
  
  /**Flag of noMoreMessagesFlag.*/
  public boolean getNoMoreMessagesFlag() {
    return this.noMoreMessagesFlag;
  }
  
  /**Set method of combineThreshold.*/
  public void setCombineThreshold(int aCombineThreshold) {
    if (this.combineThreshold == 0) {
      this.combineThreshold = aCombineThreshold;
    }
  }
  
  /**
   * Combine the incoming message queue.
   * @return IMessage
   */
  @SuppressWarnings("unchecked")
  private IMessage combine(ConcurrentLinkedQueue<IMessage> incomingQueue) {
    IMessage msg = (IMessage) this.combiner.combine(incomingQueue.iterator());
    return msg;
  }

  /** For JUnit test. */
  public boolean isCombinerFlag() {
    return combinerFlag;
  }

  public Long getMessageCount() {
    return messageCount;
  }

  public Long getMessageBytesCount() {
    return messageBytesCount;
  }
}
