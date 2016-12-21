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
import com.chinamobile.bcbsp.util.BSPJobID;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sender belongs to a communicator, for sending messages from the outgoing
 * queues.
 */
public class Sender extends Thread implements CombineSenderInterface {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(Sender.class);
  /** time change. */
  private static final float TIME_CHANGE = 1000f;
  /** the time of send messages connection. */
  private Long connectTime = 0L;
  /** Clock of send time. */
  private Long sendTime = 0L;
  /** flag of ActiveMQ sender condition controller. */
  private boolean condition = false;
  /** BSP Job ID. */
  private BSPJobID jobID = null;
  /** message counter. */
  private int messageCount = 0;
  /** message byte counter. */
  private long messageBytesCount = 0;
  /** the communication tool for BSP. */
  private Communicator communicator = null;
  /** message Queues. */
  private MessageQueuesInterface messageQueues = null;
  /** the flag of combine. */
  private boolean combinerFlag = false;
  /** combiner of sender. */
  private Combiner combiner = null;
  /** the combine tool of sender to combine for outgoing queues. */
  private CombinerTool combinerTool = null;
  /** the threshold of combine. */
  private int combineThreshold;
  /** the threshold of sender. */
  private int sendThreshold;
  /** message package size. */
  private int packSize;
  /** max number of job producer. */
  private int maxProducerNum;
  /** the flag of no more message, if true thread sleep. */
  private boolean noMoreMessagesFlag = false;
  /** ProducerTools Pool. */
  private ProducerPool producerPool = null;
  /** The current progress superstep counter. */
  private volatile int superStepCounter = 0;
  /** The flag for a super step. */
  private volatile boolean over = false;
  /** The flag for the whole staff. */
  private volatile boolean completed = false;
  
  /**
   * Constructor of Sender ActiveMQ.
   * @param comm
   */
  public Sender(Communicator comm) {
    this.communicator = comm;
    this.sendThreshold = this.communicator.getJob().getSendThreshold();
    this.packSize = this.communicator.getJob().getMessagePackSize();
    this.jobID = this.communicator.getBSPJobID();
    this.messageQueues = this.communicator.getMessageQueues();
    this.combinerFlag = this.communicator.isSendCombineSetFlag();
    // Get the combiner from the communicator.
    if (combinerFlag) {
      this.combiner = this.communicator.getCombiner();
      this.combineThreshold = this.communicator.getJob()
          .getSendCombineThreshold();
    }
    this.maxProducerNum = this.communicator.getJob().getMaxProducerNum();
    this.superStepCounter = 0;
  }
  
  /**
   * To tell the consumer tool that there are no more messages for it.
   * @param flag
   */
  public void setNoMoreMessagesFlag(boolean flag) {
    this.noMoreMessagesFlag = flag;
  }
  
  /**
   * Get the flag of noMoreMessagesFlag.
   * @return boolean
   */
  public boolean getNoMoreMessagesFlag() {
    return this.noMoreMessagesFlag;
  }
  
  /**
   * Set the sendThreshold.
   * @param aSendThreshold
   */
  public void setSendThreshold(int aSendThreshold) {
    if (this.sendThreshold == 0) {
      this.sendThreshold = aSendThreshold;
    }
  }
  
  /**
   * Set the sendThreshold.
   * @param aSendThreshold
   */
  public void setCombineThreshold(int aCombineThreshold) {
    if (this.combineThreshold == 0) {
      this.combineThreshold = aCombineThreshold;
    }
  }
  
  /**
   * Set the packSize of message.
   * @param size
   */
  public void setMessagePackSize(int size) {
    if (this.packSize == 0) {
      this.packSize = size;
    }
  }
  
  /**
   * Set the packSize of message.
   * @param size
   */
  public void setMaxProducerNum(int num) {
    if (this.maxProducerNum == 0) {
      this.maxProducerNum = num;
    }
  }
  
  /**
   * Add the time of connectTime.
   * @param size
   */
  public void addConnectTime(long time) {
    synchronized (this.connectTime) {
      this.connectTime += time;
    }
  }
  
  /**
   * Add the time of sendTime.
   * @param size
   */
  public void addSendTime(long time) {
    synchronized (this.sendTime) {
      this.sendTime += time;
    }
  }
  
  /** Run method of Sender Thread. */
  public void run() {
    try {
      LOG.info("[Sender] starts successfully!");
      this.initialize();
      synchronized (this) {
        try {
          while (!this.condition) {
            wait(); // wait for first begin.
          }
          this.condition = false;
        } catch (InterruptedException e) {
          LOG.error("[Sender] caught: ", e);
        }
      }
      do { // while for totally complete for this staff.
        LOG.info("[Sender] begins to work for sueper step <"
            + this.superStepCounter + ">!");
        this.connectTime = 0L;
        /* Clock */
        this.sendTime = 0L;
        ProducerTool producer = null;
        if (combinerFlag && !this.noMoreMessagesFlag) {
          this.combinerTool = new CombinerTool(this, messageQueues, combiner,
              combineThreshold);
          this.combinerTool.start();
        }
        String outgoingIndex = null;
        ConcurrentLinkedQueue<IMessage> maxQueue = null;
        int maxSize = 0;
        while (true) { // Keep sending until the whole outgoingQueues is empty
          maxSize = 0;
          maxQueue = null;
          // outgoingIndex = this.messageQueues.getMaxOutgoingQueueIndex();
          outgoingIndex = this.messageQueues.getNextOutgoingQueueIndex();
          if (outgoingIndex != null) {
            maxSize = this.messageQueues.getOutgoingQueueSize(outgoingIndex);
          }
          // When the whole outgoingQueues are empty and no more messages will
          // come, exit while.
          if (outgoingIndex == null) {
            if (this.noMoreMessagesFlag) {
              if (this.messageQueues.getOutgoingQueuesSize() > 0) {
                continue;
              }
              if (!this.combinerFlag) {
                /** no more messages and no combiner set */
                LOG.info("[Sender] exits while for no more messages "
                    + "and no combiner set.");
                break;
              } else {
                if (this.combinerTool == null) {
                  /* no more messages and no combiner created */
                  LOG.info("[Sender] exits while for no more messages "
                      + "and no combiner created.");
                  break;
                } else if (!this.combinerTool.isAlive()) {
                  /* no more messages and combiner has exited */
                  LOG.info("[Sender] exits while for no more messages "
                      + "and combiner has exited.");
                  break;
                }
              }
            } else {
              try {
                Thread.sleep(500);
              } catch (Exception e) {
                LOG.error("[Sender] caught: ", e);
              }
              continue;
            }
          } else if (maxSize > this.sendThreshold || this.noMoreMessagesFlag) {
            // 这里的判断是为发送超过发送阈值的消息或者超级步结束时候将所有消息发送出去
            // Get a producer tool to send the maxQueue
            producer = this.producerPool.getProducer(outgoingIndex,
                this.superStepCounter, this);
            if (producer.isIdle()) {
              // Get the queue out of the map.
              maxQueue = this.messageQueues.removeOutgoingQueue(outgoingIndex);
              if (maxQueue == null) {
                continue;
              }
              maxSize = maxQueue.size();
              // add by chen
              for (IMessage m : maxQueue) {
                this.messageBytesCount += m.size();
              }
              producer.setPackSize(this.packSize);
              producer.addMessages(maxQueue);
              this.messageCount += maxSize;
              // Set the producer's state to busy to start it.
              producer.setIdle(false);
            }
            if (producer.isFailed()) {
              this.messageQueues.removeOutgoingQueue(outgoingIndex);
            }
          }
        } // while
        LOG.info("[Sender] has started " + producerPool.getProducerCount()
            + " producers totally.");
        // Tell all producers that no more messages.
        this.producerPool.finishAll();
        while (true) { // Wait for the producers finish message sending.
          int running = this.producerPool
              .getActiveProducerCount(this.superStepCounter);
          LOG.info("[Sender] There are still <" + running
              + "> ProducerTools alive.");
          if (running == 0) {
            break;
          }
          try {
            Thread.sleep(500);
          } catch (Exception e) {
            LOG.error("[Sender] caught: ", e);
          }
        } // while
        // add by chen
        this.communicator.setCombineOutgoMessageCounter(this.messageCount);
        this.communicator.setCombineOutgoMessageBytesCounter(messageBytesCount);
        LOG.info("[Sender] has sent " + this.messageCount
            + " messages totally for super step <" + this.superStepCounter
            + ">! And enter waiting......");
        LOG.info("[==>Clock<==] <Sender's create connection> used "
            + this.connectTime / TIME_CHANGE + " seconds");
        /* Clock */
        LOG.info("[==>Clock<==] <Sender's send messages> used " + this.sendTime
            / TIME_CHANGE + " seconds");
        this.over = true;
        synchronized (this) {
          try {
            while (!this.condition) {
              wait();
            }
            this.condition = false;
          } catch (InterruptedException e) {
            LOG.error("[Sender] caught: ", e);
          }
        }
      } while (!this.completed);
      LOG.info("[Sender] exits.");
    } catch (Exception e) {
      LOG.error("Sender caught: ", e);
    }
  } // run
  
  /**
   * Show the initialization information of sender. initialize the rpcSlavePool.
   */
  private void initialize() {
    LOG.info("========== Initialize Sender ==========");
    LOG.info("[Send Threshold] = " + this.sendThreshold);
    LOG.info("[Message Pack Size] = " + this.packSize);
    if (this.combinerFlag) {
      LOG.info("[Combine Threshold for sending] = " + this.combineThreshold);
    }
    LOG.info("[Max Producer Number] = " + this.maxProducerNum);
    LOG.info("=======================================");
    this.producerPool = new ProducerPool(this.maxProducerNum,
        this.jobID.toString());
  }
  
  /**
   * Begin the s ender's task for a super step.
   * @param superStepCount
   */
  public void begin(int superStepCount) {
    this.superStepCounter = superStepCount;
    if (this.producerPool == null) {
      this.producerPool = new ProducerPool(this.maxProducerNum,
          this.jobID.toString());
      LOG.error("Test Null this.producePool is null and it is re-initialized");
    }
    this.producerPool.setActiveProducerProgress(superStepCount);
    this.producerPool.cleanFailedProducer();
    
    this.noMoreMessagesFlag = false;
    this.over = false;
    this.messageCount = 0;
    this.messageBytesCount = 0;
    synchronized (this) {
      this.condition = true;
      notify();
    }
  }
  
  /**
   * Justify if the sender has finished the task for the current super step.
   * @return over
   */
  public boolean isOver() {
    return this.over;
  }
  
  /**
   * To notice the sender to complete and return.
   */
  public void complete() {
    this.producerPool.completeAll();
    this.completed = true;
    synchronized (this) {
      this.condition = true;
      notify();
    }
  }

  public int getSuperStepCounter() {
    return superStepCounter;
  }

  public void setSuperStepCounter(int superStepCounter) {
    this.superStepCounter = superStepCounter;
  }

  public boolean isCondition() {
    return condition;
  }

  public void setCondition(boolean condition) {
    this.condition = condition;
  }
}
