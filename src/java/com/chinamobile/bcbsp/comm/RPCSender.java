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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * A sender belongs to a communicator, for sending messages from the outgoing
 * queues. This is the real thread for controlling of the message. Itself has
 * some control signal, the control sends instances of specific state.
 */
public class RPCSender extends Thread implements CombineSenderInterface {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(RPCSender.class);
  /** time change. */
  private static final float TIME_CHANGE = 1000f; 
  /** the time of send messages connection. */
  private Long connectTime = 0L;
  /** Clock of send time. */
  private Long sendTime = 0L;
  /** flag of RPC sender condition controller. */
  private boolean condition = false;
  /** BSP Job ID. */
  private BSPJobID jobID = null;
  /** message counter. */
  private int messageCount = 0;
  /** message byte counter. */
  private long messageBytesCount = 0;
  /** the communication tool for BSP. */
  private RPCCommunicator communicator = null;
  /** message Queues. */
  private MessageQueuesInterface messageQueues = null;
  /** thread pool of sender. */
  private RPCSendSlavePool rpcSlavePool = null;
  /** the flag of combine. */
  private boolean combinerFlag = false;
  /** combiner of sender. */
  private Combiner combiner = null;
  /** the combine tool of sender to combine for outgoing queues. */
  private CombinerTool combinerTool = null;
  // some controll params
  /** the threshold of combine. */
  private int combineThreshold;
  /** the threshold of sender. */
  private int sendThreshold;
  /** message package size. */
  private int packSize;
  /** max number of job producer. */
  private int maxSlaveNum;
  /** the flag of no more message, if true thread sleep. */
  private boolean noMoreMessagesFlag = false;
  /** the flag of send complete. */
  private volatile boolean completed = false;
  /** the flag for a super step. */
  private volatile boolean over = false;
  /** The superstep counter current progress. */
  private volatile int superStepCounter = 0;
  /** staff ID for test. */
  private String staffId;
  
  /**
   * Constructor of RPC Sender.
   * @param comm
   */
  public RPCSender(RPCCommunicator comm) {
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
    this.maxSlaveNum = this.communicator.getJob().getMaxProducerNum();
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
    if (this.maxSlaveNum == 0) {
      this.maxSlaveNum = num;
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
    LOG.info("[Max Slave Number] = " + this.maxSlaveNum);
    LOG.info("=======================================");
    this.rpcSlavePool = new RPCSendSlavePool(this.maxSlaveNum,
        this.jobID.toString());
  }
  
  /**
   * Begin the sender's task for a super step. 每一个超级步都要开启阻塞的sender实例
   * @param superStepCount
   */
  public void begin(int superStepCount) {
    this.superStepCounter = superStepCount;
    if (this.rpcSlavePool == null) {
      this.rpcSlavePool = new RPCSendSlavePool(this.maxSlaveNum,
          this.jobID.toString());
      LOG.error("Test Null this.producePool is null and it is re-initialized");
    }
    this.rpcSlavePool.setActiveSlaveProgress(superStepCount);
    this.rpcSlavePool.cleanFailedSlave();
    this.noMoreMessagesFlag = false;
    this.over = false;
    this.messageCount = 0;
    this.messageBytesCount = 0L;
    // sender 的同步控制
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
    this.rpcSlavePool.completeAll();
    this.completed = true;
    synchronized (this) {
      this.condition = true;
      notify();
    }
  }
  
  /** Run method of Sender Thread. */
  public void run() {
    try {
      LOG.info("[RPCSender] starts successfully!");
      this.initialize();
      synchronized (this) {
        try {
          while (!this.condition) {
            wait();
          }
          this.condition = false;
        } catch (InterruptedException e) {
          LOG.error("[RPCSender] caught: ", e);
        }
      }
      do {
        LOG.info("[RPCSender] begins to work for super step <"
            + this.superStepCounter + ">!");
        LOG.info("this.messageQueues.getNextOutgoingQueueIndex(); "
            + this.messageQueues.getNextOutgoingQueueIndex());
        this.connectTime = 0L;
        /** Clock */
        this.sendTime = 0L;
        /** Clock */
        RPCSingleSendSlave slave = null;
        if (combinerFlag && !this.noMoreMessagesFlag) {
          this.combinerTool = new CombinerTool(this, messageQueues, combiner,
              combineThreshold);
          this.combinerTool.start();
        }
        String outgoingIndex = null;
        ConcurrentLinkedQueue<IMessage> maxQueue = null;
        int maxSize = 0;
        while (true) {
          // and no more messages will come.
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
                LOG.info("[RPCSender] exits while for no more messages "
                    + "and no combiner set.");
                break;
              } else {
                if (this.combinerTool == null) {
                  /** no more messages and no combiner created */
                  LOG.info("[RPCSender] exits while for no more messages "
                      + "and no combiner created.");
                  break;
                } else if (!this.combinerTool.isAlive()) {
                  /** no more messages and combiner has exited */
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
            // Get a producer tool to send the maxQueue
            slave = this.rpcSlavePool.getSlave(outgoingIndex,
                this.superStepCounter, this);
            if (slave.isIdle()) {
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
              slave.setPackSize(this.packSize);
              LOG.info("current slave id is " + slave.getId());
              LOG.info("NOW senderSlave will sender messageQueue and size = "
                  + maxSize);
              slave.setStaffId(this.staffId);
              slave.addMessages(maxQueue);
              this.messageCount += maxSize;
              // Set the producer's state to busy to start it.
              slave.setIdle(false);
            }
            if (slave.isFailed()) {
              this.messageQueues.removeOutgoingQueue(outgoingIndex);
            }
          }
        } // while
        LOG.info("[RPCSender] has started " + this.rpcSlavePool.getSlaveCount()
            + " slaves totally.");
        // Tell all producers that no more messages.
        this.rpcSlavePool.finishAll();
        // 属于sender内部发送线程组的同步，确保所有的发送实例完成
        while (true) { // Wait for the producers finish message sending.
          int running = this.rpcSlavePool
              .getActiveSlaveCount(this.superStepCounter);
          LOG.info("[RPCSender] There are still <" + running
              + "> RPCSendSlaves alive.");
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
        LOG.info("[RPCSender] has sent " + this.messageCount
            + " messages totally for super step <" + this.superStepCounter
            + ">! And enter waiting......");
        LOG.info("[==>Clock<==] <RPCSender's create connection> used "
            + this.connectTime / TIME_CHANGE + " seconds");
        /** Clock */
        LOG.info("[==>Clock<==] <RPCSender's send messages> used "
            + this.sendTime / TIME_CHANGE + " seconds");
        /** Clock */
        this.over = true;
        synchronized (this) {
          try {
            while (!this.condition) {
              wait();
            }
            this.condition = false;
          } catch (InterruptedException e) {
            LOG.error("[RPCSender] caught: ", e);
          }
        }
      } while (!this.completed);
      LOG.info("[PRCSender] exits.");
    } catch (Exception e) {
      LOG.error("RPCSender caught: ", e);
    }
  }
  
  /**
   * Get the staffId.
   * @return String
   */
  public String getStaffId() {
    return staffId;
  }
  
  /**
   * Set the staffId.
   * @param staffId
   */
  public void setStaffId(String staffId) {
    this.staffId = staffId;
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
