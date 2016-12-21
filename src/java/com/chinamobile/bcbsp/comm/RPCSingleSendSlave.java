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

import com.chinamobile.bcbsp.rpc.RPCCommunicationProtocol;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import java.net.InetSocketAddress;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * The RPC send thread instance. Send as package for queues.
 */
public class RPCSingleSendSlave extends Thread {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(RPCSingleSendSlave.class);
  /** Converter byte to MByte. */
  private static final long MB_SIZE = 1048576;
  /** Clock for connect time. */
  private long connectTime = 0;
  /** Clock for send time. */
  private long sendTime = 0;
  /** Threshold for reconnect time. */
  private static final int RECONNECTTHRESHOLD = 10;
  /** reconnect counter. */
  private int reconnectCount = 0;
  /** Default 100 messages for a package. */
  private int packSize = 1000;
  /** total number of messages. */
  private long messageCount = 0;
  /** Message queue for sending. */
  private ConcurrentLinkedQueue<IMessage> messageQueue = null;
  /** host name and port. */
  private String hostNameAndPort = null;
  /** flag of new host name and port. */
  private boolean newHostNameAndPort = false;
  /** flag of idle thread. */
  private volatile boolean idle = true;
  /** flag of no more messages. */
  private volatile boolean noMoreMessagesFlag = false;
  /** superstep counter. */
  private volatile int superStepCounter = -1;
  /** flag of complete. */
  private volatile boolean completed = false;
  /** flag of failed. */
  private boolean isFailed = false;
  /** Clock for sleep time. */
  private long sleepTime = 500;
  /** sender of RPC. */
  private RPCSender sender = null;
  /** staff ID for test. */
  private String staffId;
  /** RPC communication protocol. */
  private RPCCommunicationProtocol senderProtocol = null;
  
  /**
   * Constructor of RPCSingleSendSlave.
   * @param group
   * @param sn
   * @param queue
   * @param hostNameAndPort
   * @param subject
   * @param sender
   */
  public RPCSingleSendSlave(ThreadGroup group, int sn,
      ConcurrentLinkedQueue<IMessage> queue, String hostNameAndPort,
      String subject, RPCSender sender) {
    super(group, "SingeRPCSendSlave-" + sn);
    this.messageQueue = queue;
    this.hostNameAndPort = hostNameAndPort;
    this.newHostNameAndPort = true;
    this.sender = sender;
  }
  
  /** Add messages into messageQueue. */
  public void addMessages(ConcurrentLinkedQueue<IMessage> messages) {
    messageQueue = messages;
  }
  
  /** Set the new host name and port for reuse the thread. */
  public void setHostNameAndPort(String hostNameAndPort) {
    if (!hostNameAndPort.equals(this.hostNameAndPort)) {
      this.hostNameAndPort = hostNameAndPort;
      this.newHostNameAndPort = true;
    }
  }
  
  /** Set method of packSize. */
  public void setPackSize(int size) {
    this.packSize = size;
  }
  
  /** Get the flag of idle. */
  public boolean isIdle() {
    return idle;
  }
  
  /** Set the flag of idle. */
  public void setIdle(boolean state) {
    this.idle = state;
  }
  
  /** Set method of NoMoreMessagesFlag. */
  public void setNoMoreMessagesFlag(boolean flag) {
    this.noMoreMessagesFlag = flag;
  }
  
  /**
   * Get the superStepCounter.
   * @return int
   */
  public int getProgress() {
    return this.superStepCounter;
  }
  
  /** Set method of superStepCounter. */
  public void setProgress(int superStepCount) {
    this.superStepCounter = superStepCount - 1;
  }
  
  /** Set complete flag true. */
  public void complete() {
    this.completed = true;
  }
  
  /** Get the flag of failed. */
  public boolean isFailed() {
    return this.isFailed;
  }

  /** Run method of Thread. */
  public void run() {
    while (true) {
      while (this.idle) {
        if (this.completed) {
          return;
        }
        if (this.noMoreMessagesFlag) {
          this.superStepCounter++;
          this.noMoreMessagesFlag = false;
        }
        try {
          Thread.sleep(this.sleepTime);
        } catch (InterruptedException e) {
          LOG.error("[RPCSingleSendSlave] to " + this.hostNameAndPort
              + " has been interrupted for ", e);
          return;
        }
      }
      if (this.hostNameAndPort == null) {
        LOG.error("Destination hostname is null.");
        return;
      }
      if (this.messageQueue == null) {
        LOG.error("Message queue for ProducerTool is null.");
        return;
      }
      this.messageCount = 0;
      this.connectTime = 0;
      this.sendTime = 0;
      while (true) {
        if (this.reconnectCount == RPCSingleSendSlave.RECONNECTTHRESHOLD) {
          break;
        }
        try {
          if (this.newHostNameAndPort) { // Should create new connection
            if (senderProtocol != null) {
              LOG.info("Sender RPC protocol is not null.");
            }else{
              LOG.info("Sender RPC protocol is null and it will instanced now.");
            }
            long start = System.currentTimeMillis();
            String[] tmp = this.hostNameAndPort.split(":");
            String hostname = tmp[0];
            int portNum = Integer.parseInt(tmp[1]);
            this.senderProtocol = (RPCCommunicationProtocol) RPC.waitForProxy(
                RPCCommunicationProtocol.class,
                RPCCommunicationProtocol.protocolVersion,
                new InetSocketAddress(hostname, portNum), new Configuration());
            this.connectTime += (System.currentTimeMillis() - start);
            this.newHostNameAndPort = false;
          }
          // Start sending messages
          this.sendPacked();
          this.idle = true;
          break;
        } catch (Exception e) {
          this.reconnectCount++;
          if (this.reconnectCount == 1) {
            LOG.error("[SingRPCSendSlave] to " + this.hostNameAndPort
                + " caught: ", e);
          }
          LOG.info("[SingRPCSendSlave] to " + this.hostNameAndPort
              + " is reconnecting for " + this.reconnectCount + "th time.");
          LOG.info("---------------- Memory Info ------------------");
          MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
          MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
          long used = memoryUsage.getUsed();
          long committed = memoryUsage.getCommitted();
          LOG.info("[JVM Memory used] = " + used / MB_SIZE + "MB");
          LOG.info("[JVM Memory committed] = " + committed / MB_SIZE + "MB");
          LOG.info("-----------------------------------------------");
          try {
            Thread.sleep(this.sleepTime);
          } catch (InterruptedException e1) {
            LOG.error("[SingRPCSendSlave] caught: ", e1);
          }
        }
      }
      LOG.info("[SingRPCSendSlave] to " + this.hostNameAndPort + " has sent "
          + this.messageCount + " messages totally! (with "
          + this.messageQueue.size() + " messages lost!)");
      this.sender.addConnectTime(this.connectTime);
      /* Clock */
      this.sender.addSendTime(this.sendTime);
      if (this.reconnectCount == RPCSingleSendSlave.RECONNECTTHRESHOLD) {
        LOG.info("[ProducerTool] to " + this.hostNameAndPort
            + " has reconnected for " + this.reconnectCount
            + " times but failed!");
        this.isFailed = true;
        break;
      }
    }
  }
  
  /** Send the package of messages with RPC.*/
  public void sendPacked() {
    IMessage msg;
    int count = 0;
    BSPMessagesPack pack = new BSPMessagesPack();
    ArrayList<IMessage> content = new ArrayList<IMessage>();
    // just for test
    int maxSize = messageQueue.size();
    while ((msg = messageQueue.poll()) != null) {
      content.add(msg);
      count++;
      this.messageCount++;
      if (count == this.packSize) {
        pack.setPack(content);
        long start = System.currentTimeMillis();
        /* Clock */
        this.senderProtocol.sendPackedMessage(pack);
        this.sendTime += (System.currentTimeMillis() - start);
        content.clear();
        count = 0;
      }
    }
    if (content.size() > 0) {
      pack.setPack(content);
      long start = System.currentTimeMillis();
      /* Clock */
      this.senderProtocol.sendPackedMessage(pack);
      this.sendTime += (System.currentTimeMillis() - start);
      /* Clock */
      content.clear();
    }
  }
  
  /** Send messages with RPC and not used as package.*/
  public void sendUnpacked() {
    int i = 0;
    IMessage msg = null;
    while ((msg = messageQueue.poll()) != null) {
      i++;
      long start = System.currentTimeMillis();
      /* Clock */
      this.senderProtocol.sendUnpackedMessage(msg);
      this.sendTime += (System.currentTimeMillis() - start);
    }
    this.messageCount += i;
  }
  
  /**
   * Get method of staffId.
   * @return String
   */
  public String getStaffId() {
    return staffId;
  }
  
  /**
   * Set method of staffId.
   * @param staffId
   */
  public void setStaffId(String staffId) {
    this.staffId = staffId;
  }
}
