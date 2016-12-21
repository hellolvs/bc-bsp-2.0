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

import com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.BSPActiveMQConnFactory;
import com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.impl.BSPActiveMQConnFactoryImpl;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.ObjectMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Producer tool for sending messages to Message Queue. */
public class ProducerTool extends Thread {
  
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(ProducerTool.class);
  /** Converter byte to MByte. */
  private static final long MB_SIZE = 1048576;
  /** Clock for connect time. */
  private long connectTime = 0;
  /** Clock for send time. */
  private long sendTime = 0;
  /** Clock for serialize time. */
  private long serializeTime = 0;
  /** Threshold for reconnect time. */
  private static final int RECONNECTTHRESHOLD = 10;
  /** reconnect counter. */
  private int reconnectCount = 0;
  /** Default 100 messages for a package. */
  private int packSize = 100;
  /** Connection for ActiveMQ. */
  private Connection connection = null;
  /** message session. */
  private Session session = null;
  /** message destination. */
  private Destination destination = null;
  /** message consumer. */
  private MessageProducer producer = null;
  /** Clock for sleep time. */
  private long sleepTime = 500;
  /** Clock for time live. */
  private long timeToLive = 0;
  /** url address."tcp://hostName:port". */
  private String url = null;
  /** flag of transacted. */
  private boolean transacted = false;
  /** flag of durable. */
  private boolean persistent = false;
  /** total number of messages. */
  private long messageCount = 0; // Total count.
  /** flag of failed. */
  private boolean isFailed = false;
  /** Message queue for sending. */
  private ConcurrentLinkedQueue<IMessage> messageQueue = null;
  /** host name and port. */
  private String hostNameAndPort = null;
  /** flag of new host name and port. */
  private boolean newHostNameAndPort = false;
  /** Should be jobID. if jobs's no =1, default is OK. */
  private String subject = null;
  /** flag of idle thread. */
  private volatile boolean idle = true;
  /** flag of no more messages. */
  private volatile boolean noMoreMessagesFlag = false;
  /** superstep counter. */
  private volatile int superStepCounter = -1;
  /** flag of complete. */
  private volatile boolean completed = false;
  /** sender of ActiveMQ. */
  private Sender sender = null;
  
  /**
   * Constructor of ProducerTool.
   * @param queue
   * @param hostNameAndPort
   * @param subject
   */
  public ProducerTool(final ThreadGroup group, final int sn,
      final ConcurrentLinkedQueue<IMessage> queue,
      final String hostNameAndPort, final String subject, final Sender sender) {
    super(group, "ProducerTool-" + sn);
    this.messageQueue = queue;
    this.hostNameAndPort = hostNameAndPort;
    this.newHostNameAndPort = true;
    this.subject = subject;
    this.sender = sender;
  }
  
  /**
   * Add messages into messageQueue.
   * @param messages
   */
  public final void addMessages(final ConcurrentLinkedQueue<IMessage> messages) {
    messageQueue = messages;
  }
  
  /** Set the host name and port. */
  public final void setHostNameAndPort(final String hostNameAndPort) {
    if (!hostNameAndPort.equals(this.hostNameAndPort)) {
      this.hostNameAndPort = hostNameAndPort;
      this.newHostNameAndPort = true;
    }
  }
  
  /** Set method of packSize. */
  public final void setPackSize(final int size) {
    this.packSize = size;
  }
  
  /** Get the flag of idle. */
  public final boolean isIdle() {
    return idle;
  }
  
  /** Set the flag of idle. */
  public final void setIdle(final boolean state) {
    this.idle = state;
  }
  
  /** Set method of NoMoreMessagesFlag. */
  public final void setNoMoreMessagesFlag(final boolean flag) {
    this.noMoreMessagesFlag = flag;
  }
  
  /** Get the superStepCounter. */
  public final int getProgress() {
    return this.superStepCounter;
  }
  
  /** Set the superStepCounter. */
  public final void setProgress(final int superStepCount) {
    this.superStepCounter = superStepCount - 1;
  }
  
  /** Set complete flag true. */
  public final void complete() {
    this.completed = true;
  }
  
  /** Show the information of Parameters. */
  public final void showParameters() {
    LOG.info("Connecting to URL: " + url);
    LOG.info("Publishing Messages " + "to queue: " + subject);
    LOG.info("Using " + (persistent ? "persistent" : "non-persistent")
        + " messages");
    LOG.info("Sleeping between publish " + sleepTime + " ms");
    
    if (timeToLive != 0) {
      LOG.info("Messages time to live " + timeToLive + " ms");
    }
  }
  
  /** Run method of Thread. */
  public final void run() {
    while (true) {
      while (this.idle) {
        if (this.completed) {
          return;
        }
        if (this.noMoreMessagesFlag) {
          this.superStepCounter++;
          this.noMoreMessagesFlag = false;
          // LOG.info("Test Progress: from " + (this.superStepCounter - 1) +
          // " to " + this.superStepCounter);
        }
        try {
          Thread.sleep(this.sleepTime);
        } catch (InterruptedException e) {
          LOG.error("[ProducerTool] to " + this.hostNameAndPort
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
        if (this.reconnectCount == ProducerTool.RECONNECTTHRESHOLD) {
          break;
        }
        try {
          if (this.newHostNameAndPort) { // Should create new connection.
            if (connection != null) {
              try {
                connection.close();
              } catch (Throwable ignore) {
                LOG.warn("[ConsumerTool] run connection " + ignore);
              }
            }
            long start = System.currentTimeMillis();
            /** Clock */
            // Make the destination broker's url.
            this.url = "tcp://" + this.hostNameAndPort;
            // Create the connection.
            // ActiveMQConnectionFactory connectionFactory = new
            // ActiveMQConnectionFactory(
            // user, password, url);
            BSPActiveMQConnFactory connectionFactory = new BSPActiveMQConnFactoryImpl();
            connectionFactory.activeMQConnFactoryMethod(url);
            connectionFactory.setCopyMessageOnSend(false);
            connection = connectionFactory.createConnection();
            connection.start();
            // Create the session
            session = connection.createSession(transacted,
                Session.AUTO_ACKNOWLEDGE);
            this.connectTime += (System.currentTimeMillis() - start);
            /* Clock */
            this.newHostNameAndPort = false;
            start = System.currentTimeMillis();
            /* Clock */
            destination = session.createQueue(subject);
            // Create the producer.
            producer = session.createProducer(destination);
            if (persistent) {
              producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            } else {
              producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
            if (timeToLive != 0) {
              producer.setTimeToLive(timeToLive);
            }
            this.connectTime += (System.currentTimeMillis() - start);
          }
          // Start sending messages
          sendLoopOptimistic(session, producer);
          this.idle = true;
          break;
        } catch (Exception e) {
          this.reconnectCount++;
          if (this.reconnectCount == 1) {
            LOG.error(
                "[ProducerTool] to " + this.hostNameAndPort + " caught: ", e);
          }
          LOG.info("[ProducerTool] to " + this.hostNameAndPort
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
            LOG.error("[ProducerTool] caught: ", e1);
          }
        }
      }
      LOG.info("[ProducerTool] to " + this.hostNameAndPort + " has sent "
          + this.messageCount + " messages totally! (with "
          + this.messageQueue.size() + " messages lost!)");
      this.sender.addConnectTime(this.connectTime);
      /* Clock */
      this.sender.addSendTime(this.sendTime);
      /* Clock */
      if (this.reconnectCount == ProducerTool.RECONNECTTHRESHOLD) {
        LOG.info("[ProducerTool] to " + this.hostNameAndPort
            + " has reconnected for " + this.reconnectCount
            + " times but failed!");
        this.isFailed = true;
        break;
      }
    }
  }
  
  /** Get the flag of failed. */
  public final boolean isFailed() {
    return this.isFailed;
  }
  
  /**
   * Send message into messageQueue and update information.
   * @param session
   * @param producer
   * @throws Exception
   *         e
   */
  private void sendLoop(final Session session, final MessageProducer producer)
      throws Exception {
    IMessage msg;
    int count = 0;
    BSPMessagesPack pack = new BSPMessagesPack();
    ArrayList<IMessage> content = new ArrayList<IMessage>();
    while ((msg = messageQueue.poll()) != null) {
      content.add(msg);
      count++;
      this.messageCount++;
      // enough for a pack.
      if (count == this.packSize) {
        pack.setPack(content);
        long start = System.currentTimeMillis();
        /** Clock */
        ObjectMessage message = session.createObjectMessage(pack);
        producer.send(message);
        this.sendTime += (System.currentTimeMillis() - start);
        /** Clock */
        content.clear();
        count = 0;
      }
    }
    // remaining messages into a pack.
    if (content.size() > 0) {
      pack.setPack(content);
      long start = System.currentTimeMillis();
      /* Clock */
      ObjectMessage message = session.createObjectMessage(pack);
      producer.send(message);
      this.sendTime += (System.currentTimeMillis() - start);
      /* Clock */
      content.clear();
    }
  }
  
  /**
   * Send message into messageQueue, update information with serialize method.
   * @param session
   * @param producer
   * @throws Exception
   *         e
   */
  private void sendLoopOptimistic(final Session session,
      final MessageProducer producer) throws Exception {
    try {
      BSPMessage msg;
      int count = 0;
      int packCounts = messageQueue.size() / this.packSize;
      // LOG.info("send packSize = "+ this.packSize);
      int packCount = 0;
      while (packCount < packCounts) {
        BytesMessage message = session.createBytesMessage();
        long start = System.currentTimeMillis();
        /* Clock */
        message.writeInt(this.packSize);
        count = 0;
        while (count < this.packSize) {
          msg = (BSPMessage) messageQueue.poll();
          // LOG.info(msg.intoString());
          // message.setInt("dstPartition", msg.getDstPartition());
          message.writeInt(msg.getDstPartition());
          // message.writeUTF(msg.getSrcVertexID());
          // message.setString("dstVertexID", msg.getDstVertexID());
          message.writeUTF(msg.getDstVertexID());
          // message.setBytes("tag", msg.getTag());
          message.writeInt(msg.getTag().length);
          message.writeBytes(msg.getTag());
          // message.setBytes("data", msg.getData());
          message.writeInt(msg.getData().length);
          message.writeBytes(msg.getData());
          count++;
          this.messageCount++;
        }
        this.serializeTime += (System.currentTimeMillis() - start);
        /* Clock */
        start = System.currentTimeMillis();
        /* Clock */
        producer.send(message);
        this.sendTime += (System.currentTimeMillis() - start);
        /* Clock */
        packCount++;
        // if (messageCount % 100000 == 0 ){
        // LOG.info("send " + messageCount);
        // }
      }
      // send remaining messags
      int sendSize = messageQueue.size();
      if (sendSize != 0) {
        BytesMessage message = session.createBytesMessage();
        long start = System.currentTimeMillis();
        /* Clock */
        // message.setInt("packSize", sendSize);
        message.writeInt(sendSize);
        while ((msg = (BSPMessage) messageQueue.poll()) != null) {
          // message.setInt("dstPartition", msg.getDstPartition());
          message.writeInt(msg.getDstPartition());
          // message.setString("dstVertexID", msg.getDstVertexID());
          message.writeUTF(msg.getDstVertexID());
          // message.setBytes("tag", msg.getTag());
          message.writeInt(msg.getTag().length);
          message.writeBytes(msg.getTag());
          // message.setBytes("data", msg.getData());
          message.writeInt(msg.getData().length);
          message.writeBytes(msg.getData());
          this.messageCount++;
        }
        this.serializeTime += (System.currentTimeMillis() - start);
        /* Clock */
        start = System.currentTimeMillis();
        /* Clock */
        producer.send(message);
        this.sendTime += (System.currentTimeMillis() - start);
        /* Clock */
      }
    } catch (Exception e) {
      LOG.error("[ProducerTool] send loop ", e);
    }
  }

  public void setFailed(boolean isFailed) {
    this.isFailed = isFailed;
  }

  public boolean isNewHostNameAndPort() {
    return newHostNameAndPort;
  }
}
