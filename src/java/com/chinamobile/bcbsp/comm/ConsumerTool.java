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

import java.io.IOException;
import java.util.Iterator;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.ObjectMessage;
import javax.jms.Topic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Consumer tool for receiving messages from Message Queue.
 */
public class ConsumerTool extends Thread implements MessageListener,
    ExceptionListener {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(ConsumerTool.class);
  /** flag of running. */
  private boolean running;
  /** message session. */
  private Session session;
  /** message destination. */
  private Destination destination;
  /** message consumer. */
  private MessageConsumer consumer;
  /** message producer. */
  private MessageProducer replyProducer;
  /** flag of pause before shut down log. */
  private boolean pauseBeforeShutdown = false;
  /** maxim of messages,If >0, end consumer based on max message number. */
  private int maxiumMessages = 0;
  /**
   * Should be jobID. if jobs's no =1, default is OK.
   */
  private String subject = "BSP.DEFAULT";
  /** topic flag. */
  private boolean topic = false;
  // private String user = ActiveMQConnection.DEFAULT_USER;
  // private String password = ActiveMQConnection.DEFAULT_PASSWORD;
  /** url address."failover://vm://brokerName". */
  private String url = null;
  /** flag of transacted. */
  private boolean transacted = false;
  /** flag of durable. */
  private boolean durable = false;
  /** client id. */
  private String clientId;
  /** DUPS_OK_ACKNOWLEDGE of session ActiveMQ. */
  private int ackMode = Session.DUPS_OK_ACKNOWLEDGE;
  /** consumer name. */
  private String consumerName = "BSPConsumer";
  /** sleep time. */
  private long sleepTime = 0;
  /** receice time out set consuming mode to listener. */
  private long receiveTimeOut = 1000;
  /** Default batch size for CLIENT_ACKNOWLEDGEMENT or SESSION_TRANSACTED. */
  private long batch = 500;
  /** received message. */
  private long messagesReceived = 0;
  /** message counter. */
  private long messageCount = 0;
  /** message byte counter. */
  private long messageBytesCount = 0;
  /** message queues. */
  private MessageQueuesInterface messageQueues;
  /** brokerName for this staff to receive messages. */
  private String brokerName;
  /** receiver of communicate. */
  private Receiver receiver = null;
  
  /**
   * Constructor method.
   * @param incomingQueues
   * @param brokerName
   * @param subject
   */
  public ConsumerTool(Receiver aReceiver, MessageQueuesInterface messageQueues,
      String brokerName, String subject) {
    this.receiver = aReceiver;
    this.messageQueues = messageQueues;
    this.brokerName = brokerName;
    this.subject = subject;
  }
  
  /**
   * Show the parameters.
   */
  public void showParameters() {
    LOG.info("Connecting to URL: " + url);
    LOG.info("Consuming " + (topic ? "topic" : "queue") + ": " + subject);
    LOG.info("Using a " + (durable ? "durable" : "non-durable")
        + " subscription");
  }
  
  /** Run of Thread. */
  public void run() {
    try {
      running = true;
      this.url = "failover://vm://" + this.brokerName;
      BSPActiveMQConnFactory connectionFactory =
          new BSPActiveMQConnFactoryImpl();
      connectionFactory.activeMQConnFactoryMethod(url);
      connectionFactory.setOptimizeAcknowledge(true);
      Connection connection = connectionFactory.createConnection();
      if (durable && clientId != null && clientId.length() > 0
          && !"null".equals(clientId)) {
        connection.setClientID(clientId);
      }
      connection.setExceptionListener(this);
      connection.start();
      session = connection.createSession(transacted, ackMode);
      if (topic) {
        destination = session.createTopic(subject);
      } else {
        destination = session.createQueue(subject);
      }
      replyProducer = session.createProducer(null);
      replyProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      if (durable && topic) {
        consumer = session.createDurableSubscriber((Topic) destination,
            consumerName);
      } else {
        consumer = session.createConsumer(destination);
      }
      consumeMessages(connection, session, consumer, receiveTimeOut);
      LOG.info("[ConsumerTool] has received " + this.messagesReceived
          + " object messages from <" + this.subject + ">.");
      LOG.info("[ConsumerTool] has received " + this.messageCount
          + " BSP messages from <" + this.subject + ">.");
      this.receiver.addMessageCount(this.messageCount);
      this.receiver.addMessageBytesCount(messageBytesCount);
    } catch (Exception e) {
      throw new RuntimeException("[ConsumerTool] caught: ", e);
    }
  }
  
  /**
   * Put message into messageQueue and update information.
   */
  public void onMessage(Message message) {
    messagesReceived++;
    try {
      if (message instanceof ObjectMessage) {
        ObjectMessage objMsg = (ObjectMessage) message;
        BSPMessagesPack msgPack = (BSPMessagesPack) objMsg.getObject();
        IMessage bspMsg;
        Iterator<IMessage> iter = msgPack.getPack().iterator();
        while (iter.hasNext()) {
          bspMsg = iter.next();
          String vertexID = bspMsg.getDstVertexID();
          this.messageQueues.incomeAMessage(vertexID, bspMsg);
          this.messageCount++;
          this.messageBytesCount += bspMsg.size();
        }
      } else {
        // Message received is not ObjectMessage.
        LOG.error("[ConsumerTool] Message received is not ObjectMessage!");
      }
      if (message.getJMSReplyTo() != null) {
        replyProducer.send(message.getJMSReplyTo(),
            session.createTextMessage("Reply: " + message.getJMSMessageID()));
      }
      if (transacted) {
        if ((messagesReceived % batch) == 0) {
          LOG.info("Commiting transaction for last " + batch
              + " messages; messages so far = " + messagesReceived);
          session.commit();
        }
      } else if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
        if ((messagesReceived % batch) == 0) {
          LOG.info("Acknowledging last " + batch
              + " messages; messages so far = " + messagesReceived);
          message.acknowledge();
        }
      }
    } catch (JMSException e) {
      throw new RuntimeException("[ConsumerTool] caught: ", e);
    } finally {
      if (sleepTime > 0) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.error("[ConsumerTool] Message received not ObjectMessage!", e);
          throw new RuntimeException("[ConsumerTool] Message received not ObjectMessage! s", e);
        }
      }
    }
  }
  
  /**
   * Put message into messageQueue with serialize method.
   */
  public void onMessageOptimistic(Message message) throws IOException {
    messagesReceived++;
    try {
      if (message instanceof BytesMessage) {
        BytesMessage mapMsg = (BytesMessage) message;
        BSPMessage bspMsg;
        int packSize = mapMsg.readInt();
        int count = 0;
        int partitionID;
        String srcVertexID;
        String dstVertexID;
        byte[] tag;
        byte[] data;
        int tagLen;
        int dataLen;
        while (count < packSize) {
          partitionID = mapMsg.readInt();
          dstVertexID = mapMsg.readUTF();
          tagLen = mapMsg.readInt();
          tag = new byte[tagLen];
          mapMsg.readBytes(tag);
          dataLen = mapMsg.readInt();
          data = new byte[dataLen];
          mapMsg.readBytes(data);
          // bspMsg = new BSPMessage(partitionID, dstVertexID, tag, data);
          // dst is message if it is not null.
          bspMsg = new BSPMessage(partitionID, dstVertexID, tag, data);
          this.messageQueues.incomeAMessage(dstVertexID, bspMsg);
          this.messageCount++;
          this.messageBytesCount += bspMsg.size();
          count++;
        }
      } else {
        // Message received is not ObjectMessage.
        LOG.error("[ConsumerTool] Message received is not BytesMessage!");
      }
      if (message.getJMSReplyTo() != null) {
        replyProducer.send(message.getJMSReplyTo(),
            session.createTextMessage("Reply: " + message.getJMSMessageID()));
      }
      if (transacted) {
        if ((messagesReceived % batch) == 0) {
          LOG.info("Commiting transaction for last " + batch
              + " messages; messages so far = " + messagesReceived);
          session.commit();
        }
      } else if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
        if ((messagesReceived % batch) == 0) {
          LOG.info("Acknowledging last " + batch
              + " messages; messages so far = " + messagesReceived);
          message.acknowledge();
        }
      }
    } catch (JMSException e) {
      LOG.error("[ConsumerTool] caught: ", e);
    } finally {
      if (sleepTime > 0) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.error("[ConsumerTool] caught: ", e);
        }
      }
    }
  }
  
  /** For Exception. */
  public synchronized void onException(JMSException ex) {
    LOG.info("[" + this.getName()
        + "] JMS Exception occured.  Shutting down client.");
    running = false;
  }
  
  /** Flag of running. */
  synchronized boolean isRunning() {
    return running;
  }
  
  /** Comsumer messages and close the connection.
   * @throws IOException,JMSException
   */
  protected void consumeMessagesAndClose(Connection connection,
      Session session, MessageConsumer consumer) throws JMSException,
      IOException {
    LOG.info("[" + this.getName() + "] We are about to wait until we consume: "
        + maxiumMessages + " message(s) then we will shutdown");
    for (int i = 0; i < maxiumMessages && isRunning();) {
      Message message = consumer.receive(1000);
      if (message != null) {
        i++;
        onMessage(message);
      }
    }
    LOG.info("[" + this.getName() + "] Closing connection");
    consumer.close();
    session.close();
    connection.close();
    if (pauseBeforeShutdown) {
      LOG.info("[" + this.getName() + "] Press return to shut down");
      // System.in.read();
    }
  }
  
  /** Comsume messages and close the connection. */
  protected void consumeMessagesAndClose(Connection connection,
      Session session, MessageConsumer consumer, long timeout)
      throws JMSException, IOException {
    LOG.info("[" + this.getName()
        + "] consume messages while continue to be delivered within: "
        + timeout + " ms, and then we will shutdown");
    Message message;
    while ((message = consumer.receive(timeout)) != null) {
      onMessage(message);
    }
    LOG.info("[" + this.getName() + "] Closing connection");
    consumer.close();
    session.close();
    connection.close();
  }
  
  /** Comsume messages. */
  protected void consumeMessages(Connection connection, Session session,
      MessageConsumer consumer, long timeout) throws JMSException, IOException {
    Message message;
    while (!this.receiver.getNoMoreMessagesFlag()) {
      while ((message = consumer.receive(timeout)) != null) {
        onMessageOptimistic(message);
      }
    }
    consumer.close();
    session.close();
    connection.close();
  }
  
  /** Set ackMode state. */
  public void setAckMode(String ackMode) {
    if ("CLIENT_ACKNOWLEDGE".equals(ackMode)) {
      this.ackMode = Session.CLIENT_ACKNOWLEDGE;
    }
    if ("AUTO_ACKNOWLEDGE".equals(ackMode)) {
      this.ackMode = Session.AUTO_ACKNOWLEDGE;
    }
    if ("DUPS_OK_ACKNOWLEDGE".equals(ackMode)) {
      this.ackMode = Session.DUPS_OK_ACKNOWLEDGE;
    }
    if ("SESSION_TRANSACTED".equals(ackMode)) {
      this.ackMode = Session.SESSION_TRANSACTED;
    }
  }
  
  /** set method of clientId. */
  public void setClientId(String clientID) {
    this.clientId = clientID;
  }
  
  /** set method of consumerName. */
  public void setConsumerName(String consumerName) {
    this.consumerName = consumerName;
  }
  
  /** set method of durable. */
  public void setDurable(boolean durable) {
    this.durable = durable;
  }
  
  /** set method of maxiumMessages. */
  public void setMaxiumMessages(int maxiumMessages) {
    this.maxiumMessages = maxiumMessages;
  }
  
  /** set method of pauseBeforeShutdown. */
  public void setPauseBeforeShutdown(boolean pauseBeforeShutdown) {
    this.pauseBeforeShutdown = pauseBeforeShutdown;
  }
  
  /** set method of receiveTimeOut. */
  public void setReceiveTimeOut(long receiveTimeOut) {
    this.receiveTimeOut = receiveTimeOut;
  }
  
  /** set method of sleepTime. */
  public void setSleepTime(long sleepTime) {
    this.sleepTime = sleepTime;
  }
  
  /** set method of subject. */
  public void setSubject(String subject) {
    this.subject = subject;
  }
  
  /** set method of topic. */
  public void setTopic(boolean topic) {
    this.topic = topic;
  }
  
  /** set method of queue. */
  public void setQueue(boolean queue) {
    this.topic = !queue;
  }
  
  /** set method of transacted. */
  public void setTransacted(boolean transacted) {
    this.transacted = transacted;
  }
  
  /** set method of url. */
  public void setUrl(String url) {
    this.url = url;
  }
  
  /** set method of batch. */
  public void setBatch(long batch) {
    this.batch = batch;
  }

  public String getSubject() {
    return subject;
  }

  public long getMessagesReceived() {
    return messagesReceived;
  }

  public void setMessagesReceived(long messagesReceived) {
    this.messagesReceived = messagesReceived;
  }
}
