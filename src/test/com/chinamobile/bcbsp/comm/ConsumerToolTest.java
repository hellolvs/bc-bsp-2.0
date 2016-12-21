
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.BSPActiveMQConnFactory;
import com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.impl.BSPActiveMQConnFactoryImpl;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.Bytes;

import javax.jms.*;

public class ConsumerToolTest {
  
  private Receiver receiver;
  private MessageQueuesInterface messageQueues;
  private String brokerName;
  private String subject;
  private BSPConfiguration conf;
  private BSPJob job;
  private BSPJobID jobID;
  private Partitioner<Text> partitioner;
  private int partitionID = 0;
  private Communicator comm;
  private ConsumerTool consumerTool;
  private MessageConsumer consumer;
  private Destination destination;
  private Session session;
  private MessageProducer replyProducer;
  
  @Before
  public void setUp() throws Exception {
    brokerName = "localhost-0";
    subject = "BSP";
    conf = new BSPConfiguration();
    jobID = new BSPJobID();
    job = new BSPJob(conf, 1);
    partitioner = new HashPartitioner<Text>();
    comm = new Communicator(jobID, job, partitionID, partitioner);
    receiver = new Receiver(comm, brokerName);
    messageQueues = new MessageQueuesForMem();
  }
  
  @Test
  public void testRun() throws Exception {
    ConcurrentLinkedQueue<IMessage> msgQueue = new ConcurrentLinkedQueue<IMessage>();
    BSPMessage msg1 = new BSPMessage(0, "001", Bytes.toBytes("tags1"),
        Bytes.toBytes("data1"));
    BSPMessage msg2 = new BSPMessage(0, "001", Bytes.toBytes("tags2"),
        Bytes.toBytes("data2"));
    BSPMessage msg3 = new BSPMessage(0, "003", Bytes.toBytes("tags3"),
        Bytes.toBytes("data3"));
    msgQueue.add(msg1);
    msgQueue.add(msg2);
    msgQueue.add(msg3);
    Receiver receiver = mock(Receiver.class);
    when(receiver.getNoMoreMessagesFlag()).thenReturn(false);
    
    Sender sender = mock(Sender.class);
    String hostNameAndPort = "localhost:61616";
    BrokerService broker = new BrokerService();
    broker.setPersistent(false);
    broker.setBrokerName("localhost-0");
    broker.addConnector("tcp://localhost:61616");
    broker.start();
    ThreadGroup group = new ThreadGroup("sender");
    ProducerTool producerTool;
    producerTool = new ProducerTool(group, 0, msgQueue, hostNameAndPort, "BSP",
        sender);
    producerTool.setPackSize(500);
    producerTool.addMessages(msgQueue);
    producerTool.setIdle(false);
    producerTool.start();
    ConsumerTool consumerTool = new ConsumerTool(receiver, messageQueues,
        "localhost-0", "BSP");
    consumerTool.start();
    while (true) {
      if (messageQueues.getIncomingQueuesSize() == 3) {
        when(receiver.getNoMoreMessagesFlag()).thenReturn(true);
      }
      if (producerTool.isIdle() && receiver.getNoMoreMessagesFlag()) {
        break;
      }
    }
    while (consumerTool.isAlive()) {
    };
    broker.stop();
    
    assertEquals("After send, msgQueue is empty.", 0, msgQueue.size());
    assertEquals(
        "After receive, messageQueuesInterface has 3 incoming messages.", 3,
        messageQueues.getIncomingQueuesSize());
  }
  
  @Test
  public void testConsumerTool() {
    consumerTool = new ConsumerTool(receiver, messageQueues, brokerName,
        subject);
    assertEquals(
        "Using receiver, messageQueues, brokerName, subject to construct a ConsumerTool",
        "BSP", consumerTool.getSubject());
  }
  
  @Test
  public void testOnMessageOptimistic() throws Exception {   
    Receiver receiver = mock(Receiver.class);
    when(receiver.getNoMoreMessagesFlag()).thenReturn(false);
    ConsumerTool consumerTool = new ConsumerTool(receiver, messageQueues,
        "localhost-0", "BSP");
    BytesMessage message = mock(BytesMessage.class);
    when(message.readInt()).thenReturn(1);
    when(message.readUTF()).thenReturn("001");    
    consumerTool.onMessageOptimistic(message);
    assertEquals("Test OnMessageOptimistic method in ConsumerTool", 1,
        consumerTool.getMessagesReceived());
  }
}
