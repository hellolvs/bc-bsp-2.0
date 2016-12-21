
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.Bytes;

public class ProducerToolTest {
  private ProducerTool producerTool;
  private ProducerPool producerPool;
  private int serialNumber;
  private int maxNum;
  private String jobID;
  private ConcurrentLinkedQueue<IMessage> queue;
  private String hostNameAndPort;
  private String subject;
  private Sender sender;
  private Communicator comm;
  private BSPJob job;
  private BSPJobID jobIDObiect;
  private Partitioner<Text> partitioner;
  private int partitionID = 0;
  private BSPConfiguration conf;
  
  @Before
  public void setUp() throws Exception {
    conf = new BSPConfiguration();
    job = new BSPJob(conf, 1);
    job.setMessageQueuesVersion(0);
    jobIDObiect = new BSPJobID();
    partitioner = new HashPartitioner<Text>();
    maxNum = 2;
    jobID = "job_201407142009_0001";
    producerPool = new ProducerPool(maxNum, jobID);
    serialNumber = 2;
    queue = new ConcurrentLinkedQueue<IMessage>();
    hostNameAndPort = "localhost:80";
    subject = "subject";
    comm = new Communicator(jobIDObiect, job, partitionID, partitioner);
    sender = new Sender(comm);
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    queue.add(msg1);
    queue.add(msg2);
  }
  
  @Test
  public void testRun() throws Exception {
    MessageQueuesForMem messageQueues = new MessageQueuesForMem();

    //the msgQueue is to be sent by consumerTool
    ConcurrentLinkedQueue<IMessage> msgQueue = new ConcurrentLinkedQueue<IMessage>();      
    BSPMessage msg1 = new BSPMessage(0, "001", Bytes.toBytes("tags1"),Bytes.toBytes("data1"));
    BSPMessage msg2 = new BSPMessage(0, "001", Bytes.toBytes("tags2"),Bytes.toBytes("data2"));
    BSPMessage msg3 = new BSPMessage(0, "003", Bytes.toBytes("tags3"),Bytes.toBytes("data3"));
    msgQueue.add(msg1);
    msgQueue.add(msg2);
    msgQueue.add(msg3);
    
    assertEquals("Before sending, msgQueue has 3 message.",
            3, msgQueue.size());
    assertEquals("Before receive, messageQueuesInterface has 0 incoming messages.",
            0, messageQueues.getIncomingQueuesSize());
    
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
    producerTool = new ProducerTool(group, 0, msgQueue, hostNameAndPort, "BSP", sender);
    producerTool.setPackSize(500);
    producerTool.addMessages(msgQueue);
    producerTool.setIdle(false);   
    producerTool.start();
    
    ConsumerTool consumerTool = new ConsumerTool(receiver, messageQueues, "localhost-0", "BSP");
    consumerTool.start();
    
    while(true){
        if(messageQueues.getIncomingQueuesSize() == 3){
            when(receiver.getNoMoreMessagesFlag()).thenReturn(true); 
        }
        if(producerTool.isIdle() && receiver.getNoMoreMessagesFlag()){                
            break;
        }
    }     
    while(consumerTool.isAlive()){};
    broker.stop();
    
    assertEquals("After send, msgQueue is empty.",
            0, msgQueue.size());
    assertEquals("After receive, messageQueuesInterface has 3 incoming messages.",
            3, messageQueues.getIncomingQueuesSize());
    
  }
  
  @Test
  public void testProducerTool() {
    producerTool = new ProducerTool(producerPool, serialNumber, queue,
        hostNameAndPort, subject, sender);
    assertEquals(
        "Using producerPool, serialNumber, queue, partithostNameAndPortioner, subject, sender to construct a ProducerTool",
        true, producerTool.isNewHostNameAndPort());
  }
  
  @Test
  public void testAddMessages() {
    producerTool = new ProducerTool(producerPool, serialNumber, queue,
        hostNameAndPort, subject, sender);
    producerTool.setHostNameAndPort("localhost:80");
    assertEquals("test addMessages method in ProducerTool. ", true,
        producerTool.isNewHostNameAndPort());
    producerTool.setHostNameAndPort("localhost:8080");
    assertEquals("test addMessages method in ProducerTool. ", true,
        producerTool.isNewHostNameAndPort());
  }
}
