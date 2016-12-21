
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

public class ReceiverTest {
  private Communicator comm;
  private BSPJobID jobID;
  private BSPJob job;
  private int partitionID = 0;
  private Partitioner<Text> partitioner;
  private Receiver receiver;
  private String brokerName;
  private BSPConfiguration conf;
  
  @Before
  public void setUp() throws Exception {
    conf = new BSPConfiguration();
    job = new BSPJob(conf, 1);
    partitioner = new HashPartitioner<Text>();
    jobID = new BSPJobID();
    brokerName = "broker";
  }
  
  @Test
  public void testRun() throws Exception {
    MessageQueuesInterface msgQueues = new MessageQueuesForMem();
    BSPMessage msg1 = new BSPMessage(0, "001", Bytes.toBytes("tags1"),Bytes.toBytes("data1"));
    BSPMessage msg2 = new BSPMessage(0, "001", Bytes.toBytes("tags2"),Bytes.toBytes("data2"));
    BSPMessage msg3 = new BSPMessage(0, "003", Bytes.toBytes("tags3"),Bytes.toBytes("data3"));
    msgQueues.outgoAMessage("localhost:61616", msg1);
    msgQueues.outgoAMessage("localhost:61616", msg2);
    msgQueues.outgoAMessage("localhost:61616", msg3);
    msgQueues.outgoAMessage("localhost:61616", msg3);
    
    Communicator comm = mock(Communicator.class);
    BSPJob job = mock(BSPJob.class);
    when(job.getSendCombineThreshold()).thenReturn(1);
    when(job.getMessagePackSize()).thenReturn(500); 
    when(job.getMaxProducerNum()).thenReturn(5);
    when(job.getMaxConsumerNum()).thenReturn(5);
    BSPJobID jobid = mock(BSPJobID.class);
    when(jobid.toString()).thenReturn("jobid123");
    
    when(comm.getJob()).thenReturn(job);
    when(comm.getBSPJobID()).thenReturn(jobid);
    when(comm.getMessageQueues()).thenReturn(msgQueues);
    when(comm.isSendCombineSetFlag()).thenReturn(false);

    
    Sender sender = new Sender(comm);
    Receiver receiver = new Receiver(comm, "localhost-0");
    
           
    BrokerService broker = new BrokerService();
    broker.setPersistent(false);
    broker.setBrokerName("localhost-0");
    broker.addConnector("tcp://localhost:61616");
    broker.start();
    
    sender.start();
    receiver.start();
    sender.begin(0);
    
    while(true){
        if(msgQueues.getIncomingQueuesSize() == 4){
            receiver.setNoMoreMessagesFlag(true);
            break;
        }
    }
  }
  
  @Test
  public void testReceiver() {
    comm = new Communicator(jobID, job, partitionID, partitioner);
    receiver = new Receiver(comm, brokerName);
    assertEquals(
        "Using jobID, job, partitionID, partitioner to construct a Receiver",
        comm.isReceiveCombineSetFlag(), receiver.isCombinerFlag());
  }
  
  @Test
  public void testAddMessageCount() {
    comm = new Communicator(jobID, job, partitionID, partitioner);
    receiver = new Receiver(comm, brokerName);
    receiver.addMessageCount(1L);
    assertEquals("Test AddMessageCount method in Receiver", 1L, receiver
        .getMessageCount());
  }
  
  @Test
  public void testAddMessageBytesCount() {
    comm = new Communicator(jobID, job, partitionID, partitioner);
    receiver = new Receiver(comm, brokerName);
    receiver.addMessageBytesCount(32L);
    assertEquals("Test AddMessageBytesCount method in Receiver", 32L, receiver
        .getMessageBytesCount());
  }
  
}
