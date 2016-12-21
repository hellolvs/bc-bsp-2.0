
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.graph.GraphDataForDisk;
import com.chinamobile.bcbsp.graph.GraphDataForMem;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.router.HashRoute;
import com.chinamobile.bcbsp.router.routeparameter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class CommunicatorTest {
  private int partitionID = 1;
  private BSPConfiguration conf;
  private BSPJob job;
  private BSPJobID jobID;
  private Partitioner<Text> partitioner;
  private int dstPartitionID = 1;
  private MessageQueuesInterface messageQueues;
  private IMessage msg;
  private Communicator comm;
  private HashMap<Integer, Integer> hashBucketToPartition;
  private HashMap<Integer, String> partiToWorkerManagerNameAndPort;
  private GraphDataInterface graphData = new GraphDataForMem();
  
  @Before
  public void setUp() throws Exception {
    job = mock(BSPJob.class);
    comm = new Communicator(jobID, job, partitionID, partitioner);
    comm.setOutgoMessageBytesCounter(0);
    comm.setOutgoMessageCounter(0);
    comm.setSendMessageCounter(0);
    conf = new BSPConfiguration();
    jobID = new BSPJobID();
    partitioner = new HashPartitioner<Text>(2);
    messageQueues = comm.getMessageQueues();
    msg = new BSPMessage();
    messageQueues.incomeAMessage("001", msg);
    messageQueues.exchangeIncomeQueues();
    hashBucketToPartition = new HashMap<Integer, Integer>();
    partiToWorkerManagerNameAndPort = new HashMap<Integer, String>();
    partiToWorkerManagerNameAndPort.put(0, "Slave1.Hadoop");
    partiToWorkerManagerNameAndPort.put(1, "Slave2.Hadoop");
  }
  
  @Test
  // @Ignore(value = "need zookeeper 2017-07-18")
  public void testCommunicator() throws IOException {
    job = mock(BSPJob.class);
    comm = new Communicator(jobID, job, partitionID, partitioner);
    assertEquals(
        "Using jobID, job, partitionID, partitioner to construct a Communicator",
        0, comm.getSendMessageCounter());
  }
  
  @Test
  public void testInitializeRouteparameterHashMapOfIntegerStringGraphDataInterface() {
    routeparameter routeparameter = new routeparameter();
    HashMap<Integer, String> aPartiToWorkerManagerNameAndPort = new HashMap<Integer, String>();
    aPartiToWorkerManagerNameAndPort.put(1, "192.168.0.2:8080");
    GraphDataInterface graphData = new GraphDataForMem();
    comm.initialize(routeparameter, aPartiToWorkerManagerNameAndPort, graphData);
    assertEquals(
        "Using routeparameter, aPartiToWorkerManagerNameAndPort, graphData to test initialize method.",
        0, comm.getSendMessageCounter());
  }
  
  @Test
  public void testInitializeRouteparameterHashMapOfIntegerStringInt() {
    routeparameter routeparameter = new routeparameter();
    HashMap<Integer, String> aPartiToWorkerManagerNameAndPort = new HashMap<Integer, String>();
    comm.initialize(routeparameter, aPartiToWorkerManagerNameAndPort, 100);
    assertEquals(
        "Using routeparameter, aPartiToWorkerManagerNameAndPort, edgeSize to test initialize method.",
        100, comm.getEdgeSize());
    assertEquals(
        "Using routeparameter, aPartiToWorkerManagerNameAndPort, edgeSize to test initialize method.",
        aPartiToWorkerManagerNameAndPort,
        comm.getPartitionToWorkerManagerNameAndPort());
  }
  
  @Test
  public void testGetMessageIterator() {
    messageQueues = comm.getMessageQueues();
    IMessage msg = new BSPMessage();
    messageQueues.incomeAMessage("001", msg);
    messageQueues.exchangeIncomeQueues();
    int oldIncomedsize = messageQueues.getIncomedQueuesSize();
    try {
      Iterator<IMessage> msgIterator = comm.getMessageIterator("001");
      assertEquals("Test getMessageIterator method.", true,
          msgIterator.hasNext());
      while (msgIterator.hasNext()) {
        assertEquals("Test getMessageIterator method.", msg, msgIterator.next());
      }
      assertEquals("Test getMessageIterator method.", (oldIncomedsize - 1),
          messageQueues.getIncomedQueuesSize());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @Test
  public void testGetMessageQueue() {
    messageQueues = comm.getMessageQueues();
    IMessage msg = new BSPMessage();
    messageQueues.incomeAMessage("001", msg);
    messageQueues.exchangeIncomeQueues();
    ConcurrentLinkedQueue<IMessage> incomedQueue;
    try {
      incomedQueue = comm.getMessageQueue("001");
      assertEquals("Test getMessageIterator method.", msg, incomedQueue
          .iterator().next());
      assertEquals("Test getMessageIterator method.", 1, incomedQueue.size());
      assertEquals("Test getMessageIterator method.", 0,
          messageQueues.getIncomedQueuesSize());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @Test
  public void testSend() throws IOException {
    partitioner = new HashPartitioner(2);
    comm = new Communicator(jobID, job, partitionID, partitioner);
    BSPMessage msg1 = new BSPMessage();
    msg1.setDstVertex("1");
    msg1.setDstPartition(0);
    routeparameter routeparameter = mock(routeparameter.class);
    hashBucketToPartition.put(0, 0);
    hashBucketToPartition.put(1, 1);
    HashMap<Integer, Integer> rangerr = new HashMap<Integer, Integer>();
    rangerr.put(50, 0);
    rangerr.put(100, 1);
    when(routeparameter.getPartitioner()).thenReturn(partitioner);
    when(routeparameter.getHashBucketToPartition()).thenReturn(null);
    when(routeparameter.getRangeRouter()).thenReturn(rangerr);
    comm.initialize(routeparameter, partiToWorkerManagerNameAndPort, graphData);
    comm.setRouteparameter(routeparameter);
    comm.setPartitionID(0);
    comm.send(msg1);
    assertEquals("Test send method.", 1, comm.getIncomingQueuesSize());
    assertEquals("Test send method.", 1, comm.getSendMessageCounter());
    BSPMessage msg2 = mock(BSPMessage.class);
    when(msg2.getDstVertexID()).thenReturn("66");
    msg2.setDstPartition(1);
    comm.send(msg2);
    assertEquals("Test send method.", 1, comm.getOutgoMessageCounter());
  }
  
  @Test
  public void testExchangeIncomeQueues() {
    messageQueues = comm.getMessageQueues();
    IMessage msg = new BSPMessage();
    messageQueues.incomeAMessage("001", msg);
    assertEquals("Test send method.", 1, comm.getIncomingQueuesSize());
    comm.exchangeIncomeQueues();
    assertEquals("Test send method.", 1, comm.getIncomedQueuesSize());
    assertEquals("Test send method.", 0, comm.getIncomingQueuesSize());
  }
  
  @Test
  public void testStart() {
    
  }
  
  @Test
  public void testRecoveryForMigrate() {
    Map<String, LinkedList<IMessage>> incomedMessages = new HashMap<String, LinkedList<IMessage>>();
    LinkedList<IMessage> value = new LinkedList<IMessage>();
    IMessage msg1 = new BSPMessage();
    value.add(msg1);
    IMessage msg2 = new BSPMessage();
    value.add(msg2);
    incomedMessages.put("001", value);
    comm.recoveryForMigrate(incomedMessages);
    assertEquals("Test recoveryForMigrate method.", 2,
        comm.getIncomedQueuesSize());
  }
}
