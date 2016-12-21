
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
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.examples.pagerank.SumCombiner;
import com.chinamobile.bcbsp.graph.GraphDataForMem;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.router.routeparameter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class RPCCommunicatorTest {
  private int partitionID = 0;
  private BSPConfiguration conf;
  private BSPJob job;
  private BSPJobID jobID;
  private Partitioner<Text> partitioner;
  private int dstPartitionID = 1;
  private MessageQueuesInterface messageQueues;
  private IMessage msg;
  private RPCCommunicator comm;
  private HashMap<Integer, String> partiToWorkerManagerNameAndPort;
  private GraphDataInterface graphData = new GraphDataForMem();

  @Before
  public void setUp() throws Exception {
    job = mock(BSPJob.class);
    when(job.getMessageQueuesVersion()).thenReturn(1);
    when(job.getMemoryDataPercent()).thenReturn(0.8f);
    when(job.getBeta()).thenReturn(0.5f);
    when(job.getHashBucketNumber()).thenReturn(10);
    when(job.getJobID()).thenReturn(new BSPJobID("jtIdentifier", 2));
    when(job.getHashBucketNumber()).thenReturn(1);
    when(job.getConf()).thenReturn(new BSPConfiguration());
    jobID = new BSPJobID();
    partitioner = new HashPartitioner<Text>();
    partiToWorkerManagerNameAndPort = new HashMap<Integer, String>();
    partiToWorkerManagerNameAndPort.put(0, "Slave1.Hadoop");
    partiToWorkerManagerNameAndPort.put(1, "Slave2.Hadoop");
  }
  
  @Test
  public void testRPCCommunicator() {
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
    assertEquals(
        "Using jobID, job, partitionID, partitioner to construct a RPCCommunicator",
        0, comm.getSendMessageCounter());
    assertEquals(
        "Using jobID, job, partitionID, partitioner to construct a RPCCommunicator",
        0, comm.getReceivedMessageCounter());
  }
  
  @Test
  public void testInitializeRouteparameterHashMapOfIntegerStringGraphDataInterface() {
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
    routeparameter routeparameter = new routeparameter();
    HashMap<Integer, String> aPartiToWorkerManagerNameAndPort = new HashMap<Integer, String>();
    aPartiToWorkerManagerNameAndPort.put(1, "192.168.0.2:8080");
    GraphDataInterface graphData = new GraphDataForMem();
    comm
        .initialize(routeparameter, aPartiToWorkerManagerNameAndPort, graphData);
    assertEquals(
        "Using routeparameter, aPartiToWorkerManagerNameAndPort, graphData to test initialize method.",
        0, comm.getSendMessageCounter());
  }
  
  @Test
  public void testInitializeRouteparameterHashMapOfIntegerStringInt() {
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
    routeparameter routeparameter = new routeparameter();
    HashMap<Integer, String> aPartiToWorkerManagerNameAndPort = new HashMap<Integer, String>();
    comm.initialize(routeparameter, aPartiToWorkerManagerNameAndPort, 100);
    assertEquals(
        "Using routeparameter, aPartiToWorkerManagerNameAndPort, edgeSize to test initialize method.",
        100, comm.getEdgeSize());
    assertEquals(
        "Using routeparameter, aPartiToWorkerManagerNameAndPort, edgeSize to test initialize method.",
        aPartiToWorkerManagerNameAndPort, comm
            .getPartitionToWorkerManagerNameAndPort());
  }
  
  @Test
  public void testSend() throws IOException {
    partitioner = new HashPartitioner(2);
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
    BSPMessage msg1 = new BSPMessage();
    msg1.setDstVertex("1");
    msg1.setDstPartition(0);
    routeparameter routeparameter = mock(routeparameter.class);
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
    assertEquals("Test send method.", 1, comm.getOutgoMessageCounter());  }
  
  @Test
  public void testGetMessageIterator() {
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
    messageQueues = comm.getMessageQueues();
    msg = new BSPMessage();
    messageQueues.incomeAMessage("001", msg);
    messageQueues.exchangeIncomeQueues();
    int oldIncomedsize = messageQueues.getIncomedQueuesSize();
    try {
      Iterator<IMessage> msgIterator = comm.getMessageIterator("001");
      assertEquals("Test getMessageIterator method.", true, msgIterator
          .hasNext());
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
  public void testStart() {
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
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
      assertEquals("Test getMessageIterator method.", 0, messageQueues
          .getIncomedQueuesSize());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @Test
  public void testExchangeIncomeQueues() {
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
    messageQueues = comm.getMessageQueues();
    comm.start();
    IMessage msg = new BSPMessage();
    messageQueues.incomeAMessage("001", msg);
    assertEquals("Test ExchangeIncomeQueues method.", 1, comm
        .getIncomingQueuesSize());
    comm.exchangeIncomeQueues();
    assertEquals("Test ExchangeIncomeQueues method.", 1, comm
        .getIncomedQueuesSize());
    assertEquals("Test ExchangeIncomeQueues method.", 0, comm
        .getIncomingQueuesSize());
  }
  
  @Test
  public void testGetCombiner() {
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
    job.getConf().setClass(Constants.USER_BC_BSP_JOB_COMBINER_CLASS,
        SumCombiner.class, Combiner.class);
    System.out.println("comm.getCombiner().toString()"
        + comm.getCombiner().getClass().getName());
    assertEquals("Test GetCombiner method.",
        "com.chinamobile.bcbsp.examples.pagerank.SumCombiner", comm
            .getCombiner().getClass().getName());
  }
  
  @Test
  public void testRecoveryForMigrate() {
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
    Map<String, LinkedList<IMessage>> incomedMessages = new HashMap<String, LinkedList<IMessage>>();
    LinkedList<IMessage> value = new LinkedList<IMessage>();
    IMessage msg1 = new BSPMessage();
    value.add(msg1);
    IMessage msg2 = new BSPMessage();
    value.add(msg2);
    incomedMessages.put("001", value);
    comm.recoveryForMigrate(incomedMessages);
    assertEquals("Test recoveryForMigrate method.", 2, comm
        .getIncomedQueuesSize());
  }
  
}
