
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.comm.io.util.MemoryAllocator;
import com.chinamobile.bcbsp.comm.io.util.MessageBytePoolPerPartition;
import com.chinamobile.bcbsp.comm.io.util.WritableBSPMessages;
import com.chinamobile.bcbsp.examples.bytearray.pagerank.PageRankMessage;
import com.chinamobile.bcbsp.graph.GraphDataForMem;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.graph.GraphDataMananger;
import com.chinamobile.bcbsp.graph.MetaDataOfGraph;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.router.routeparameter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class CommunicatorNewTest {
  private static Log LOG = LogFactory.getLog(BSPStaff.class);
  private BSPJob job;
  private BSPJobID jobID;
  private BSPConfiguration conf;
  private Partitioner<Text> partitioner;
  private CommunicatorNew comm;
  private int partitionID;
  private MessageQueuesNewByteArray messageManager;
  private HashMap<Integer, IMessage> currentMessageObjects;
  
  @Before
  public void setUp() throws Exception {
    conf = mock(BSPConfiguration.class);
    jobID = mock(BSPJobID.class);
    jobID.forName("job_201407142009_0001");
    job = mock(BSPJob.class);
    when(job.getJobID()).thenReturn(jobID);
    when(job.getMessageClass()).thenReturn(PageRankMessage.class);
    when(job.getHashBucketNumber()).thenReturn(2);
    when(job.getNumBspStaff()).thenReturn(2);
    partitioner = mock(HashPartitioner.class);
    when(partitioner.getPartitionID(new Text("0"))).thenReturn(0);
    currentMessageObjects = new HashMap<Integer, IMessage>();
  }
  
  @Test
  public void testCommunicatorNew() {
    partitionID = 1;
    comm = new CommunicatorNew(jobID, job, partitionID, partitioner);
    assertEquals(
        "Using jobID, job, partitionID, partitioner to construct a CommunicatorNew",
        1, comm.getPartitionID());
    
  }
  
  @Test
  public void testInitializeRouteparameterHashMapOfIntegerStringGraphDataInterface() {
    routeparameter routeparameter = new routeparameter();
    HashMap<Integer, String> aPartiToWorkerManagerNameAndPort = new HashMap<Integer, String>();
    aPartiToWorkerManagerNameAndPort.put(1, "192.168.0.2:8080");
    GraphDataInterface graphData = new GraphDataMananger();
    comm = new CommunicatorNew(jobID, job, partitionID, partitioner);
    comm.initialize(routeparameter, aPartiToWorkerManagerNameAndPort, graphData);
    assertEquals(
        "Using routeparameter, aPartiToWorkerManagerNameAndPort, graphData to test initialize method.",
        0, comm.getSendMessageCounter());
  }
  
  @Test
  public void testSend() throws IOException {
    PageRankMessage msg1 = mock(PageRankMessage.class);
    when(msg1.getDstVertexID()).thenReturn("1");
    partitioner.setNumPartition(2);
    comm = new CommunicatorNew(jobID, job, partitionID, partitioner);
    comm.send(msg1);
    assertEquals("Test send method in CommunicatorNew", 1,
        MetaDataOfMessage.SENDMSGCOUNTER);
  }
  
  @Test
  public void testStartStringBSPStaff() {
    GraphDataInterface graphData = new GraphDataMananger();
    comm = new CommunicatorNew(jobID, job, partitionID, partitioner);
    comm.setGraphData(graphData);
    BSPStaff bspstaff = new BSPStaff();
    bspstaff.setActiveMQPort(0);
    bspstaff.setCommunicator(comm);
    comm.start("Master.Hadoop", bspstaff);
    assertEquals("Using hostname, BSPStaff to test start method", 5000, MetaDataOfMessage.MESSAGE_SEND_BUFFER_THRESHOLD);
  }
  
  @Test
  public void testExchangeIncomeQueues() {
    job.getJobID().forName("job_201407142009_0001");
    comm = new CommunicatorNew(jobID, job, partitionID, partitioner);
    comm.exchangeIncomeQueues();
  }
  
  @Test
  public void testSendPackedMessageWritableBSPMessagesIntIntInt() {
    job.getJobID().forName("job_201407142009_0001");
    MetaDataOfMessage.PARTITIONBUCKET_NUMBER = 2;
    MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = 6;
    MetaDataOfGraph.initStatisticsPerBucket();
    MetaDataOfGraph.BCBSP_MAX_BUCKET_INDEX = 5;
    MemoryAllocator ma = new MemoryAllocator(job);
    ma.setupOnEachSuperstep(0, LOG);
    comm = new CommunicatorNew(jobID, job, partitionID, partitioner);
    MessageBytePoolPerPartition msgPack = new MessageBytePoolPerPartition();
    msgPack.initialize(10);
    assertEquals("Using hostname, BSPStaff to test sendPackedMessage method", 0,
        comm.sendPackedMessage(msgPack, 0, 1, 1));
  }
  
}
