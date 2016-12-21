
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class ProducerPoolTest {
  private int maxNum;
  private String jobID;
  private ProducerPool producerPool;
  private String hostNameAndPort;
  private int superStepCount;
  private Sender sender;
  private BSPJobID bspJobID;
  private BSPJob job;
  private Communicator comm;
  private MessageQueuesInterface messageQueues;
  
  @Before
  public void setUp() throws Exception {
    maxNum = 2;
    jobID = "job_201407142009_0001";
    hostNameAndPort = "Slave1.Hadoop:3030";
    superStepCount = 2;
    bspJobID = mock(BSPJobID.class);
    bspJobID.forName("job_201407142009_0001");
    job = mock(BSPJob.class);
    when(job.getSendCombineThreshold()).thenReturn(1);
    when(job.getMessagePackSize()).thenReturn(1);
    when(job.getMaxProducerNum()).thenReturn(2);
    comm = mock(Communicator.class);
    when(comm.getJob()).thenReturn(job);
    when(comm.getBSPJobID()).thenReturn(bspJobID);
    messageQueues = new MessageQueuesForMem();
    when(comm.getMessageQueues()).thenReturn(messageQueues);
    when(comm.isSendCombineSetFlag()).thenReturn(false);
    sender = new Sender(comm);
  }
  
  @Test
  public void testProducerPool() {
    producerPool = new ProducerPool(maxNum, jobID);
    assertEquals("Using maxNum, jobID to construct a ProducerPool", 2,
        producerPool.getMaxProducerNum());
  }
  
  @Test
  public void testFinishAll() {
    producerPool = new ProducerPool(maxNum, jobID);
    producerPool.getProducer(hostNameAndPort, 0, sender);
    producerPool.finishAll();
  }
  
  @Test
  public void testCompleteAll() {
    producerPool = new ProducerPool(maxNum, jobID);
    producerPool.getProducer(hostNameAndPort, 0, sender);
    producerPool.completeAll();
  }
  
  @Test
  public void testGetActiveProducerCount() {
    producerPool = new ProducerPool(maxNum, jobID);
    producerPool.getProducer(hostNameAndPort, 0, sender);
    assertEquals("Test getActiveProducerCount method in ProducerPool", 1,
        producerPool.getActiveProducerCount(0));
  }
  
  @Test
  public void testSetActiveProducerProgress() {
    producerPool = new ProducerPool(maxNum, jobID);
    producerPool.getProducer(hostNameAndPort, 0, sender);
    producerPool.setActiveProducerProgress(1);
    assertEquals("Test SetActiveProducerProgress method in ProducerPool", 1,
        producerPool.getActiveProducerCount(1));
  }
  
  @Test
  public void testCleanFailedProducer() {
    producerPool = new ProducerPool(maxNum, jobID);
    producerPool.getProducer(hostNameAndPort, 0, sender).setFailed(true);
    System.out.println(producerPool.getProducer(hostNameAndPort, 0, sender)
        .isFailed());
    assertEquals("Test GetProducer method in ProducerPool", 1,
        producerPool.getActiveProducerCount(0));
    producerPool.cleanFailedProducer();
    assertEquals("Test GetProducer method in ProducerPool", 0,
        producerPool.getProducers().entrySet().size());
  }
  
  @Test
  public void testGetProducerCount() {
    producerPool = new ProducerPool(maxNum, jobID);
    producerPool.getProducer(hostNameAndPort, 0, sender);
    assertEquals("Test GetProducer method in ProducerPool", 1,
        producerPool.getProducerCount());
  }
  
  @Test
  public void testGetProducer() {
    producerPool = new ProducerPool(maxNum, jobID);
    producerPool.getProducer(hostNameAndPort, 0, sender);
    assertEquals("Test GetProducer method in ProducerPool", 1,
        producerPool.getActiveProducerCount(0));
  }
  
}
