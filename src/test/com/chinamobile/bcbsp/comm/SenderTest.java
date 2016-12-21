
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class SenderTest {
  private Sender sender;
  private Communicator comm;
  private BSPJobID jobID;
  private BSPJob job;
  private int partitionID = 1;
  private Partitioner<Text> partitioner;
  private MessageQueuesInterface messageQueues;
  
  @Before
  public void setUp() throws Exception {
    jobID = mock(BSPJobID.class);
    jobID.forName("job_201407142009_0001");
    job = mock(BSPJob.class);
    when(job.getSendCombineThreshold()).thenReturn(1);
    when(job.getMessagePackSize()).thenReturn(1);
    when(job.getMaxProducerNum()).thenReturn(2);
    comm = mock(Communicator.class);
    when(comm.getJob()).thenReturn(job);
    when(comm.getBSPJobID()).thenReturn(jobID);
    messageQueues = new MessageQueuesForMem();
    when(comm.getMessageQueues()).thenReturn(messageQueues);
    when(comm.isSendCombineSetFlag()).thenReturn(false);
  }
  
  @Test
  public void testSender() {
    sender = new Sender(comm);
    assertEquals("Using comm to construct a Sender", 0,
        sender.getSuperStepCounter());
  }
  
  @Test
  public void testBegin() {
    sender = new Sender(comm);
    sender.begin(0);
    assertEquals("Test begin method in RPCSender", true, sender.isCondition());
  }
  
}
