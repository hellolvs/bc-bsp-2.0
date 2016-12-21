
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class RPCSenderTest {
  private int partitionID = 0;
  private BSPConfiguration conf;
  private BSPJob job;
  private BSPJobID jobID;
  private Partitioner<Text> partitioner;
  private int dstPartitionID = 1;
  private MessageQueuesInterface messageQueues;
  private IMessage msg;
  private RPCCommunicator comm;
  private RPCSender sender;
  
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
    when(job.getReceiveCombineThreshold()).thenReturn(100);
    jobID = new BSPJobID();
    partitioner = new HashPartitioner<Text>();
    comm = mock(RPCCommunicator.class);
    when(comm.getJob()).thenReturn(job);
    when(comm.getBSPJobID()).thenReturn(jobID);
    when(comm.getMessageQueues()).thenReturn(messageQueues);
    when(comm.isSendCombineSetFlag()).thenReturn(false);
  }

  
  @Test
  public void testRPCSender() {
    sender = new RPCSender(comm);
    assertEquals("Use RPCCommunicator to construct RPCSender", 0, sender
        .getSuperStepCounter());
  }
  
  @Test
  public void testBegin() {
    sender = new RPCSender(comm);
    sender.begin(0);
    assertEquals("Test begin method in RPCSender", true, sender
        .isCondition());
  }
}
