
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.Bytes;

public class RPCReceiverTest {
  private int partitionID = 0;
  private BSPConfiguration conf;
  private BSPJob job;
  private BSPJobID jobID;
  private Partitioner<Text> partitioner;
  private int dstPartitionID = 1;
  private MessageQueuesInterface messageQueues;
  private IMessage msg;
  private RPCCommunicator comm;
  private RPCReceiver receiver;
  
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
    comm = new RPCCommunicator(jobID, job, partitionID, partitioner);
    
  }
  
  @Test
  public void testRPCReceiver() {
    receiver = new RPCReceiver(comm);
    assertEquals("Use RPCCommunicator to construct  RPCReceiver", 0L, receiver
        .getMessageCount());
  }
  
  @Test
  public void testReceivePackedMessageBSPMessagesPack() {
    ArrayList<IMessage> pack = new ArrayList<IMessage>();
    BSPMessage msg1 = new BSPMessage(0, "001", Bytes.toBytes("tags1"), Bytes
        .toBytes("data1"));
    BSPMessage msg2 = new BSPMessage(0, "001", Bytes.toBytes("tags2"), Bytes
        .toBytes("data2"));
    BSPMessage msg3 = new BSPMessage(0, "003", Bytes.toBytes("tags3"), Bytes
        .toBytes("data3"));
    pack.add(msg1);
    pack.add(msg2);
    pack.add(msg3);
    receiver = new RPCReceiver(comm);
    BSPMessagesPack packedMessages = new BSPMessagesPack();
    packedMessages.setPack(pack);
    assertEquals("Test receivePackedMessage method in RPCReceiver", 0, receiver
        .receivePackedMessage(packedMessages));
  }
  
  @Test
  public void testResetCounters() {
    receiver = new RPCReceiver(comm);
    receiver.resetCounters();
    assertEquals("Test receivePackedMessage method in RPCReceiver", 0L,
        receiver.getMessageCount());
    assertEquals("Test receivePackedMessage method in RPCReceiver", 0L,
        receiver.getMessageBytesCount());
  }
}
