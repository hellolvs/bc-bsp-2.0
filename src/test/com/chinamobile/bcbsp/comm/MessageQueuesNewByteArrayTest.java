
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.comm.io.util.MessageBytePoolPerPartition;
import com.chinamobile.bcbsp.comm.io.util.WritableBSPMessages;
import com.chinamobile.bcbsp.examples.bytearray.pagerank.PageRankMessage;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class MessageQueuesNewByteArrayTest {
  private MessageQueuesNewByteArray messageManager;
  private BSPJob job;
  private int partitionID;
  
  @Before
  public void setUp() throws Exception {
    partitionID = 1;
    MetaDataOfMessage.HASH_NUMBER = 10;
    MetaDataOfMessage.PARTITIONBUCKET_NUMBER = 2;
    job = mock(BSPJob.class);
    when(job.getJobID()).thenReturn(new BSPJobID("jtIdentifier", 2));
    when(job.getMessageClass()).thenReturn(PageRankMessage.class);
  }
  
  @Test
  public void testMessageQueuesNewByteArray() {
    messageManager = new MessageQueuesNewByteArray(job, partitionID);
    messageManager.getSingletonBytePartition().getSize();
    assertEquals("Using job, partitionID to construct a MessageQueuesNewByteArray", 0, messageManager.getSingletonBytePartition().getSize());
  }
  
  @Test
  public void testOutgoAMessage() {
    messageManager = new MessageQueuesNewByteArray(job, partitionID);
    IMessage msg = new PageRankMessage();
    assertEquals("Test outgoAMessage method in MessageQueuesNewByteArray", 2, messageManager.outgoAMessage(0, msg));

  }
  
  @Test
  @Ignore(value = "out stream buffer < = 0 2017-07-22")
  public void testIncomeAMessageIntWritableBSPMessagesInt() {
    IMessage msg1 = new PageRankMessage();
    IMessage msg2 = new PageRankMessage();
    msg1.setMessageId(1);
    msg2.setMessageId(2);
    int srcPartitionDstBucket = 0;
    int superstep = 1;
    MessageBytePoolPerPartition messages = new MessageBytePoolPerPartition();
    messages.initialize(10);
    messages.add(msg1);
    messages.add(msg2);
    messageManager = new MessageQueuesNewByteArray(job, partitionID);
    messageManager.incomeAMessage(srcPartitionDstBucket, messages, superstep);
    assertEquals("Test outgoAMessage method in MessageQueuesNewByteArray", 2, MetaDataOfMessage.RMBLength[0] );
  }
  
  @Test
  public void testExchangeIncomeQueues() {
    fail("Not yet implemented");
    MetaDataOfMessage.computeRMBLength();
  }
  
  @Test
  public void testRemoveOutgoingQueue() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testGetOutgoingQueueSize() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testRemoveIncomedQueue() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testLoadBucketMessage() {
    fail("Not yet implemented");
  } 
}
