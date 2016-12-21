
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.comm.MessageQueuesForDisk.BucketMeta;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class MessageQueuesForDiskTest {
  private MessageQueuesForDisk messageQueues;
  private BSPJob job;
  private BSPConfiguration conf;
  private BSPJobID jobID;
  private int partitionID;
  
  @Before
  public void setUp() throws Exception {
    job = mock(BSPJob.class);
    when(job.getMemoryDataPercent()).thenReturn(0.8f);
    when(job.getBeta()).thenReturn(0.5f);
    when(job.getHashBucketNumber()).thenReturn(10);
    when(job.getJobID()).thenReturn(new BSPJobID("jtIdentifier", 2));
  }
  
  @Test
  public void testMessageQueuesForDisk() {
    messageQueues = new MessageQueuesForDisk(job, 0);
    System.out.println("Job hash number" + job.getHashBucketNumber());
    assertEquals(
        "Using job, messageQueues, partitionID to construct a MessageQueuesForDisk",
        10, messageQueues.getHashBucketNumber());
    assertEquals(
        "Using job, messageQueues, partitionID to construct a MessageQueuesForDisk",
        -1, messageQueues.getCurrentBucket());
  }
  
  @Test
  public void testClearAllQueues() {
    messageQueues = new MessageQueuesForDisk(job, 0);
    messageQueues.setCurrentBucket(10);
    messageQueues.clearAllQueues();
    assertEquals(
        "Using job, messageQueues, partitionID to construct a MessageQueuesForDisk",
        -1, messageQueues.getCurrentBucket());
  }
  
  @Test
  public void testExchangeIncomeQueues() {
    messageQueues = new MessageQueuesForDisk(job, 0);
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.incomeAMessage("001", msg1);
    messageQueues.incomeAMessage("002", msg2);
    messageQueues.exchangeIncomeQueues();
    assertEquals("Using exchangeIncomeQueues method in MessageQueuesForDisk",
        2, messageQueues.getIncomedQueuesSize());
  }
  
  @Test
  public void testGetMaxOutgoingQueueIndex() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
    outgoingQueues.put("001", messageQueue);
    messageQueues = new MessageQueuesForDisk(job, 0);
    messageQueues.setOutgoingQueues(outgoingQueues);
    assertEquals(
        "Test getMaxOutgoingQueueIndex method in MessageQueuesForDisk", "001",
        messageQueues.getMaxOutgoingQueueIndex());
  }
  
  @Test
  public void testGetOutgoingQueuesSize() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
    outgoingQueues.put("001", messageQueue);
    messageQueues = new MessageQueuesForDisk(job, 0);
    messageQueues.setOutgoingQueues(outgoingQueues);
    assertEquals("Test getOutgoingQueuesSize method in MessageQueuesForDisk",
        2, messageQueues.getOutgoingQueuesSize());
  }
  
  @Test
  public void testIncomeAMessage() {
    messageQueues = new MessageQueuesForDisk(job, 0);
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.incomeAMessage("001", msg1);
    messageQueues.incomeAMessage("002", msg2);
    assertEquals("Test IncomeAMessage method in MessageQueuesForDisk", 2,
        messageQueues.getIncomingQueuesSize());
  }
  
  @Test
  public void testOutgoAMessage() {
    messageQueues = new MessageQueuesForDisk(job, 0);
    IMessage msg1 = new BSPMessage();
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
    messageQueues.setOutgoingQueues(outgoingQueues);
    messageQueues.outgoAMessage("001", msg1);
    System.out
        .println("outgoing size " + messageQueues.getOutgoingQueuesSize());
    assertEquals("Test outgoAMessage method in MessageQueuesForDisk", 1,
        messageQueues.getOutgoingQueuesSize());
  }
  
  @Test
  public void testRemoveIncomedQueue() {
    messageQueues = new MessageQueuesForDisk(job, 0);
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.incomeAMessage("001", msg1);
    messageQueues.incomeAMessage("002", msg2);
    messageQueues.exchangeIncomeQueues();
    messageQueues.removeIncomedQueue("001");
    assertEquals("Test RemoveIncomedQueue method in MessageQueuesForDisk", 1,
        messageQueues.getIncomedQueuesSize());
  }
  
  @Test
  public void testRemoveIncomingQueue() {
    messageQueues = new MessageQueuesForDisk(job, 0);
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.incomeAMessage("001", msg1);
    messageQueues.incomeAMessage("002", msg2);
    messageQueues.removeIncomingQueue("001");
    assertEquals("Test RemoveIncomingQueue method in MessageQueuesForDisk", 1,
        messageQueues.getIncomingQueuesSize());
  }
  
  @Test
  public void testRemoveOutgoingQueue() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
    outgoingQueues.put("001", messageQueue);
    messageQueues = new MessageQueuesForDisk(job, 0);
    messageQueues.setOutgoingQueues(outgoingQueues);
    assertEquals("Test outgoAMessage method in MessageQueuesForDisk", 2,
        messageQueues.removeOutgoingQueue("001").size());
  }
  
  @Test
  public void testGetOutgoingQueueSize() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
    outgoingQueues.put("001", messageQueue);
    messageQueues = new MessageQueuesForDisk(job, 0);
    messageQueues.setOutgoingQueues(outgoingQueues);
    assertEquals("Test outgoAMessage method in MessageQueuesForDisk", 2,
        messageQueues.getOutgoingQueueSize("001"));
  }
  
  @Test
  public void testGetNextOutgoingQueueIndex() throws Exception {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue1 = new ConcurrentLinkedQueue<IMessage>();
    messageQueue1.add(msg1);
    messageQueue1.add(msg2);
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
    outgoingQueues.put("001", messageQueue1);
    ConcurrentLinkedQueue<IMessage> messageQueue2 = new ConcurrentLinkedQueue<IMessage>();
    messageQueue2.add(msg1);
    messageQueue2.add(msg2);
    outgoingQueues.put("002", messageQueue2);
    messageQueues = new MessageQueuesForDisk(job, 0);
    messageQueues.setOutgoingQueues(outgoingQueues);
    assertEquals(
        "Test getNextOutgoingQueueIndex method in MessageQueuesForDisk", "002",
        messageQueues.getNextOutgoingQueueIndex());
  }
  
  @Test
  @Ignore(value = "inner class BucketMeta 2017-07-22")
  public void testGetAMessage() {
    fail("Not yet implemented");
  }
  
}
