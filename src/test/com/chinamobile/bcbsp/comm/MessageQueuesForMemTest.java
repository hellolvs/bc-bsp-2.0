
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Before;
import org.junit.Test;

public class MessageQueuesForMemTest {
  private MessageQueuesForMem messageQueues;
  private ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
  private ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> incomedQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
  private ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> incomingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
  
  @Before
  public void setUp() throws Exception {
    messageQueues = new MessageQueuesForMem();
  }
  
  @Test
  public void testClearAllQueues() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    outgoingQueues.put("001", messageQueue);
    incomedQueues.put("001", messageQueue);
    incomingQueues.put("001", messageQueue);
    messageQueues.setIncomedQueues(incomedQueues);
    messageQueues.setIncomingQueues(incomingQueues);
    messageQueues.outgoAMessage("001", msg1);
    messageQueues.outgoAMessage("001", msg2);
    assertEquals("Test clearAllQueues before clear.", 2, messageQueues
        .getIncomedQueuesSize());
    assertEquals("Test clearAllQueues before clear.", 2, messageQueues
        .getIncomingQueuesSize());
    assertEquals("Test clearAllQueues before clear.", 2, messageQueues
        .getOutgoingQueuesSize());
    messageQueues.clearAllQueues();
    assertEquals("Test clearAllQueues before clear.", 0, messageQueues
        .getIncomedQueuesSize());
    assertEquals("Test clearAllQueues before clear.", 0, messageQueues
        .getIncomingQueuesSize());
    assertEquals("Test clearAllQueues before clear.", 0, messageQueues
        .getOutgoingQueuesSize());
  }
  
  @Test
  public void testExchangeIncomeQueues() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    incomingQueues.put("001", messageQueue);
    messageQueues.setIncomingQueues(incomingQueues);
    messageQueues.exchangeIncomeQueues();
    assertEquals("Test exchangeIncomeQueues before clear.", 2, messageQueues
        .getIncomedQueuesSize());
  }
  
  @Test
  public void testGetIncomedQueuesSize() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    incomedQueues.put("001", messageQueue);
    messageQueues.setIncomedQueues(incomedQueues);
    assertEquals("Test getIncomedQueuesSize method", 2, messageQueues
        .getIncomedQueuesSize());
  }
  
  @Test
  public void testGetIncomingQueuesSize() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    incomingQueues.put("001", messageQueue);
    messageQueues.setIncomingQueues(incomingQueues);
    assertEquals("Test getIncomedQueuesSize method", 2, messageQueues
        .getIncomingQueuesSize());
  }
  
  @Test
  public void testGetMaxIncomingQueueIndex() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    incomingQueues.put("001", messageQueue);
    messageQueues.setIncomingQueues(incomingQueues);
    assertEquals("Test getIncomedQueuesSize method", "001", messageQueues
        .getMaxIncomingQueueIndex());
  }
  
  @Test
  public void testGetMaxOutgoingQueueIndex() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.outgoAMessage("001", msg1);
    messageQueues.outgoAMessage("001", msg2);
    assertEquals("Test getIncomedQueuesSize method", "001", messageQueues
        .getMaxOutgoingQueueIndex());
  }
  
  @Test
  public void testGetOutgoingQueuesSize() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.outgoAMessage("001", msg1);
    messageQueues.outgoAMessage("001", msg2);
    assertEquals("Test getOutgoingQueuesSize method", 2, messageQueues
        .getOutgoingQueuesSize());
  }
  
  @Test
  public void testIncomeAMessage() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    incomingQueues.put("001", messageQueue);
    messageQueues.setIncomingQueues(incomingQueues);
    IMessage msg3 = new BSPMessage();
    messageQueues.incomeAMessage("001", msg3);
    assertEquals("Test IncomeAMessage method", 3, messageQueues
        .getIncomingQueuesSize());
  }
  
  @Test
  public void testOutgoAMessage() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.outgoAMessage("001", msg1);
    messageQueues.outgoAMessage("001", msg2);
    assertEquals("Test getOutgoingQueuesSize method", 2, messageQueues
        .getOutgoingQueuesSize());
  }
  
  @Test
  public void testRemoveIncomedQueue() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    incomedQueues.put("001", messageQueue);
    messageQueues.setIncomedQueues(incomedQueues);
    assertEquals("Test removeIncomedQueue method", 2, messageQueues
        .removeIncomedQueue("001").size());
  }
  
  @Test
  public void testGetIncomingQueueSize() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    incomingQueues.put("001", messageQueue);
    messageQueues.setIncomingQueues(incomingQueues);
    assertEquals("Test getIncomingQueueSize method", 2, messageQueues
        .getIncomingQueueSize("001"));
  }
  
  @Test
  public void testRemoveIncomingQueue() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    incomingQueues.put("001", messageQueue);
    messageQueues.setIncomingQueues(incomingQueues);
    assertEquals("Test removeIncomingQueue method", 2, messageQueues
        .removeIncomingQueue("001").size());
  }
  
  @Test
  public void testRemoveOutgoingQueue() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.outgoAMessage("001", msg1);
    messageQueues.outgoAMessage("001", msg2);
    assertEquals("Test removeOutgoingQueue method", 2, messageQueues
        .removeOutgoingQueue("001").size());
  }
  
  @Test
  public void testGetOutgoingQueueSize() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.outgoAMessage("001", msg1);
    messageQueues.outgoAMessage("001", msg2);
    assertEquals("Test getOutgoingQueueSize method", 2, messageQueues
        .getOutgoingQueueSize("001"));
  }
  
  @Test
  public void testGetNextOutgoingQueueIndex() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    messageQueues.outgoAMessage("001", msg1);
    messageQueues.outgoAMessage("001", msg2);
    messageQueues.outgoAMessage("002", msg1);
    assertEquals("Test getNextOutgoingQueueIndex method", "002", messageQueues
        .getNextOutgoingQueueIndex());
  }
  
  @Test
  public void testGetAMessage() {
    IMessage msg1 = new BSPMessage();
    IMessage msg2 = new BSPMessage();
    msg1.setMessageId("001");
    msg2.setMessageId("002");
    ConcurrentLinkedQueue<IMessage> messageQueue = new ConcurrentLinkedQueue<IMessage>();
    messageQueue.add(msg1);
    messageQueue.add(msg2);
    incomingQueues.put("001", messageQueue);
    messageQueues.setIncomingQueues(incomingQueues);
    assertEquals("Test getAMessage method", "001", messageQueues
        .getAMessage().getMessageId());
  }
  
}
