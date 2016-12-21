package com.chinamobile.bcbsp.comm.io.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.Ignore;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.comm.MetaDataOfMessage;
import com.chinamobile.bcbsp.examples.bytearray.pagerank.PRVertexLiteNew;
import com.chinamobile.bcbsp.examples.bytearray.pagerank.PageRankMessage;
import com.chinamobile.bcbsp.graph.MetaDataOfGraph;

public class MessageStoreProcessedTest {
  private MessageStoreProcessed messagetore;
  private int cachePartitionNum;
  private int partitionId;
  private int superstep;
  @Before
  public void setUp() throws Exception {
    cachePartitionNum = 2;
    partitionId = 0;
    superstep = 2;
  }
  
  @Test
  public void testMessageStoreProcessed() {
    messagetore = new MessageStoreProcessed(cachePartitionNum);
    boolean nullflag = (messagetore.getCurrentMessageObjects()==null);
    assertEquals("Use cachePartitionNum to construct MessageStoreProcessed", false, nullflag);

  }
  
  @Test
  @Ignore(value = " messageReceivecCache is 0")
  public void testPreLoadMessages() {
    messagetore = new MessageStoreProcessed(cachePartitionNum);
    MessageBytePoolPerPartition msgBytesPool = mock(MessageBytePoolPerPartition.class);
    byte[] byteArray = new byte[3];
    when(msgBytesPool.getByteArray()).thenReturn(byteArray);
    when(msgBytesPool.getMsgCount()).thenReturn(2);
    messagetore.getmCache().addMessageBytePool(msgBytesPool, 0);
    messagetore.preLoadMessages(partitionId, superstep);    
  }
  
  @Test
  @Ignore(value = " messageReceivecCache is 0")
  public void testAddMsgBytePoolToMsgObjects() {
    messagetore = new MessageStoreProcessed(cachePartitionNum);
    MessageBytePoolPerPartition messages = mock(MessageBytePoolPerPartition.class);
    ExtendedDataOutput bytesPoolHandler = mock(ExtendedDataOutput.class);
    when(messages.getIterator()).thenReturn(new MessageIterator(bytesPoolHandler));
    messagetore.addMsgBytePoolToMsgObjects(messages);
  }
  
  @Test
  public void testRemoveMessagesForOneVertex() {
    messagetore = new MessageStoreProcessed(cachePartitionNum);
    PRVertexLiteNew v = mock(PRVertexLiteNew.class);
    when(v.getVertexID()).thenReturn(1);
    IMessage msg = new PageRankMessage();
    messagetore.getCurrentMessageObjects().put(1, msg);
    System.out.println(messagetore.removeMessagesForOneVertex((String.valueOf(v.getVertexID())), partitionId));
    assertEquals("Test RemoveMessagesForOneVertex method in MessageStoreProcessed", 1, messagetore.removeMessagesForOneVertex((String.valueOf(v.getVertexID())), partitionId).size());

  }
  
  @Test
  @Ignore(value = " no value to compare")
  public void testRefreshMessageBytePools() {
    MetaDataOfMessage.PARTITIONBUCKET_NUMBER = 1;
    MetaDataOfMessage.HASH_NUMBER = 2;
    messagetore = new MessageStoreProcessed(cachePartitionNum);
    PRVertexLiteNew v = mock(PRVertexLiteNew.class);
    when(v.getVertexID()).thenReturn(1);
    IMessage msg = new PageRankMessage();
    messagetore.getCurrentMessageObjects().put(1, msg);
    messagetore.add(null, cachePartitionNum);
    messagetore.refreshMessageBytePools(messagetore);    
  }
  
}
