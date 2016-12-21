package com.chinamobile.bcbsp.comm.io.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.examples.bytearray.pagerank.PageRankMessage;

public class MessageSendCacheTest {
  private int partitionNum;
  private MessageSendCache sendCache;
  private MessageBytePoolPerPartition msgBytesPool;
  
  @Before
  public void setUp() throws Exception {
    partitionNum = 5;
  }
  
  @Test
  public void testMessageSendCache() {
    sendCache = new MessageSendCache(partitionNum);
    assertEquals("Use partitionNum to construct MessageSendCache", 5, sendCache.getMsgBytesPoolHandlers().length);   

  }
  
  @Test
  public void testAddMessage() {
    IMessage msg1 = new PageRankMessage();
//    msgBytesPool = mock(MessageBytePoolPerPartition.class);
    sendCache = new MessageSendCache(partitionNum);
//    byte[] byteArray = new byte[3];
//    when(msgBytesPool.getByteArray()).thenReturn(byteArray);
//    when(msgBytesPool.getMsgCount()).thenReturn(2);
    assertEquals("Test AddMessage method.", 8, sendCache.addMessage(msg1, 0));  
    }
  
}
