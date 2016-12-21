package com.chinamobile.bcbsp.comm.io.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

public class MessageReceiveCacheTest {
  private int partitionNum;
  private MessageReceiveCache recevCache;
  private MessageBytePoolPerPartition msgBytesPool;
  @Before
  public void setUp() throws Exception {
    partitionNum = 5;
  }
  
  @Test
  public void testMessageReceiveCache() {
    recevCache = new MessageReceiveCache(partitionNum);
    assertEquals("Use partitionNum to construct MessageCache", 5, recevCache.getMsgBytesPoolHandlers().length);   
  }
  
  @Test
  public void testAddMessageBytePool() {
    msgBytesPool = mock(MessageBytePoolPerPartition.class);
    recevCache = new MessageReceiveCache(partitionNum);
    byte[] byteArray = new byte[3];
    when(msgBytesPool.getByteArray()).thenReturn(byteArray);
    when(msgBytesPool.getMsgCount()).thenReturn(2);
    assertEquals("Test AddMessageBytePool method.", 2, recevCache.addMessageBytePool(msgBytesPool, 0));

  }
  
}
