package com.chinamobile.bcbsp.comm.io.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.chinamobile.bcbsp.graph.MetaDataOfGraph;

public class MessageCacheTest {
  private int partitionNum;
  private MessageCache cache;
  
  @Before
  public void setUp() throws Exception {
    partitionNum = 5;
  }
  
  @Test
  public void testMessageCache() {
    cache = new MessageCache(partitionNum);
    assertEquals("Use partitionNum to construct MessageCache", 5, cache.getMsgBytesPoolHandlers().length);
  }
  
  @Test
  @Ignore(value = "no value ti compare" )
  public void testRemoveMsgBytePool() {
    cache = new MessageCache(partitionNum);
  }
  
  @Test
  public void testGetTotalCount() {
    cache = new MessageCache(partitionNum);
    cache.initialize(partitionNum);
    assertEquals("Test GetTotalCount method in MessageCache", 0, cache.getTotalCount());
  }
  
  @Test
  public void testGetTotalSize() {
    cache = new MessageCache(partitionNum);
    cache.initialize(partitionNum);
    assertEquals("Test GetTotalCount method in MessageCache", 0, cache.getTotalSize());
  }  
}
