
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.util.Bytes;

public class BSPMessageTest {
  private int dstPartition;
  private String dstVertex;
  private byte[] tag;
  private byte[] data;
  private static int count = 0;
  private String dstParVertex = null;
  
  @Before
  public void setUp() throws Exception {
    dstPartition = 1;
    dstVertex = "001";
    tag = Bytes.toBytes("tags");
    data = Bytes.toBytes("data");
    dstParVertex = "1:001:data";
  }
  
  @Test
  public void testBSPMessageIntStringByteArrayByteArray() {
    BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
    assertEquals(
        "Using dstPartition, dstVertex, tag, data to construct a BSPMessage",
        data.length, msg.getData().length);
  }
  
  @Test
  public void testBSPMessageStringByteArrayByteArray() {
    BSPMessage msg = new BSPMessage(dstVertex, tag, data);
    assertEquals("Using dstVertex, tag, data to construct a BSPMessage",
        data.length, msg.getData().length);
    assertEquals("Using dstVertex, tag, data to construct a BSPMessage", 0, msg
        .getDstPartition());
  }
  
  @Test
  public void testBSPMessageStringByteArray() {
    BSPMessage msg = new BSPMessage(dstVertex, data);
    assertEquals("Using dstVertex, data to construct a BSPMessage",
        data.length, msg.getData().length);
    assertEquals("Using dstVertex, data to construct a BSPMessage", 0, msg
        .getTag().length);
  }
  
  @Test
  public void testBSPMessageIntStringByteArray() {
    BSPMessage msg = new BSPMessage(dstPartition, dstVertex, data);
    assertEquals(
        "Using dstPartition, dstVertex, data to construct a BSPMessage",
        data.length, msg.getData().length);
    assertEquals(
        "Using dstPartition, dstVertex, data to construct a BSPMessage", 0, msg
            .getTag().length);
  }
  
  @Test
  public void testBSPMessageString() {
    BSPMessage msg = new BSPMessage(dstParVertex);
    assertEquals("Using String to construct a BSPMessage", 1, msg
        .getDstPartition());
    assertEquals("Using String to construct a BSPMessage", data.length, msg
        .getData().length);
    assertEquals("Using String to construct a BSPMessage", dstVertex, msg
        .getDstVertex());
  }
  
  @Test
  public void testIntoString() {
    BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
    assertEquals("testintoString method", "1:001:data", msg.intoString());
  }
  
  @Test
  public void testFromString() {
    BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
    String msgData = "1:001:data";
    msg.fromString(msgData);
    assertEquals("Using String to construct a BSPMessage", dstVertex, msg
        .getDstVertex());
    assertEquals("Using String to construct a BSPMessage", 1, msg
        .getDstPartition());
    assertEquals("Using String to construct a BSPMessage", data.length, msg
        .getData().length);
  }
  
  @Test
  public void testSizeNull() {
    BSPMessage msg = new BSPMessage(dstPartition, dstVertex, data);
    assertEquals("Using size method when tag is null BSPMessage",
        (4 + dstVertex.length() * 2 + data.length), msg.size());
  }
  
  public void testSize() {
    BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
    assertEquals("Using size method when tag is not null BSPMessage", (4
        + dstVertex.length() * 2 + tag.length + data.length), msg.size());
    
  }
  
}
