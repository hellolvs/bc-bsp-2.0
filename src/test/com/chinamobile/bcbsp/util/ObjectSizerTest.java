package com.chinamobile.bcbsp.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.comm.BSPMessage;

public class ObjectSizerTest {
  private ObjectSizer size;
  private ObjectSizer size64;
  @Before
  public void setUp() throws Exception {
    size = ObjectSizer.forSun32BitsVM();
    size64 = ObjectSizer.forSun64BitsVM();
  }
  
  @Test
  public void testObjectSizer() {
    //size = new ObjectSizer((byte) 4, (byte) 8, (byte) 4);
    System.out.println(size.getEmptyArrayVarSize());
  }
  
  @Test
  public void testSizeOfBSPMessage() {
//    assertEquals(86, size.sizeOf(new BSPMessage("1:b:c:d")));
    assertEquals(94, size64.sizeOf(new BSPMessage("1:b:c:d")));
  }
}
