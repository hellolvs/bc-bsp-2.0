package com.chinamobile.bcbsp.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class StaffIDTest {
  private StaffID staffId;
  
  @Before
  public void setUp() throws Exception {
    staffId = new StaffID("", 1, 1);
  }
  
  @Test
  public void testEqualsObject() {
//    StaffID staffId1 = new StaffID("", 1, 1);
//    System.out.println(staffId);
//    assertEquals(true, staffId.equals(staffId1));
    
    StaffID staffId1 = new StaffID("", 2, 2);
    assertEquals(false, staffId.equals(staffId1));
  }
  
  @Test
  public void testCompareTo() {
    StaffID staffId1 = new StaffID("", 2, 2);
    System.out.println(staffId.compareTo(staffId1));
    assertEquals(-1, staffId.compareTo(staffId1));
  }
  
  @Test
  public void testForName() {
    BSPJobID jobId = new BSPJobID();
    staffId.forName(staffId.toString());
    assertEquals(1, staffId.getId());
  }
  
}
