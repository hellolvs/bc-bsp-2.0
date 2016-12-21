package com.chinamobile.bcbsp.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class StaffAttemptIDTest {
  private StaffAttemptID staffId;
  
  @Before
  public void setUp() throws Exception {
    staffId = new StaffAttemptID("", 1, 1, 1);
  }
  
  @Test
  public void testStaffAttemptID() {
    staffId = new StaffAttemptID();
    assertEquals("attempt__0000_000000_0", staffId.toString());
  }

  @Test
  public void testStaffAttemptIDStringIntIntInt() {
    staffId = new StaffAttemptID("", 1,1,1);
    assertEquals("attempt__0001_000001_1", staffId.toString().toString());
  }
  
  @Test
  public void testEqualsObject() {
    StaffAttemptID staffId1 = new StaffAttemptID("", 1, 1, 1);
    assertEquals(true, staffId.equals(staffId1));
    
//    StaffAttemptID staffId1 = new StaffAttemptID("", 2, 2, 2);
//    assertEquals(false, staffId.equals(staffId1));
  }

  @Test
  public void testForName() {
    staffId = new StaffAttemptID();
    staffId.forName(staffId.toString());
    assertEquals(0, staffId.getId());
  }

}
