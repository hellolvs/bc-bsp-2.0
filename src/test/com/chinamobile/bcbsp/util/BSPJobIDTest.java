package com.chinamobile.bcbsp.util;

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class BSPJobIDTest {
  private BSPJobID jobId;
  private ID id;
  private String jtIdentifier = "-";
  
  @Before
  public void setUp() throws Exception {
    jobId = new BSPJobID(jtIdentifier, 1);
//    jtIdentifier = jobId.getJtIdentifier();
  }
  
  @Test
  public void testBSPJobID() {
    jobId = new BSPJobID();
    System.out.println(jobId);
    System.out.println(jobId.getJtIdentifier());
  }
  
  @Test
  public void testBSPJobIDStringInt() {
    jobId = new BSPJobID(jtIdentifier, 1);
    System.out.println(jobId);
    System.out.println(jobId.getJtIdentifier().getClass());
    System.out.println((new Text(jtIdentifier)).getClass());
  }
  
  @Test
  public void testEqualsObject() {
    
//    BSPJobID jobId1 = new BSPJobID(jtIdentifier, 1);
//    System.out.println(jobId.equals(jobId1));
//    assertEquals(true, jobId.equals(jobId1));
    
    BSPJobID jobId1 = new BSPJobID(jtIdentifier, 2);
    System.out.println(jobId.equals(jobId1));
    assertEquals(false, jobId.equals(jobId1));
  }
  
  @Test
  public void testCompareTo() {
    BSPJobID jobId1 = new BSPJobID(jtIdentifier, 2);
    StaffID staffId = new StaffID(jobId1, 1);
    System.out.println(jobId.compareTo(staffId.getJobID()));
    assertEquals(-1, jobId.compareTo(staffId.getJobID()));
  }
  
  @Test
  public void appendTo() {
    System.out.println(jobId);
    assertEquals("job_-_0001", jobId.appendTo(new StringBuilder("job")).toString());
  }
  
  @Test
  public void testForName() {
    jobId = new BSPJobID();
    System.out.println(jobId);
    jobId.forName(jobId.toString());
    assertEquals(0, jobId.getId());
  }
  
}
