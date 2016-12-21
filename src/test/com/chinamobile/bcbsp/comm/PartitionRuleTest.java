package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class PartitionRuleTest {
  
  @Before
  public void setUp() throws Exception {
    MetaDataOfMessage.HASH_NUMBER = 10;
  }
  
  @Test
  public void testLocalPartitionRule() {
    assertEquals("Test LocalPartitionRule method.", 9, PartitionRule.localPartitionRule("1"));    
  }
  
}
