
package com.chinamobile.bcbsp.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class RandomVariableTest {
  private RandomVariable rand;
  
  @Before
  public void setUp() throws Exception {
    rand = new RandomVariable();
  }
  
  @Test
  public void testRandInt() {
    boolean flag = false;
    
    if (rand.randInt(1, 2) == 1 || rand.randInt(1, 2) == 2) {
      flag = true;
    }
    
    assertEquals(true, flag);
  }
  
  @Test
  public void testRandString() {
    assertEquals(5, rand.randString("abc", 2).length());
  }
  
  @Test
  public void testUniform() {
    boolean flag = false;
    
    if (rand.uniform(1, 2) >= 1 && rand.uniform(1, 2) <= 2) {
      flag = true;
    }
    
    assertEquals(true, flag);
  }
  
  @Test
  public void testDirac() {
    boolean flag = false;
    double[] values = {1.1,1.2,1.3};
    double[] prob = {0.1,0.2,0.4};
    
    if (rand.dirac(values, prob) == 0.0 || rand.dirac(values, prob) == 1.1
        || rand.dirac(values, prob) == 1.2) {
      flag = true;
    }
    assertEquals(true, flag);
  }
}
