
package com.chinamobile.bcbsp.bspcontroller;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants.BspControllerRole;
import com.chinamobile.bcbsp.workermanager.WorkerManagerControlInterface;

public class SimpleStaffSchedulerTest {
  private SimpleStaffScheduler simpleStaffScheduler;
  private BspControllerRole role;
//  private WorkerManagerControlInterface controller;
  
  @Before
  public void setUp() throws Exception {
	  
    BSPConfiguration conf = new BSPConfiguration();
    simpleStaffScheduler = new SimpleStaffScheduler();
//    BSPController result = new BSPController(conf, new SimpleDateFormat("yyyyMMddHHmm").format(new Date()));
    BSPController result = new BSPController();
    role = BspControllerRole.NEUTRAL;
    simpleStaffScheduler.setRole(role);
    simpleStaffScheduler.setConf(conf);
    simpleStaffScheduler.setWorkerManagerControlInterface(result);
  }
  
  @Test
  public void testStart() {
	boolean flag = true;
    simpleStaffScheduler.start();
    if(simpleStaffScheduler.getQueueManager().findQueue(
            "HIGHER_WAIT_QUEUE") == null){
    	flag = false;
    }
    assertEquals(true, flag);
  }
  
  @Test
  public void testStop() {
	  simpleStaffScheduler.stop();
	  assertEquals(null, simpleStaffScheduler.getQueueManager());
  }
}
