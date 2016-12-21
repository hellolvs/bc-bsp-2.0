package com.chinamobile.bcbsp.bspcontroller;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.security.auth.login.Configuration;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants.BspControllerRole;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class AdjustPriorityTest {
	  private BSPConfiguration conf;
	  private QueueManager queueManager;
	 // private BspControllerRole role = BspControllerRole.NEUTRAL;
	  private JobInProgress jip;
	  private String priority = null;
	  private AdjustPriority adjustpriority;
	@Before
	  public void setUp() throws Exception {
		priority = "HIGH_WAIT_QUEUE";
	    conf =new BSPConfiguration();
	    queueManager = new QueueManager(conf);
	    jip = mock(JobInProgress.class);
	    when(jip.getQueueNameFromPriority()).thenReturn("HIGHER_WAIT_QUEUE");
	    when(jip.getHRN()).thenReturn(10.0);
	  }
	@Test
	public void testAdjustPriority() {
		adjustpriority = new AdjustPriority(queueManager);
        adjustpriority.adjustPriority(jip, priority);
		
	}

	@Test
	public void testResortQueue() {
		queueManager.createHRNQueue("HIGHER_WAIT_QUEUE");
		queueManager.createHRNQueue("HIGH_WAIT_QUEUE");
		queueManager.createHRNQueue("NORMAL_WAIT_QUEUE");
		queueManager.createHRNQueue("LOW_WAIT_QUEUE");
		queueManager.createHRNQueue("LOWER_WAIT_QUEUE");
		queueManager.addJob(priority, jip);
		adjustpriority = new AdjustPriority(queueManager);
		adjustpriority.resortQueue();
		//fail("Not yet implemented");
	}

}
