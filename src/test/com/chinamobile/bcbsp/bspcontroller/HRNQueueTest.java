package com.chinamobile.bcbsp.bspcontroller;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class HRNQueueTest {
	private FCFSQueue fcfsQueue;
	private String priority = "HIGH_WAIT_QUEUE";
	private JobInProgress job = mock(JobInProgress.class);
	private HRNQueue hrnQueue;

	@Test
	public void testHRNQueue() {
		hrnQueue = new HRNQueue(priority);
	    assertEquals("Test HRNQueue Constructor method ", priority, hrnQueue.getName());
		//fail("Not yet implemented");
	}

	@Test
	public void testAddJob() {
		hrnQueue = new HRNQueue(priority);
		hrnQueue.addJob(job);
	}

	@Test
	public void testResortQueue() {
		hrnQueue = new HRNQueue(priority);
		hrnQueue.resortQueue();
	}

	@Test
	public void testRemoveJobJobInProgress() {
		hrnQueue = new HRNQueue(priority);
		hrnQueue.removeJob(job);
	}

	@Test
	public void testRemoveJob() {
		hrnQueue = new HRNQueue(priority);
		hrnQueue.addJob(job);
		hrnQueue.removeJob();
	}
}
