package com.chinamobile.bcbsp.bspcontroller;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

public class FCFSQueueTest {
	private FCFSQueue fcfsQueue;
	private String priority = "HIGH_WAIT_QUEUE";
	private JobInProgress job = mock(JobInProgress.class);


	@Test
	public void testAddJob() {
		fcfsQueue = new FCFSQueue(priority);
		fcfsQueue.addJob(job);
	}

	@Test
	public void testResortQueue() {
		fcfsQueue = new FCFSQueue(priority);
		fcfsQueue.resortQueue();
	}

	@Test
	public void testRemoveJobJobInProgress() {
		fcfsQueue = new FCFSQueue(priority);
		fcfsQueue.removeJob(job);
	}

	@Test
	public void testRemoveJob() {
		fcfsQueue = new FCFSQueue(priority);
		fcfsQueue.addJob(job);
		fcfsQueue.removeJob();
	}

}
