package com.chinamobile.bcbsp.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ThreadPoolTest {

	ThreadPool tpool;

	@Before
	public void setUp() throws Exception {
		tpool = new ThreadPool(10);

	}

	@Test
	public void testThreadPool() {
		assertEquals(10, tpool.getToeCount());
	}

	@Ignore
	public void testCleanup() {
		tpool.cleanup();
		assertEquals(0, tpool.getActiveToeCount());
	}

	@Test
	public void testGetActiveToeCount() {
		assertEquals(10, tpool.getActiveToeCount());
	}

	@Test
	public void testGetToeCount() {
		assertEquals(10, tpool.getToeCount());
	}

	@Test
	public void testGetThread() {

		for (int i = 0; i < 10; i++) {
			ThreadSignle ts = tpool.getThread();
			ts.setStatus(true);
		}
		assertNull(tpool.getThread());

	}

	@Ignore
	public void testKillThread() {
		tpool.killThread(tpool.getThread());
	}

}
