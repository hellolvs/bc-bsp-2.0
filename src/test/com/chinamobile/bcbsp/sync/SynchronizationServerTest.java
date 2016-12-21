package com.chinamobile.bcbsp.sync;

import static org.junit.Assert.*;

import org.junit.Before;

import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;

public class SynchronizationServerTest {

	SynchronizationServer ss;

	@Before
	public void setUp() throws Exception {
		BSPConfiguration bspconf = new BSPConfiguration();
		bspconf.set(Constants.ZOOKEEPER_QUORUM, "192.168.1.3");
		ss = new SynchronizationServer(bspconf);
	}

	@Test
	public void testStartServer() {

		boolean answer = ss.startServer();
		assertEquals(true, answer);
	}

	@Test
	public void testStopServer() {
		ss.startServer();
		boolean answer = ss.stopServer();
		assertEquals(true, answer);
	}

}
