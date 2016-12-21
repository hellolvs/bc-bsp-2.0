package com.chinamobile.bcbsp.bspstaff;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BSPStaffRunnerTest {
	 private StaffRunner runner = new StaffRunner(null, null, null);
	@Before
	public void setUp() throws Exception {
		 runner.setFaultSSStep(20);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBSPStaffRunner() {
		 assertEquals(runner.getFaultSSStep(), 20);
	}

}
