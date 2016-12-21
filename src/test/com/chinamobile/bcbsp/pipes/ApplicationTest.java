package com.chinamobile.bcbsp.pipes;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;

public class ApplicationTest {
	Configuration conf = new Configuration();
	BSPConfiguration bspconf = new BSPConfiguration();
	BSPJob bspjob ;
	BSPStaff staff;
	StaffAttemptID staffid;
	WorkerAgentProtocol workerAgent;
	@Before
	public void setUp() throws Exception {
		bspjob=mock(BSPJob.class);
		staff=mock(BSPStaff.class);
		workerAgent=mock(WorkerAgentProtocol.class);
		staffid=mock(StaffAttemptID.class);
		conf.set("bcbsp.log.dir", "/root/Desktop");
		when(bspjob.getConf()).thenReturn( conf);
		when(staff.getStaffID()).thenReturn(staffid);
		when(staffid.toString()).thenReturn("123456");
		when(staff.getJobExeLocalPath()).thenReturn("/root/Desktop/BspMerge");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testApplicationBSPJobStringString() throws IOException, InterruptedException {
		Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		assertEquals("application:create a runner", application==null,false);
		//fail("Not yet implemented");
	}

	@Test
	public void testApplicationBSPJobStaffWorkerAgentProtocolString() throws IOException, InterruptedException {
		Application application = new Application(bspjob,staff,workerAgent,"WorkerManager");
		assertEquals("application:create a runner", application==null,false);
		//fail("Not yet implemented");
	}

	@Test
	public void testRunClient() throws IOException {
		 Map<String, String> env = new HashMap<String, String>();
		 List<String> cmd = new ArrayList<String>();
		String executable="/root/Desktop/BspMerge";
		cmd.add(executable);
		Process process;
		Application application=mock(Application.class);
		 process = application.runClient(cmd, env);
		 assertEquals("RunClient:create a runner", application==null,false);
		//fail("Not yet implemented");
	}

	@Test
	public void testGetDownlink() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetPartitionId() {
		fail("Not yet implemented");
	}

	@Test
	public void testWaitForFinish() {
		fail("Not yet implemented");
	}

	@Test
	public void testAbort() {
		fail("Not yet implemented");
	}

	@Test
	public void testCleanup() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetCommunicator() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetHandler() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetHandler() {
		fail("Not yet implemented");
	}

}
