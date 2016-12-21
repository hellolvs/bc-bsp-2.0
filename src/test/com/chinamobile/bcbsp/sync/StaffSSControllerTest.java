package com.chinamobile.bcbsp.sync;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import org.apache.hadoop.ipc.RPC;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;

public class StaffSSControllerTest extends Thread {
	BSPConfiguration bspconf;
	WorkerAgentProtocol umbilical;

	@Before
	public void setUp() throws IOException {

		bspconf = new BSPConfiguration();
		umbilical = (WorkerAgentProtocol) RPC.getProxy(
				WorkerAgentProtocol.class, WorkerAgentProtocol.versionID,
				new InetSocketAddress("192.168.1.2", 65002), bspconf);
		bspconf.set(Constants.ZOOKEEPER_QUORUM, "192.168.1.3");

	}

	@SuppressWarnings({ "static-access", "unused" })
	@Test
	public void testStaffSSController() {
		String jobId = "job_201207241653_0001";
		BSPJobID JID = new BSPJobID().forName(jobId);
		String staffId_one = "attempt_201207241653_0001_000001_0";

		StaffSSController ssc = new StaffSSController(JID,
				new StaffAttemptID().forName(staffId_one), umbilical, bspconf);
	}

	@SuppressWarnings("static-access")
	@Test
	public void testScheduleBarrier() throws IOException, InterruptedException {
		String jobId = "job_201207241653_0001";
		BSPJobID JID = new BSPJobID().forName(jobId);
		String staffId_one = "attempt_201207241653_0001_000001_0";
		StaffSSController ssco = new StaffSSController(JID,
				new StaffAttemptID().forName(staffId_one), umbilical, bspconf);
		SuperStepReportContainer ssrc1 = new SuperStepReportContainer();
		ssrc1.setPort1(60);
		ssrc1.setPort2(61);
		ssrc1.setCheckNum(1);
		ssrc1.setPartitionId(0);
		GeneralSSController gss = new GeneralSSController(JID, bspconf);
		SynchronizationServer ss = new SynchronizationServer(bspconf);
		ss.startServer();
		gss.setup();
		HashMap<Integer, String> abc = ssco.scheduleBarrier(ssrc1);
		assertEquals(1, abc.size());

	}

	@SuppressWarnings("static-access")
	@Test
	public void testLoadDataBarrierSuperStepReportContainerString() {
		String jobId = "job_201207241653_0001";
		BSPJobID JID = new BSPJobID().forName(jobId);
		String staffId_one = "attempt_201207241653_0001_000002_0";
		StaffSSController ssco = new StaffSSController(JID,
				new StaffAttemptID().forName(staffId_one), umbilical, bspconf);
		SuperStepReportContainer ssrc1 = new SuperStepReportContainer();
		ssrc1.setCheckNum(1);
		ssrc1.setPartitionId(0);
		ssrc1.setDirFlag(new String[] { "0" });
		GeneralSSController gss = new GeneralSSController(JID, bspconf);
		SynchronizationServer ss = new SynchronizationServer(bspconf);
		ss.startServer();
		gss.setup();
		boolean abc = ssco.loadDataBarrier(ssrc1, "Hash");
		assertEquals(true, abc);
	}

	@SuppressWarnings("static-access")
	@Ignore
	public void testFirstStageSuperStepBarrier() throws Exception {
		String jobId = "job_201207241653_0001";
		BSPJobID JID = new BSPJobID().forName(jobId);

		String staffId_one = "attempt_201207241653_0001_000002_0";

		StaffSSController ssco = new StaffSSController(JID,
				new StaffAttemptID().forName(staffId_one), umbilical);
		SuperStepReportContainer ssrc1 = new SuperStepReportContainer();
		ssrc1.setCheckNum(1);
		ssrc1.setPartitionId(0);
		ssrc1.setDirFlag(new String[] { "0" });
		GeneralSSController gss = new GeneralSSController(JID, bspconf);
		SynchronizationServer ss = new SynchronizationServer(bspconf);
		ss.startServer();
		gss.setup();
		gss.new ZooKeeperRun().startNextSuperStep(new SuperStepCommand());
		boolean abc = ssco.firstStageSuperStepBarrier(0, ssrc1);
		assertEquals(true, abc);
	}

	@SuppressWarnings("static-access")
	@Test
	public void testSaveResultStageSuperStepBarrier() {
		String jobId = "job_201207241653_0001";
		BSPJobID JID = new BSPJobID().forName(jobId);

		String staffId_one = "attempt_201207241653_0001_000002_0";

		StaffSSController ssco = new StaffSSController(JID,
				new StaffAttemptID().forName(staffId_one), umbilical);
		SuperStepReportContainer ssrc1 = new SuperStepReportContainer();
		ssrc1.setCheckNum(1);
		ssrc1.setPartitionId(0);
		ssrc1.setDirFlag(new String[] { "1" });
		boolean abc = ssco.saveResultStageSuperStepBarrier(0, ssrc1);
		assertEquals(true, abc);
	}

	@SuppressWarnings("static-access")
	@Test
	public void testRangerouter() {
		String jobId = "job_201207241653_0002";
		BSPJobID JID = new BSPJobID().forName(jobId);
		String staffId_one = "attempt_201207241653_0001_000002_0";
		StaffSSController ssco = new StaffSSController(JID,
				new StaffAttemptID().forName(staffId_one), umbilical, bspconf);
		SuperStepReportContainer ssrc1 = new SuperStepReportContainer();
		ssrc1.setCheckNum(1);
		ssrc1.setPartitionId(0);
		ssrc1.setDirFlag(new String[] { "0" });
		HashMap<Integer, Integer> rp = new HashMap<Integer, Integer>();
		rp.put(0, 50);
		ssrc1.setCounter(rp);
		GeneralSSController gss = new GeneralSSController(JID, bspconf);
		SynchronizationServer ss = new SynchronizationServer(bspconf);
		ss.startServer();
		gss.setup();
		HashMap<Integer, Integer> rpp = ssco.rangerouter(ssrc1);
		int range = rpp.get(0);
		assertEquals(50, range);
	}

}
