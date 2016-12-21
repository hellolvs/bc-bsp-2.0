package com.chinamobile.bcbsp.sync;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPIds;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPCreateModeImpl;
import com.chinamobile.bcbsp.util.BSPJobID;

import static org.mockito.Mockito.*;

public class WorkerSSControllerTest {

	BSPJobID JID;
	WorkerSSController wc;
	BSPZookeeper bspzk;

	@Before
	public void setUp() throws Exception {
		String jobId = "job_201207241653_0001";
		JID = new BSPJobID().forName(jobId);
		BSPConfiguration bspconf = new BSPConfiguration();
		bspconf.set(Constants.ZOOKEEPER_QUORUM, "192.168.1.3");
		wc = new WorkerSSController(JID, "Slave1.Hadoop", bspconf);
		bspzk = wc.getZooKeeper();
		String node = "/bspRoot";
		this.deleteZKNodes(node);
	}

	@Test
	public void testFirstStageSuperStepBarrier() throws Exception {

		SuperStepReportContainer ssrc = new SuperStepReportContainer();
		ssrc.setDirFlag(new String[] { "1" });

		bspzk.create("/bspRoot", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17),
				new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-ss",
				new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-ss"
				+ "/" + Integer.toString(0), new byte[0],
				BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-ss"
				+ "/" + Integer.toString(0) + "/" + "Slave1.Hadoop",
				new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		boolean answer = wc.firstStageSuperStepBarrier(0, ssrc);
		assertEquals(true, answer);

	}

	@Test
	public void testSecondStageSuperStepBarrier() throws KeeperException,
			InterruptedException {
		SuperStepReportContainer ssrc = new SuperStepReportContainer();
		ssrc.setDirFlag(new String[] { "2" });

		String[] aggValues = { "1", "2", "3" };
		ssrc.setAggValues(aggValues);
		ssrc.setMigrateInfo("abc");
		bspzk.create("/bspRoot", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17),
				new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-sc",
				new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-sc"
				+ "/" + Integer.toString(6), new byte[0],
				BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17)
				+ "-counters", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17)
				+ "-counters" + "/" + Integer.toString(6), new byte[0],
				BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());
		Counters counter = mock(Counters.class);
		when(counter.makeEscapedCompactString()).thenReturn("abcde");

		wc.setcounters(counter);
		boolean answer = wc.secondStageSuperStepBarrier(6, ssrc);
		assertEquals(true, answer);

	}

	@Test
	public void testCheckPointStageSuperStepBarrier() throws KeeperException,
			InterruptedException {

		SuperStepReportContainer ssrc = new SuperStepReportContainer();
		ssrc.setDirFlag(new String[] { "5" });
		ssrc.setStageFlag(6);
		ssrc.setActiveMQWorkerNameAndPorts("Slave1.Hadoop:40000");

		bspzk.create("/bspRoot", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17),
				new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-ss",
				new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-ss"
				+ "/" + Integer.toString(5), new byte[0],
				BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());

		boolean answer = wc.checkPointStageSuperStepBarrier(5, ssrc);
		assertEquals(true, answer);
	}

	@Test
	public void testSaveResultStageSuperStepBarrier() throws KeeperException,
			InterruptedException {

		SuperStepReportContainer ssrc = new SuperStepReportContainer();
		ssrc.setDirFlag(new String[] { "0", "5" });
		ssrc.setStageFlag(6);
		ssrc.setActiveMQWorkerNameAndPorts("Slave1.Hadoop:40000");

		bspzk.create("/bspRoot", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-ss",
				new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-ss"
				+ "/" + Integer.toString(5), new byte[0],
				BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-ss"
				+ "/" + Integer.toString(0), new byte[0],
				BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());
		bspzk.create("/bspRoot" + "/" + JID.toString().substring(17) + "-ss"
				+ "/" + Integer.toString(0) + "/" + "Slave1.Hadoop",
				new byte[0], BSPIds.OPEN_ACL_UNSAFE,
				new BSPCreateModeImpl().getPERSISTENT());
		boolean answer = wc.saveResultStageSuperStepBarrier(5, ssrc);
		assertEquals(true, answer);

	}

	public void deleteZKNodes(String node) throws Exception {
		if (bspzk.equaltoStat(node, false)) {
			List<String> children = this.bspzk.getChildren(node, false);
			if (children.size() > 0) {
				for (String child : children) {
					deleteZKNodes(node + "/" + child);
				}
			}
			this.bspzk
					.delete(node, this.bspzk.exists(node, false).getVersion());
		}
	}

}
