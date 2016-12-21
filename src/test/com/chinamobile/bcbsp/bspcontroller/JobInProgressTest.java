package com.chinamobile.bcbsp.bspcontroller;

import static org.junit.Assert.*;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.client.BSPJobClient.RawSplit;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.Constants.BspControllerRole;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.StaffInProgress;
import com.chinamobile.bcbsp.fault.storage.MonitorFaultLog;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFSDataInputStream;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFSDataInputStreamImpl;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffID;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

public class JobInProgressTest {
	private JobInProgress jobInProgress;
	private WorkerManagerStatus wms;
	private BSPConfiguration conf;
	private BSPJobID jobId;

	@Before
	public void setUp() throws Exception {
		jobId = new BSPJobID("-", 1);
		conf = new BSPConfiguration();
		jobInProgress = new JobInProgress();
		wms = new WorkerManagerStatus();
		wms.setWorkerManagerName("slave1");
		jobInProgress.setJobId(jobId);
	}

	@Test
	public void testAddFailedWorker() {
		for (int i = 0; i < 3; i++) {
			if (i < 2) {
				assertEquals(false, jobInProgress.addFailedWorker(wms));
			} else
				assertEquals(true, jobInProgress.addFailedWorker(wms));
		}
	}

	@Test
	public void testFindStaffInProgress() throws IOException {
		ArrayList<WorkerManagerStatus> wmsl = new ArrayList<WorkerManagerStatus>();
		HDFSOperator haLogOperator = new HDFSOperator();

		// haLogOperator.deleteFile(conf.get(Constants.BC_BSP_HA_LOG_DIR));
		 haLogOperator.createFile(conf.get(Constants.BC_BSP_HA_LOG_DIR) +
		 Constants.BC_BSP_HA_SUBMIT_LOG);
		BSPFSDataInputStream bspin = new BSPFSDataInputStreamImpl(
				haLogOperator, conf.get(Constants.BC_BSP_HA_LOG_DIR)
						+ jobId.toString());
		Text loaFactor = new Text();
		System.out.println(bspin.getIn());
		loaFactor.readFields(bspin.getIn());
		while (bspin != null) {
			try {
				WorkerManagerStatus wmStatus = new WorkerManagerStatus();
				wmStatus.readFields(bspin.getIn());
				wmsl.add(wmStatus);
			} catch (EOFException e) {
				bspin = null;
			}
		}
		// Staff t = jip.obtainNewStaff(wmlist, i,
		// Double.parseDouble(loaFactor.toString()));
		jobInProgress.obtainNewStaff(wmsl, 0,
				Double.parseDouble(loaFactor.toString()));
	}

	@Test
	public void testInitStaffs() {
		fail("Not yet implemented");
	}

	@Test
	public void testJobInProgressBSPJobIDPathBSPControllerConfiguration() {
		fail("Not yet implemented");
	}

	@Test
	public void testJobInProgressBSPJobBSPJobIDBSPControllerIntHashMapOfIntegerString() {
		fail("Not yet implemented");
	}

	@Test
	public void testObtainNewStaffCollectionOfWorkerManagerStatusIntDouble()
			throws IOException {
		StaffInProgress staffs[] = new StaffInProgress[1];
		ArrayList<WorkerManagerStatus> wmsl = new ArrayList<WorkerManagerStatus>();

		WorkerManagerStatus wmStatus = new WorkerManagerStatus();
		wmsl.add(wmStatus);
		RawSplit split = new RawSplit();
        split.setClassName("no");
        split.setDataLength(0);
        split.setBytes("no".getBytes(), 0, 2);
        split.setLocations(new String[] {"no"});
        // this staff will not load data from DFS
        staffs[0] = new StaffInProgress(this.jobId, null, new BSPController(), null,
        		jobInProgress, 0, split);
        staffs[0].setSid(new StaffAttemptID());
		jobInProgress.setStaffs(staffs);;
		Text loaFactor = new Text();
		jobInProgress.obtainNewStaff(wmsl, 0, 1);
	}

	@Test
	public void testObtainNewStaffWorkerManagerStatusArrayIntDoubleBoolean() {
		fail("Not yet implemented");
	}

	@Test
	public void testFindWorkerManagerStatus() {
		fail("Not yet implemented");
	}

	@Test
	public void testFindMaxFreeWorkerManagerStatus() {
		fail("Not yet implemented");
	}

	@Test
	public void testUpdateStaffStatus() {
		fail("Not yet implemented");
	}

	@Test
	public void testUpdateStaffs() {
		fail("Not yet implemented");
	}

	@Test
	public void testIsCheckPoint() {
		fail("Not yet implemented");
	}

	@Test
	public void testIsRecovery() {
		fail("Not yet implemented");
	}

	@Test
	public void testGeneralAggregate() {
		fail("Not yet implemented");
	}

	@Test
	public void testGenerateCommand() {
		fail("Not yet implemented");
	}

	@Test
	public void testRemoveStaffFromWorker() {
		fail("Not yet implemented");
	}

	@Test
	public void testCompletedJob() {
		fail("Not yet implemented");
	}

	@Test
	public void testUpdCountersTJobs() {
		fail("Not yet implemented");
	}

	@Test
	public void testFailedJob() {
		fail("Not yet implemented");
	}

	@Test
	public void testKillJob() {
		fail("Not yet implemented");
	}

	@Test
	public void testKillJobRapid() {
		fail("Not yet implemented");
	}

	@Test
	public void testReportLOG() {
		fail("Not yet implemented");
	}

	@Test
	public void testIsCommandBarrier() {
		fail("Not yet implemented");
	}

	@Test
	public void testAddWMNames() {
		fail("Not yet implemented");
	}

	@Test
	public void testCleanWMNames() {
		fail("Not yet implemented");
	}

	@Test
	public void testCleanHaLog() {
		fail("Not yet implemented");
	}

	@Test
	public void testMigrateStaff() {
		fail("Not yet implemented");
	}

	@Test
	public void testObtainNewStaffForMigrate() {
		fail("Not yet implemented");
	}

	@Test
	public void testFindMaxFreeWorkerManagerStatusForMigrate() {
		fail("Not yet implemented");
	}

	@Test
	public void testClearStaffsForJob() {
		fail("Not yet implemented");
	}

	@Test
	public void testIsCleanedWMNs() {
		fail("Not yet implemented");
	}

	@Test
	public void testIsRemovedFromListener() {
		fail("Not yet implemented");
	}

	@Test
	public void testIsCompletedRecovery() {
		fail("Not yet implemented");
	}

}
