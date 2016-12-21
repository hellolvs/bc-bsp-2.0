package com.chinamobile.bcbsp.bspstaff;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.bspcontroller.JobInProgress;
import com.chinamobile.bcbsp.client.BSPJobClient.RawSplit;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffID;
import com.chinamobile.bcbsp.util.StaffStatus;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

public class StaffInProgressTest {
	StaffInProgress staffinprogress;
	Configuration conf = new Configuration();
	BSPJobID jobid;
	private BSPJob bspJob;
	BSPController bspcontroller = new BSPController();
	JobInProgress job ;
	 private RawSplit rawSplit;
	 WorkerManagerStatus status;
	 StaffAttemptID staffID;
	 StaffID staffid;
	 StaffStatus sstatus;
	 JobStatus jstatus; 
	 @Before
	public void setUp() throws Exception {
		jobid=new BSPJobID("jobid",5);
		job = mock(JobInProgress.class);
		rawSplit = mock(RawSplit.class);
		status = mock(WorkerManagerStatus.class);
		when(status.getWorkerManagerName()).thenReturn("name");
		when(job.getNumAttemptRecovery()).thenReturn(0);
		staffinprogress = new StaffInProgress(jobid,"sjz",bspcontroller,new Configuration(),job,5,rawSplit);
		Path path = new Path("jobfilepath");
		staffid= new StaffID(jobid,5);
		staffID= new StaffAttemptID(staffid,5);
		jstatus = mock(JobStatus.class);
		sstatus = new StaffStatus(jobid,staffID,5,StaffStatus.State.RUNNING,"stateString","workManager",StaffStatus.Phase.STARTING);
		when(job.getStatus()).thenReturn(jstatus);
		when(jstatus.getRunState()).thenReturn(JobStatus.SUCCEEDED);
		//job = new JobInProgress(jobid,path,bspcontroller,conf);
		
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testStaffInProgressBSPJobIDStringInt() {
		staffinprogress = new StaffInProgress(jobid,"sjz",5);
	}

	@Test
	public void testStaffInProgressBSPJobIDStringBSPControllerConfigurationJobInProgressIntRawSplit() {
		staffinprogress = new StaffInProgress(jobid,"sjz",null,new Configuration(),job,5,null);
	}

	@Test
	public void testGetStaffToRunWorkerManagerStatus() {
		try {
			Staff s=staffinprogress.getStaffToRun(status);
			assertEquals(  s.getPartition()==5,true);
		} catch (IOException e) {
			System.out.println(e);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testGetStaffToRunWorkerManagerStatusBoolean() {
		try {
			Staff s=staffinprogress.getStaffToRun(status);
			staffinprogress.getStaffToRun(status,false);
		} catch (IOException e) {
			System.out.println(e);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testGetChangeWorkerState() {
		staffinprogress.setChangeWorkerState(true);
	assertEquals(staffinprogress.getChangeWorkerState(),true);
	staffinprogress.setChangeWorkerState(false);
	assertEquals(staffinprogress.getChangeWorkerState(),false);}

	@Test
	public void testSetChangeWorkerState() {
		staffinprogress.setChangeWorkerState(true);
		assertEquals(staffinprogress.getChangeWorkerState(),true);
		staffinprogress.setChangeWorkerState(false);
		assertEquals(staffinprogress.getChangeWorkerState(),false);
	}
	@Ignore(value="never used")
	@Test
	public void testGetStartTime() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetJob() {
		assertEquals(staffinprogress.getJob()==null,false);
	}

	@Test
	public void testGetSIPId(){
		assertEquals(staffinprogress.getSIPId().getId()==5,true);
	}

	@Test
	public void testGetStaffId() {
		assertEquals(staffinprogress.getStaffId().getId()==5,true);
	}
	@Ignore(value="the Get method")
	@Test
	public void testGetRawSplit() {
		fail("Not yet implemented");
	}
	@Ignore(value="the Get method")
	@Test
	public void testGetWorkerManagerStatus() {
		fail("Not yet implemented");
	}
	@Ignore(value="the Get method")
	@Test
	public void testGetStaffs() {
		fail("Not yet implemented");
	}
	@Ignore(value="the Get method")
	@Test
	public void testGetS() {
		fail("Not yet implemented");
	}
	@Ignore(value="the Get method")
	@Test
	public void testGetStaffID() {
		fail("Not yet implemented");
	}
	@Ignore(value="never used")
	@Test
	public void testIsFirstAttempt() {
		fail("Not yet implemented");
	}

	@Test
	public void testIsRunning() {
		assertEquals(staffinprogress.isRunning(),false);
		

		try {
			Staff s=staffinprogress.getStaffToRun(status);
			assertEquals(  s.getPartition()==5,true);
		} catch (IOException e) {
			System.out.println(e);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals(staffinprogress.isRunning(),true);
	}

	@Test
	public void testIsComplete() {
		staffinprogress.updateStatus(sstatus);
		assertEquals(staffinprogress.isComplete(),false);
		staffinprogress.completed(staffID);
		assertEquals(staffinprogress.isComplete(),true);
		}

	@Test
	public void testIsCompleteStaffAttemptID() {
		staffinprogress.updateStatus(sstatus);
		assertEquals(staffinprogress.isComplete(staffID),false);
		staffinprogress.completed(staffID);
		assertEquals(staffinprogress.isComplete(staffID),true);
	}

	@Test
	public void testShouldCloseForClosedJob() {
		staffinprogress.updateStatus(sstatus);
		assertEquals(staffinprogress.shouldCloseForClosedJob(staffID),true);
	}

	@Test
	public void testCompleted() {
		staffinprogress.updateStatus(sstatus);
		staffinprogress.completed(staffID);
		assertEquals(staffinprogress.isComplete(staffID),true);
	
	}

	@Test
	public void testTerminated() {
		staffinprogress.updateStatus(sstatus);
		staffinprogress.terminated(staffID);
		assertEquals(staffinprogress.isComplete(staffID),false);
	}

	@Test
	public void testUpdateStatus() {
		staffinprogress.updateStatus(sstatus);
		StaffStatus s=staffinprogress.getStaffStatus(staffID);
		assertEquals(s.getStaffId().getId()==5,true);
	}

	@Test
	public void testGetStaffStatus() {

		staffinprogress.updateStatus(sstatus);
		StaffStatus s=staffinprogress.getStaffStatus(staffID);
		assertEquals(s.getStaffId().getId()==5,true);
	
	}

	@Test
	public void testKill() {
		assertEquals(staffinprogress.isFailed(),false);
		staffinprogress.kill();
		assertEquals(staffinprogress.isFailed(),true);
	}

	@Test
	public void testIsFailed() {
		assertEquals(staffinprogress.isFailed(),false);
		staffinprogress.kill();
		assertEquals(staffinprogress.isFailed(),true);
	}

	@Test
	public void testGetStaffToRunForMigrate() {
		try {
			staffinprogress.getStaffToRun(status);
			staffinprogress.getStaffToRunForMigrate(status, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
	}

}
