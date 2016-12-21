package com.chinamobile.bcbsp.workermanager;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.sync.WorkerSSController;
import com.chinamobile.bcbsp.sync.WorkerSSControllerInterface;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerAgentForJobTest {
  
  private BSPJobID jobid = new BSPJobID().forName("job_201207241653_0001");
  private int superstepcounter = 2;
  String aWorkerName = "slave1";
  private WorkerSSControllerInterface wsi = (WorkerSSControllerInterface) new WorkerSSController();
  private WorkerAgentForJob wafj =  new WorkerAgentForJob(wsi);
  
  @Before
  public void setUp() throws Exception {  
   wafj.setJobId(jobid);
  }
  
  @Test
  public void testWorkerAgentForJobWorkerSSControllerInterface() {
    WorkerAgentForJob wafjTest=new WorkerAgentForJob(wsi);
    assertEquals(true,wsi.equals(wafj.getWssc()));
  }
  
  @Test
  public void testLocalBarrier() {   
    SuperStepReportContainer ssrc_one = new SuperStepReportContainer();
    ssrc_one.setLocalBarrierNum(2);
    ssrc_one.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
    ssrc_one.setPartitionId(0);
    String staffId_one = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
    assertEquals(false,
        wafj.localBarrier(jobid, staffid_one, superstepcounter, ssrc_one));
    // SuperStepReportContainer ssrc_two=new SuperStepReportContainer();
    // ssrc_two.setLocalBarrierNum(2);
    // ssrc_two.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
    // ssrc_two.setPartitionId(1);
    // String staffId_two = "attempt_201207241653_0001_000002_0";
    // StaffAttemptID staffid_two=new StaffAttemptID().forName(staffId_two);
    // assertEquals(true ,wafj.localBarrier(jobid, staffid_two,
    // superstepcounter, ssrc_two));
  }
  
  @Test
  public void testUpdateStaffsReporter() {

    Map<StaffAttemptID, SuperStepReportContainer> runningStaffInformation = new HashMap<StaffAttemptID, SuperStepReportContainer>();
    String staffId = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffAttemptId = new StaffAttemptID().forName(staffId);
    SuperStepReportContainer ssrc_one = new SuperStepReportContainer();
    runningStaffInformation.put(staffAttemptId, ssrc_one);
    assertEquals(false, wafj.updateStaffsReporter(staffAttemptId));
  }
  
  @Test
  public void testSetWorkerNametoPartitions() {
    
    int partitionId = 2;
    wafj.setWorkerNametoPartitions(jobid, partitionId, aWorkerName);
    assertEquals(true,wafj.getPartitionToWorkerManagerName().containsKey(partitionId));
  }
    
  @Test
  public void testAddStaffReportCounter() {
    wafj.addStaffReportCounter();
    assertEquals(1,wafj.getStaffReportCounter());
  }
  
  @Test
  public void testAddStaffCounter() {
    String staffId = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffAttemptId = new StaffAttemptID().forName(staffId);
    wafj.addStaffCounter(staffAttemptId);
    assertEquals(true,wafj.getRunningStaffInformation().containsKey(staffAttemptId));
  }
  
  @Test
  public void testAddCounters() {
   Counters pCounters= new Counters();
   wafj.addCounters(pCounters);
  }
  
  @Test
  public void testClearStaffRC() {
    wafj.clearStaffRC(jobid);
    assertEquals(0,wafj.getStaffReportCounter());
  } 
}
