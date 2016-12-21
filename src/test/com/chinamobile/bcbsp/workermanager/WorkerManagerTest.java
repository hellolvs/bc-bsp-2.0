package com.chinamobile.bcbsp.workermanager;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.TestUtil;
import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.action.LaunchStaffAction;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.controllerProtocol.ControllerProtocolServer;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.rpc.ControllerProtocol;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.sync.WorkerSSControllerInterface;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPIds;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPCreateModeImpl;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffStatus;
import com.chinamobile.bcbsp.workermanager.WorkerManager.RunningJob;
import com.chinamobile.bcbsp.workermanager.WorkerManager.StaffInProgress;

public class WorkerManagerTest {
  WorkerManager wm = null;
  BSPConfiguration conf = null;
  
  @Before
  public void setUp() throws Exception {
    conf = new BSPConfiguration();
    conf.set(Constants.BC_BSP_CONTROLLER_ADDRESS, "192.168.1.2");
    conf.set(Constants.ZOOKEEPER_QUORUM, "192.168.1.3");
    conf.set(Constants.ZOOKEPER_CLIENT_PORT, "2181");
    conf.set(Constants.BSPCONTROLLER_STANDBY_LEADER, "/leader/");
    wm = new WorkerManager(conf);
   
  }
  
  @Test
  public void testWorkerManager() throws IOException {
    WorkerManager wmtest=new WorkerManager(conf);
    String port=wmtest.getConf().get(Constants.ZOOKEPER_CLIENT_PORT);
    assertEquals("2181",port);
  }
  
  @Test
  public void testDispatch() throws IOException {
    BSPJobID jobID = new BSPJobID();
    Directive directive = new Directive();
    assertEquals(true, wm.dispatch(jobID, directive, true, true, 0));
  }
  
  @Test
  public void testGetLocalDirs() throws IOException {
    WorkerManager wm1 = new WorkerManager(conf);
    assertEquals(conf.getStrings(Constants.BC_BSP_LOCAL_DIRECTORY),
        wm1.getLocalDirs());
  }
  
  @Test
  public void testDeleteLocalFiles() throws IOException {
    File path = new File("/root/usr/test");
    File dir = new File(path, "hello.txt");
    conf.set(Constants.BC_BSP_LOCAL_DIRECTORY, "/root/usr/test/hello.txt");
    WorkerManager wm = new WorkerManager(conf);
    wm.deleteLocalFiles();
    assertEquals(false, dir.exists());
  }
  
  @Test
  public void testDeleteLocalDir() throws IOException {
    File path = new File("/root/usr/test");
    conf.set(Constants.BC_BSP_LOCAL_DIRECTORY, "/root/usr/test");
    wm.deleteLocalFiles();
    assertEquals(false, path.exists());
  }
  
  @Test
  public void testDeleteLocalFilesString() throws IOException {
    File path = new File("/root/usr/test");
    File dir = new File(path, "hello.txt");
    conf.set(Constants.BC_BSP_LOCAL_DIRECTORY, "/root/usr/test/hello.txt");
    WorkerManager wm = new WorkerManager(conf);
    String[] localDirs = conf.getStrings(Constants.BC_BSP_LOCAL_DIRECTORY);
    wm.deleteLocalFiles("/root/usr/test/hello.txt");
    assertEquals(false, dir.exists());
  }
  
  @Test
  public void testCleanupStorage() throws IOException {
    File path = new File("/root/usr/test/");
    File dir = new File(path, "hello.txt");
    conf.set(Constants.BC_BSP_LOCAL_DIRECTORY, "/root/usr/test/hello.txt");
    WorkerManager wm = new WorkerManager(conf);
    String[] localDirs = conf.getStrings(Constants.BC_BSP_LOCAL_DIRECTORY);
    wm.cleanupStorage();
    assertEquals(false, dir.exists());
  }
  
  @Test
  public void testUpdateStaffStatistics() throws Exception {
    wm.setCurrentStaffsCount(2);
    wm.setFinishedStaffsCount(2);
    
    Map<StaffAttemptID, WorkerManager.StaffInProgress> finishedStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    
    Map<BSPJobID, RunningJob> runningJobs = new ConcurrentHashMap<BSPJobID, RunningJob>();
    Map<BSPJobID, RunningJob> finishedJobs = new ConcurrentHashMap<BSPJobID, RunningJob>();
    Map<BSPJobID, WorkerAgentForJob> runningJobtoWorkerAgent = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    
    BSPJobID bspJobId1 = new BSPJobID();
    BSPJobID bspJobId2 = new BSPJobID();
    
    RunningJob rJob1 = new RunningJob();
    RunningJob rJob2 = new RunningJob();
    rJob1.setStaffCounter(2);
    rJob2.setStaffCounter(2);
    
    StaffAttemptID staffID1_1 = new StaffAttemptID(bspJobId1.getJtIdentifier(),
        bspJobId1.getId(), 0, 0);
    StaffAttemptID staffID1_2 = new StaffAttemptID(bspJobId1.getJtIdentifier(),
        bspJobId1.getId(), 0, 0);
    StaffAttemptID staffID2_1 = new StaffAttemptID(bspJobId2.getJtIdentifier(),
        bspJobId1.getId(), 0, 0);
    StaffAttemptID staffID2_2 = new StaffAttemptID(bspJobId2.getJtIdentifier(),
        bspJobId1.getId(), 0, 0);
    WorkerManager.StaffInProgress sip1 = wm.new StaffInProgress();
    WorkerManager.StaffInProgress sip2 = wm.new StaffInProgress();
    finishedStaffs.put(staffID1_1, sip1);
    finishedStaffs.put(staffID1_2, sip2);
    wm.setFinishedStaffs(finishedStaffs);
    
    runningJobs.put(bspJobId1, rJob1);
    wm.setRunningJobs(runningJobs);
    
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    BSPJobID jobID = new BSPJobID();
    StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
        jobID.getId(), 0, 0);
    
    runningJobtoWorkerAgent.put(bspJobId2, wafj);
    wm.setRunningJobtoWorkerAgent(runningJobtoWorkerAgent);
    
    wm.updateStaffStatistics(bspJobId2);
    assertEquals(1, (int) wm.getCurrentStaffsCount());
    assertEquals(3, (int) wm.getFinishedStaffsCount());
    assertEquals(1, (int) wm.getRunningJobs().get(bspJobId2).getStaffCounter());
  }
 
//  @Test
//  public void testIsRunning() {
//    Boolean running = true;
//    assertEquals(true, wm.isRunning());
//  }
  @Test
  public void testGetStaff() throws IOException {
    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    String staffId_one = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
    LaunchStaffAction las = new LaunchStaffAction();
    Staff stf = las.getStaff();
    BSPJob bspjob = new BSPJob(conf, 2);
    String workerManagerName = "slave1";
    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
    runningStaffs.put(staffid_one, sip);
    wm.setRunningStaffs(runningStaffs);
    assertEquals(stf, wm.getStaff(staffid_one));
    
  }
  
  @Test
  public void testGetStaffRecoveryState() {
    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    String staffId_one = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
    Fault fault = new Fault();
    runningStaffs.put(staffid_one, sip);
    runningStaffs.get(staffid_one).setStaffStatus(1, fault);
    wm.setRunningStaffs(runningStaffs);
    assertEquals(false, wm.getStaffRecoveryState(staffid_one));
  }
  
  @Test
  public void testGetStaffChangeWorkerState() {
    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    String staffId_one = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
    runningStaffs.put(staffid_one, sip);
    wm.setRunningStaffs(runningStaffs);
    assertEquals(false, wm.getStaffChangeWorkerState(staffid_one));
  }
  
  @Test
  public void testGetFailCounter() {
    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    String staffId_one = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
    runningStaffs.put(staffid_one, sip);
    wm.setRunningStaffs(runningStaffs);
    assertEquals(0, wm.getFailCounter(staffid_one));
    
    sip.setFailCounter(2);
    assertEquals(2, wm.getFailCounter(staffid_one));
  }
  
  @Test
  public void testGetWorkerManagerNameBSPJobIDStaffAttemptID() {
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    try {
      TestUtil.set(wm, "runningJobtoWorkerAgent", runningJobs);
      TestUtil.set(wafj, "workerManagerName", "Test");
    } catch (Exception e) {
      e.printStackTrace();
    }
    BSPJobID jobID = new BSPJobID();
    StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
        jobID.getId(), 0, 0);
    runningJobs.put(jobID, wafj);
    String workerManagerName = wm.getWorkerManagerName(jobID, staffID);
    assertEquals(true, "Test".equals(workerManagerName));
  }
 
  @Test
  public void testAddCounters() {
    // need to change
    Counters counters = new Counters();
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    BSPJobID jobID = new BSPJobID();
    StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
        jobID.getId(), 0, 0);
    wafj.setNumberWorkers(jobID, staffID, 2);
    Map<BSPJobID, WorkerAgentForJob> runningJobstoWorkerAgent = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    runningJobstoWorkerAgent.put(jobID, wafj);
    wm.setRunningJobtoWorkerAgent(runningJobstoWorkerAgent);
    wm.addCounters(jobID, counters);
    assertEquals(true, counters.equals(wafj.getCounters()));   
  }
  
  @Test
  public void testGetNumberWorkers() {
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    BSPJobID jobID = new BSPJobID();
    StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
        jobID.getId(), 0, 0);
    wafj.setNumberWorkers(jobID, staffID, 2);
    Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    runningJobs.put(jobID, wafj);
    try {
      TestUtil.set(wm, "runningJobtoWorkerAgent", runningJobs);
    } catch (Exception e) {
      e.printStackTrace();
    }
    int num = wm.getNumberWorkers(jobID, staffID);
    assertEquals(2, num);
  }
  
  @Test
  public void testSetNumberWorkers() {
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    BSPJobID jobID = new BSPJobID();
    StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
        jobID.getId(), 0, 0);
    wafj.setNumberWorkers(jobID, staffID, 0);
    Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    runningJobs.put(jobID, wafj);
    try {
      TestUtil.set(wm, "runningJobtoWorkerAgent", runningJobs);
    } catch (Exception e) {
      e.printStackTrace();
    }
    int num = wm.getNumberWorkers(jobID, staffID);
    assertEquals(0, num);
    
    wm.setNumberWorkers(jobID, staffID, 2);
    num = wm.getNumberWorkers(jobID, staffID);
    assertEquals(2, num);
  }
  
  @Test
  public void testAddStaffReportCounter() {
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    BSPJobID jobID = new BSPJobID();
    @SuppressWarnings("unused")
    StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
        jobID.getId(), 0, 0);
    Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    runningJobs.put(jobID, wafj);
    
    wm.setRunningJobtoWorkerAgent(runningJobs);
    wafj.setStaffReportCounter(2);
    
    wm.addStaffReportCounter(jobID);
    assertEquals(3, (int) wafj.getStaffReportCounter());
    
  }
  
  @Test
  public void testSetStaffStatus() {
    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    String staffId_one = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
    Fault fault = new Fault();
    runningStaffs.put(staffid_one, sip);
    wm.setRunningStaffs(runningStaffs);
    wm.setStaffStatus(staffid_one, 1, fault, 1);
    assertEquals(
        true,
        "RUNNING".equals(wm.getRunningStaffs().get(staffid_one).getStatus()
            .getRunState().toString()));
  }
  
  @Test
  public void testGetStaffStatus() {
    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    String staffId_one = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
    BSPJobID jobID = new BSPJobID();
    StaffStatus stfs = new StaffStatus(jobID, staffid_one, 0,
        StaffStatus.State.UNASSIGNED, "running", "slave1",
        StaffStatus.Phase.STARTING);
    sip.setStaffStatus(stfs);
    runningStaffs.put(staffid_one, sip);
    wm.setRunningStaffs(runningStaffs);
    String state = wm.getStaffStatus(staffid_one).getStateString();
    assertEquals(true, "running".equals(state));
  }
  
  @Test
  public void testSetWorkerNametoPartitions() {
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    BSPJobID jobID = new BSPJobID();
    @SuppressWarnings("unused")
    StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
        jobID.getId(), 0, 0);
    Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    runningJobs.put(jobID, wafj);
    HashMap<Integer, String> partitionToWorkerManagerName = null;
    int port = 0;
    try {
      partitionToWorkerManagerName = (HashMap<Integer, String>) TestUtil.get(
          wafj, "partitionToWorkerManagerName");
      TestUtil.set(wm, "runningJobtoWorkerAgent", runningJobs);
      port = (Integer) TestUtil.get(wafj, "portForJob");
    } catch (Exception e) {
      e.printStackTrace();
    }
    wm.setWorkerNametoPartitions(jobID, 5, "TEST");
    
    String check = partitionToWorkerManagerName.get(5);
    String shouldBe = "TEST:" + port;
    assertEquals(true, check.equals(shouldBe));
  }
  
  @Test
  public void testGetHostName() {
    String hostNameTest = wm.getHostName();
    String hostName = conf.get(Constants.BC_BSP_WORKERAGENT_HOST,
        Constants.DEFAULT_BC_BSP_WORKERAGENT_HOST);
    assertEquals(true, hostName.equals(hostNameTest));
  }
  
  @Test
  public void testClearFailedJobList() {
    ArrayList<BSPJobID> failedJobList = new ArrayList<BSPJobID>();
    BSPJobID jobID_one = new BSPJobID();
    BSPJobID jobID_two = new BSPJobID();
    failedJobList.add(jobID_one);
    failedJobList.add(jobID_two);
    wm.setFailedJobList(failedJobList);
    wm.clearFailedJobList();
    assertEquals(0, wm.getFailedJobList().size());
  }
  
  @Test
  public void testAddFailedJob() {
    ArrayList<BSPJobID> failedJobList = new ArrayList<BSPJobID>();
    BSPJobID jobID_one = new BSPJobID();
    failedJobList.add(jobID_one);
    wm.setFailedJobList(failedJobList);
    assertEquals(1, wm.getFailedJobList().size());
    
    BSPJobID jobID_two = new BSPJobID();
    wm.addFailedJob(jobID_two);
    assertEquals(2, wm.getFailedJobList().size());
  }
  
  @Test
  public void testGetFailedJobCounter() {
    ArrayList<BSPJobID> failedJobList = new ArrayList<BSPJobID>();
    BSPJobID jobID_one = new BSPJobID();
    failedJobList.add(jobID_one);
    wm.setFailedJobList(failedJobList);
    assertEquals(1, wm.getFailedJobCounter());
  }
  
  @Test
  public void testGetFreePort() {
    int port = wm.getFreePort();
    assertEquals(60001, port);
    
  }
  
  @Test
  public void testGetMigrateSuperStep() {
    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    String staffId_one = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
    sip.setMigrateSS(2);
    runningStaffs.put(staffid_one, sip);
    wm.setRunningStaffs(runningStaffs);
    int migrateSuperStep = wm.getMigrateSuperStep(staffid_one);
    assertEquals(2, migrateSuperStep);
  }
  
  @Test
  public void testClearStaffRC() {
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    wafj.setStaffReportCounter(2);
    BSPJobID jobID = new BSPJobID();
    Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    runningJobs.put(jobID, wafj);
    wm.setRunningJobtoWorkerAgent(runningJobs);
    wm.clearStaffRC(jobID);
    assertEquals(0, (int) wafj.getStaffReportCounter());
  }
  
  @Test
  public void testLocalBarrier() {
    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    BSPJobID jobID = new BSPJobID();
    StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
        jobID.getId(), 0, 0);
    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
    SuperStepReportContainer ssrc = new SuperStepReportContainer();
    runningStaffs.put(staffID, sip);
    wm.setRunningStaffs(runningStaffs);
    
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    wafj.setStaffReportCounter(2);
    Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    runningJobs.put(jobID, wafj);
    wm.setRunningJobtoWorkerAgent(runningJobs);
    
    wm.localBarrier(jobID, staffID, 0, ssrc);
    
    assertEquals(1, runningStaffs.get(staffID).getStatus().getProgress());
    assertEquals(false, wm.localBarrier(jobID, staffID, 0, ssrc));
  }
  
  @Test
  public void testUpdateWorkerJobState() {
    WorkerAgentForJob wafj = new WorkerAgentForJob(
        TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
    BSPJobID jobID = new BSPJobID();
    StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
        jobID.getId(), 0, 0);
    Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
    runningJobs.put(jobID, wafj);
    wm.setRunningJobtoWorkerAgent(runningJobs);
    assertEquals(false, wm.updateWorkerJobState(staffID));
  }
  
  @Test
  public void testInitFileSystem() throws Exception {
    ControllerProtocolServer cpserver= new ControllerProtocolServer();
    InetSocketAddress bspControllerAddr = new InetSocketAddress(
        "Master.Hadoop", 65001);
    ControllerProtocol controllerClient = (ControllerProtocol) RPC
        .waitForProxy(ControllerProtocol.class, 0L, bspControllerAddr, conf);
    controllerClient = mock(ControllerProtocol.class);
    when(controllerClient.getSystemDir()).thenReturn("/home/user");
    wm.setControllerClient(controllerClient);
    wm.initFileSystem();
    assertEquals(false, wm.isJustInited());   
    cpserver.stop();
  }
  
  @Test
  public void testSetStaffAgentAddress() {
    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
    String staffId_one = "attempt_201207241653_0001_000001_0";
    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
    BSPJobID jobID = new BSPJobID();
    runningStaffs.put(staffid_one, sip);
    wm.setRunningStaffs(runningStaffs);
    wm.setStaffAgentAddress(staffid_one, "Master.Hadoop:65002");
  } 
  
  //@Test
//  public void testOfferService() throws Exception {
//    BSPZookeeper bspzk=wm.getBspzk();
//    if (bspzk.equaltostat(Constants.BSPCONTROLLER_LEADER, true)) {
//      bspzk.create(Constants.BSPCONTROLLER_LEADER,
//          new byte[0] , BSPIds.OPEN_ACL_UNSAFE,
//          new BSPCreateModeImpl().getEPHEMERAL());
// //     this.becomeActive();
////      LOG.info("acitve address is " + bspControllerAddr);
//    }
//    if (bspzk.equaltostat(Constants.BSPCONTROLLER_STANDBY_LEADER,
//        false)) {
//      bspzk.create(Constants.BSPCONTROLLER_STANDBY_LEADER,
//          new byte[0], BSPIds.OPEN_ACL_UNSAFE,
//          new BSPCreateModeImpl().getPERSISTENT());
//    } else {
//      bspzk.setData(Constants.BSPCONTROLLER_STANDBY_LEADER,
//          new byte[0],
//          bspzk.exists(Constants.BSPCONTROLLER_STANDBY_LEADER, false)
//              .getVersion());
//    }
//        
////    if(bspzk.exists("/leader", false)!=null){
////      bspzk.create("/leader", new byte[0] , BSPIds.OPEN_ACL_UNSAFE,
////        new BSPCreateModeImpl().getPERSISTENT());
////    }
////    
//   // wm.choseActiveControllerAddress(); 
////    InetSocketAddress bspControllerAddr = new InetSocketAddress(
////        "Master.Hadoop", 65003);
////     wm.setBspControllerAddr(bspControllerAddr);
////    wm.setStandbyControllerAddr(bspControllerAddr);
//    
//    String standControllerAddr = wm.getData(
//        Constants.BSPCONTROLLER_STANDBY_LEADER);
//   
//    InetSocketAddress standbyControllerAddr = NetUtils
//        .createSocketAddr(standControllerAddr);
//    System.out.print(standbyControllerAddr.toString());
//    wm.setStandbyControllerAddr(standbyControllerAddr);
//    
//    
//    
////    String controllerAddr = wm.getData(Constants.BSPCONTROLLER_LEADER);
////    InetSocketAddress bspControllerAddr = NetUtils.createSocketAddr(controllerAddr);
//    wm.setBspControllerAddr(standbyControllerAddr);
//    
//    WorkerManagerStatus workerMangerStatus = new WorkerManagerStatus();
//    wm.setWorkerMangerStatus(workerMangerStatus);
//    
//    List<StaffStatus> reportStaffStatusList = new ArrayList<StaffStatus>();
//    Fault fault = new Fault();
//    wm.setReportStaffStatusList(reportStaffStatusList);
//    
//    Map<StaffAttemptID, WorkerManager.StaffInProgress> runningStaffs = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
//    String staffId_one = "attempt_201207241653_0001_000001_0";
//    StaffAttemptID staffid_one = new StaffAttemptID().forName(staffId_one);
//    WorkerManager.StaffInProgress sip = wm.new StaffInProgress();
//    StaffStatus stfs = new StaffStatus();
//    stfs.setRunState(StaffStatus.State.RUNNING);
//    sip.setStaffStatus(stfs);
//    
//    runningStaffs.put(staffid_one, sip);
//    wm.setRunningStaffs(runningStaffs);
//    
//    Map<StaffAttemptID, WorkerManager.StaffInProgress> reprotStaffsMap = new ConcurrentHashMap<StaffAttemptID, WorkerManager.StaffInProgress>();
//    wm.setReprotStaffsMap(reprotStaffsMap);
//    
//    List<Fault> workerFaultList = new ArrayList<Fault>();
//    wm.setWorkerFaultList(workerFaultList);
//    wm.offerService();
//
//    
//  }
  
  @Test
  public void testEnsureFreshControllerClient() throws IOException {
    ControllerProtocolServer cpserver=new ControllerProtocolServer();
    InetSocketAddress bspControllerAddr = new InetSocketAddress(
        "Master.Hadoop", 65001);
    wm.setBspControllerAddr(bspControllerAddr);
    
    ControllerProtocol standbyControllerClient =(ControllerProtocol) RPC.waitForProxy(
        ControllerProtocol.class, ControllerProtocol.versionID,
        bspControllerAddr, conf);
    
    wm.setStandbyControllerClient(standbyControllerClient);
    InetSocketAddress standbyControllerAddr= new InetSocketAddress(
        "Master.Hadoop", 65001);
   wm.setStandbyControllerAddr(standbyControllerAddr);
   
   wm.ensureFreshControllerClient();
   assertEquals(true,wm.getStandbyControllerAddr().toString().equals(bspControllerAddr.toString()));
   cpserver.stop();
  }
  
  @Test
  public void testGetData() throws IOException, Exception, InterruptedException {
    BSPZookeeper bspzk=wm.getBspzk();
    if (bspzk.equaltostat(Constants.BSPCONTROLLER_LEADER, true)) {
        bspzk.create(Constants.BSPCONTROLLER_LEADER,
        new byte[0] , BSPIds.OPEN_ACL_UNSAFE,
      new BSPCreateModeImpl().getEPHEMERAL());
     }   
    wm.setBspzk(bspzk);
    String controllerAddr = wm.getData(Constants.BSPCONTROLLER_LEADER);
    assertEquals("",controllerAddr);
  }  
   
  @Test
  public void testChoseActiveControllerAddress() {
    wm.choseActiveControllerAddress();
  }
  
  @Test
  public void testRun() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testClose() {
    fail("Not yet implemented");
  }
  
}
