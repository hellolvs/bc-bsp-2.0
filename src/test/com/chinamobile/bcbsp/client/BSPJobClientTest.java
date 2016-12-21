package com.chinamobile.bcbsp.client;

import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.client.BSPJobClient.RawSplit;
import com.chinamobile.bcbsp.rpc.JobSubmissionProtocol;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileSystem;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFileSystemImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPZookeeperImpl;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.workermanager.WorkerManager;
import com.chinamobile.bcbsp.jobSubmissionProtocol.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BSPJobClientTest {
  
  BSPConfiguration conf = null;
  BSPZookeeper bspzk=null;
  BSPJobClient bspJobClient=null;
  @Before
  public void setUp() throws Exception {
    JobSubmissionProtocolServer jspserver=new JobSubmissionProtocolServer();
    conf = new BSPConfiguration();
    conf.set(Constants.BC_BSP_CONTROLLER_ADDRESS, "127.0.0.1:40000");   
    bspJobClient = new BSPJobClient();
    bspJobClient.setConf(conf);
    InetSocketAddress bspControllerAddr=new InetSocketAddress("Master.Hadoop",54752);
    JobSubmissionProtocol jobSubmitClient=(JobSubmissionProtocol) RPC.getProxy(
        JobSubmissionProtocol.class, 0L,
        bspControllerAddr, conf,
        NetUtils.getSocketFactory(conf, JobSubmissionProtocol.class));
    bspJobClient.setJobSubmitClient(jobSubmitClient);
    
    
  }    
  
  
  @After
  public void tearDown() throws Exception {

  }
  @Test
  public void testReadSplitFile() throws IOException {
//    BSPController controller=new BSPController();
//    
//    String path="/user/usr";
//    File pathFile = new File("/user/usr");
//    File dir = new File(pathFile, "test.txt");
//    if(!dir.exists()){
//      pathFile.createNewFile();
//    }
//    conf.set(Constants.BC_BSP_LOCAL_DIRECTORY, "/user/usr/test.txt");
//    conf.set(Constants.BC_BSP_SHARE_DIRECTORY, "/user");
//    controller.setConf(conf);
//    BSPFileSystem bspfs = new BSPFileSystemImpl(conf);
//    controller.setBspfs(bspfs);
//    DataInputStream splitFile = bspfs.open(new BSPHdfsImpl().newPath(path));
//    RawSplit[] splits;
//    try {
//      splits = BSPJobClient.readSplitFile(splitFile);
//    } finally {
//      splitFile.close();
//    }
  }
  
  @Test
  public void testMonitorAndPrintJob() throws IOException, InterruptedException {
    
    // test object
    BSPJobClient testbspjobclient = new BSPJobClient();
   
    //param1 job
    RunningJob info;
    
    JobStatus testJobStatus1;
    String strIden1 = new String("testID1");
    BSPJobID bspJobID1 = new BSPJobID(strIden1,1);
    String strUser1 = new String("MaYue1");
    int nProgress1 = 10;
    int nRunState1 = JobStatus.SUCCEEDED;
    testJobStatus1 = new JobStatus(bspJobID1,strUser1,nProgress1,nRunState1,2,2,2);
    info = testbspjobclient.new NetworkedJob(testJobStatus1);
    
    //param2 info
    BSPJob job = new BSPJob();
    
    boolean testResult = testbspjobclient.monitorAndPrintJob(job,info);
    boolean expect = true;
    
    assertEquals("the job is successful",expect,testResult);

}
  
  @Test
  public void testRun() throws Exception {
      
      int nExpectedResult = -1;
      BSPJobClient testBSPJobClient = new BSPJobClient();
     
      System.out.println("Test cmd is NULL");
      String[] NullCmd = new String[]{};
      int nTestResult;
      
      try {
          nTestResult = testBSPJobClient.run(NullCmd);
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
      
      System.out.println("Test cmd is -list");
      String[] listCmd = new String[]{new String("-list")};
      
      try {
          nTestResult = testBSPJobClient.run(listCmd);
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
      
      System.out.println("Test cmd is -workers");
      String[] workersCmd = new String[]{new String("-workers")};
            
      try {
          nTestResult = testBSPJobClient.run(workersCmd);
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
      
      System.out.println("Test cmd is -submit");
      String[] submitCmd = new String[]{new String("-submit")};
     
      try {
          nTestResult = testBSPJobClient.run(submitCmd);
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
      
      System.out.println("Test cmd is -kill");
      String[] killCmd = new String[]{new String("-kill")};
            
      try {
          nTestResult = testBSPJobClient.run(killCmd);
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
      
      System.out.println("Test cmd is -status");
      String[] statusCmd = new String[]{new String("-status")};        
     
      try {
          nTestResult = testBSPJobClient.run(statusCmd);
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
      
      System.out.println("Test cmd is -list-staffs");
      String[] list_staffsCmd = new String[]{new String("-list-staffs")};       
     
      try {
          nTestResult = testBSPJobClient.run(list_staffsCmd);
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
      
      System.out.println("Test cmd is -setcheckpoint");
      String[] setcheckpointCmd = new String[]{new String("-setcheckpoint")};
             
      try {
          nTestResult = testBSPJobClient.run(setcheckpointCmd);
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
      
      
      System.out.println("Test cmd is -master");
      String[] masterCmd = new String[]{new String("-master")};

      try {
          nTestResult = testBSPJobClient.run(masterCmd);
          
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
      
      System.out.println("Test cmd is -kill-task");
      String[] kill_taskCmd = new String[]{new String("-kill-task")};
            
      try {
          nTestResult = testBSPJobClient.run(kill_taskCmd);
          
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {

          e.printStackTrace();
      }
      
      System.out.println("Test cmd is -fail-task");
      String[] fail_taskCmd = new String[]{new String("-fail-task")};
             
      try {
          nTestResult = testBSPJobClient.run(fail_taskCmd);
          
          assertEquals("Expected result is -1",nExpectedResult,nTestResult);
      } catch (Exception e) {
          e.printStackTrace();
      }
}
  
  @Test
  public void testDisplayJobList() {
      
      JobStatus testJobStatus1; //RUNNING
      JobStatus testJobStatus2; //KILLED
      JobStatus testJobStatus3; //SUCCEEDED
      JobStatus testJobStatus4; //FAILED
      JobStatus testJobStatus5; //PREP
      JobStatus testJobStatus6; //RECOVERY
      
      JobStatus[] testJobStatusArray;
      
      String strIden1 = new String("testID1");
      BSPJobID bspJobID1 = new BSPJobID(strIden1,1);
      String strUser1 = new String("MaYue1");
      int nProgress1 = 10;
      int nRunState1 = JobStatus.RUNNING;
      testJobStatus1 = new JobStatus(bspJobID1,strUser1,nProgress1,nRunState1,1,1,1);
      
      String strIden2 = new String("testID2");
      BSPJobID bspJobID2 = new BSPJobID(strIden2,2);
      String strUser2 = new String("MaYue2");
      int nProgress2 = 80;
      int nRunState2 = JobStatus.KILLED;
      testJobStatus2 = new JobStatus(bspJobID2,strUser2,nProgress2,nRunState2,1,1,1);
      
      String strIden3 = new String("testID3");
      BSPJobID bspJobID3 = new BSPJobID(strIden3,3);
      String strUser3 = new String("MaYue3");
      int nProgress3 = 20;
      int nRunState3 = JobStatus.SUCCEEDED;
      testJobStatus3 = new JobStatus(bspJobID3,strUser3,nProgress3,nRunState3,1,1,1);
      
      String strIden4 = new String("testID4");
      BSPJobID bspJobID4 = new BSPJobID(strIden4,4);
      String strUser4 = new String("MaYue4");
      int nProgress4 = 30;
      int nRunState4 = JobStatus.FAILED;
      testJobStatus4 = new JobStatus(bspJobID4,strUser4,nProgress4,nRunState4,1,1,1);
      
      String strIden5 = new String("testID5");
      BSPJobID bspJobID5 = new BSPJobID(strIden5,5);
      String strUser5 = new String("MaYue5");
      int nProgress5 = 40;
      int nRunState5 = JobStatus.PREP;
      testJobStatus5 = new JobStatus(bspJobID5,strUser5,nProgress5,nRunState5,1,1,1);
      
      String strIden6 = new String("testID6");
      BSPJobID bspJobID6 = new BSPJobID(strIden6,6);
      String strUser6 = new String("MaYue6");
      int nProgress6 = 50;
      int nRunState6 = JobStatus.RECOVERY;
      testJobStatus6 = new JobStatus(bspJobID6,strUser6,nProgress6,nRunState6,1,1,1);
      
      
      
      testJobStatusArray = new JobStatus[]{testJobStatus1,testJobStatus2,testJobStatus3,
              testJobStatus4,testJobStatus5,testJobStatus6};
      BSPJobClient testBSPJobClient = new BSPJobClient();
      testBSPJobClient.displayJobList(testJobStatusArray);
      
      //test boundary
      JobStatus[] EmptyJobStatusArray = new JobStatus[]{};
      testBSPJobClient.displayJobList(EmptyJobStatusArray);
      
      //JobStatus[] EmptyContentJobStatusArray = new JobStatus[10];
      //testBSPJobClient.displayJobList(EmptyContentJobStatusArray);

}
  

  
  @Test
  public void testEnsureFreshjJobSubmitClient() {
    fail("Not yet implemented");
  }
  
  
  @Test
  public void testRunJob() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testGetClusterStatus() {
    fail("Not yet implemented");
  }
  
 // @Test
//  public void testZkWaitConnected() throws IOException {
//    String zkAddress = conf.get(Constants.ZOOKEEPER_QUORUM) + ":" +
//        conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
//            Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
////    BSPZookeeper bspzk=new BSPZookeeperImpl(zkAddress,
////        Constants.SESSION_TIME_OUT, BSPJobClient.class);
////    bspJobClient.
//    
//  }

  @Test
  public void testSubmitJobInternal() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testBSPJobClientConfiguration() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testBSPJobClientConfigurationBoolean() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testBSPJobClient() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testClose() {
    fail("Not yet implemented");
  }
  
  @Test
  public void testMain() {
    fail("Not yet implemented");
  }
  
}
