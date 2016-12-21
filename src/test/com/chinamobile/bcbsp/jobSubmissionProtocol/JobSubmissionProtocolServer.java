package com.chinamobile.bcbsp.jobSubmissionProtocol;

import java.io.IOException;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants.BspControllerRole;
import com.chinamobile.bcbsp.bspcontroller.ClusterStatus;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.JobProfile;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffStatus;

public class JobSubmissionProtocolServer implements JobSubmissionProtocol {

  private int i=0;
  Server masterServer=null;
  private static int flag=0;
  private String workerManagerName;
  
  public void setWorkerManagerName(String workerManagerName) {
      this.workerManagerName = workerManagerName;
  }
  public static int getFlag() {
      return flag;
  }
  public static void setFlag(int flag) {
    JobSubmissionProtocolServer.flag = flag;
  }
  public JobSubmissionProtocolServer(){
      i=1;
      BSPConfiguration bspconf=new BSPConfiguration();
      try{
        masterServer = RPC.getServer(this, "192.168.1.2",54752, bspconf);
        this.start();
      }catch(Exception e){
          System.out.println(e);
      }
      
  }
  public void print(){
      while(this.flag==0){
          try {
              Thread.sleep(100);
          } catch (InterruptedException e) {
              e.printStackTrace();
          }
          System.out.println("the Server is on!");
      }
  }
  public void start() {
  
          try {
              masterServer.start();
          } catch (IOException e) {
              e.printStackTrace();
          }
          
  
  }
  public void stop() {
      masterServer.stop();
      this.flag=1;
      System.out.println("the Server is OFF!");
  }
  
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public BSPJobID getNewJobId() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JobStatus submitJob(BSPJobID jobID, String jobFile) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getFilesystemName() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JobStatus[] jobsToComplete() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException {
    JobStatus job1=new JobStatus();
    JobStatus job2=new JobStatus();
    //JobStatus[] jobs = new JobStatus[2];
    JobStatus[] jobs = {job1,job2};
    return jobs;
  }

  @Override
  public StaffAttemptID[] getStaffStatus(BSPJobID jobId) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public StaffStatus[] getStaffDetail(BSPJobID jobId) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setCheckFrequency(BSPJobID jobID, int cf) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setCheckFrequencyNext(BSPJobID jobId) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String getSystemDir() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void killJob(BSPJobID jobid) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean killStaff(StaffAttemptID staffId, boolean shouldFail)
      throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean recovery(BSPJobID jobId) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void recordFault(Fault f) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public BspControllerRole getRole() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Counters getCounters(BSPJobID jobid) {
    // TODO Auto-generated method stub
    return null;
  }
  
}
