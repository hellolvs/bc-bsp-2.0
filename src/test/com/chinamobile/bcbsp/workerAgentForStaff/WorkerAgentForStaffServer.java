package com.chinamobile.bcbsp.workerAgentForStaff;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;

public class WorkerAgentForStaffServer implements WorkerAgentForStaffInterface{


  private int i=0;
  Server masterServer=null;
  private static int flag=0;
  
public static int getFlag() {
    return flag;
}
public static void setFlag(int flag) {
  WorkerAgentForStaffServer.flag = flag;
}
  public WorkerAgentForStaffServer(){
    i=1;
    BSPConfiguration bspconf=new BSPConfiguration();
    try{
      masterServer = RPC.getServer(this, "192.168.1.2", 65002, bspconf);
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
  public WorkerAgentForStaffInterface getWorker(BSPJobID jobId,
      StaffAttemptID staffId, int belongPartition) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
      int belongPartition, BytesWritable data, String type) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
      int belongPartition, BytesWritable data) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String address() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void onKillStaff() {
    // TODO Auto-generated method stub
    
  }
  
}
