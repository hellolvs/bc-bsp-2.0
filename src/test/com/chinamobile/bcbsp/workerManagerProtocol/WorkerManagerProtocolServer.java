package com.chinamobile.bcbsp.workerManagerProtocol;

import java.io.IOException;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.util.BSPJobID;

public class WorkerManagerProtocolServer implements WorkerManagerProtocol{

  

  private int i=0;
  Server masterServer=null;
  private static int flag=0;
  
public static int getFlag() {
    return flag;
}
public static void setFlag(int flag) {
  WorkerManagerProtocolServer.flag = flag;
}
  public WorkerManagerProtocolServer(){
    i=1;
    BSPConfiguration bspconf=new BSPConfiguration();
    try{
      masterServer = RPC.getServer(this, "192.168.1.2", 65003, bspconf);
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
  public boolean dispatch(BSPJobID jobId, Directive directive,
      boolean isRecovery, boolean changeWorkerState, int failCounter)
      throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void clearFailedJobList() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addFailedJob(BSPJobID jobId) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int getFailedJobCounter() {
    // TODO Auto-generated method stub
    return 0;
  }
  
}
