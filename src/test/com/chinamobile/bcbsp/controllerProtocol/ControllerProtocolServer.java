package com.chinamobile.bcbsp.controllerProtocol;

import java.io.IOException;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

public class ControllerProtocolServer implements ControllerProtocol{

  
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
    ControllerProtocolServer.flag = flag;
  }
  public ControllerProtocolServer(){
      i=1;
      BSPConfiguration bspconf=new BSPConfiguration();
      try{
        masterServer = RPC.getServer(this, "192.168.1.2", 65001, bspconf);
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
  public boolean register(WorkerManagerStatus status) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean report(Directive directive) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getSystemDir() {
    // TODO Auto-generated method stub
    return null;
  }
  
}
