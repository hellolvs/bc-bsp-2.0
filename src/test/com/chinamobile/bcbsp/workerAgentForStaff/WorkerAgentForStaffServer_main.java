package com.chinamobile.bcbsp.workerAgentForStaff;

public class WorkerAgentForStaffServer_main {
  public static void main(String[] args){
  WorkerAgentForStaffServer wafss=new WorkerAgentForStaffServer();
  while(true){
    
    try {
        if(wafss.getFlag()==0){
        Thread.sleep(1000);
        }else{
            break;
        }
    } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
  }
  }
}
