package com.chinamobile.bcbsp.workerManagerProtocol;


public class WorkerManagerProtocolServer_main {
  public static void main(String[] args){
    WorkerManagerProtocolServer wafss=new WorkerManagerProtocolServer();
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
