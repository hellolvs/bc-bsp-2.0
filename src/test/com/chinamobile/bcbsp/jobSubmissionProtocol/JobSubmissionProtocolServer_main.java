package com.chinamobile.bcbsp.jobSubmissionProtocol;


public class JobSubmissionProtocolServer_main {
  public static void main(String[] args){
    JobSubmissionProtocolServer wafss=new JobSubmissionProtocolServer();
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
