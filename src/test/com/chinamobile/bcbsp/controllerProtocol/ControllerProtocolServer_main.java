package com.chinamobile.bcbsp.controllerProtocol;


public class ControllerProtocolServer_main {
  public static void main(String[] args){
    ControllerProtocolServer wafss=new ControllerProtocolServer();
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
