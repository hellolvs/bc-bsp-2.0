package com.chinamobile.bcbsp.rpcCommunicateProtocol;

public class RPCCommunicationProtocolServer_main {
  public static void main(String[] args){
    RPCCommunicationProtocolServer wafss=new RPCCommunicationProtocolServer();
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
