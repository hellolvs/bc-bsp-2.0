package com.chinamobile.bcbsp.rpcCommunicateProtocol;

import java.io.IOException;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.comm.BSPMessagesPack;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.comm.io.util.WritableBSPMessages;

public class RPCCommunicationProtocolServer implements RPCCommunicationProtocol{



  private int i=0;
  Server masterServer=null;
  private static int flag=0;
  
  public static int getFlag() {
    return flag;
  }
  public static void setFlag(int flag) {
    RPCCommunicationProtocolServer.flag = flag;
  }
  public RPCCommunicationProtocolServer(){
    i=1;
    BSPConfiguration bspconf=new BSPConfiguration();
    try{
      masterServer = RPC.getServer(this, "192.168.1.2", 65005, bspconf);
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
  public int sendPackedMessage(BSPMessagesPack packedMessages) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int sendPackedMessage(BSPMessagesPack packedMessages, String str) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int sendUnpackedMessage(IMessage messages) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int sendPackedMessage(WritableBSPMessages packedMessages,
      int srcPartition, int dstBucket, int superstep) {
    // TODO Auto-generated method stub
    return 0;
  }
  
}
