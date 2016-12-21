
package com.chinamobile.bcbsp.examples.simrank;

/**
 * PageRankBSP.java
 */
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.IMessage;


import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * PageRankBSP.java This is the user-defined arithmetic which implements
 * {@link BSP}.
 *
 *
 *
 */

public class SRBSP extends BSP<SRmessage> {

 
  public static final Log LOG = LogFactory.getLog(SRBSP.class);
 
  public static final String ERROR_SUM = "aggregator.error.sum";
  
  public static final double ERROR_THRESHOLD = 0.1;
  
  public static final double CLICK_RP = 0.0001;
  
  public static final double FACTOR = 0.15;

 
  private String newVertexValue = "";
  
  private double receivedMsgValue = 0.0f;
  
  private double receivedMsgSum = 0.0f;
 
  private double sendMsgValue = 0.0f;
  
  @SuppressWarnings("unchecked")
  private Iterator<Edge> outgoingEdges;
  
  private SREdge edge;
  
  private SRmessage msg;
  
  private int failCounter = 0;

  @Override
  public void setup(Staff staff) {
    this.failCounter = staff.getFailCounter();
    LOG.info("Test FailCounter: " + this.failCounter);
  }
@Override
  public void compute(Iterator<SRmessage> messages,
      BSPStaffContextInterface context) throws Exception {
    int count=0;
    receivedMsgValue = 0.0f;
    receivedMsgSum = 0.0f;
    while (messages.hasNext()) {
      receivedMsgValue = ((SRmessage) (messages.next())).getContent();
      receivedMsgSum += receivedMsgValue;
      count++;
    }

    SRVertex vertex = (SRVertex) context.getVertex();
    String vertexvalue = vertex.getVertexValue();
	String[] valuearray = vertexvalue.split(":");
	double message = Double.parseDouble(valuearray[0]);
	int divcount = Integer.parseInt(valuearray[1]);
   
    if (context.getCurrentSuperStepCounter() == 0) {
    	sendMsgValue = message;
    	
    } else {
    	//������Ϣ�������㶥��Ե�simrankֵ
    	sendMsgValue = receivedMsgSum*0.8/divcount;
    	String[] vertexIDarray= vertex.getVertexID().split("_");
    	if(vertexIDarray[0].equals(vertexIDarray[1]))
    	                                sendMsgValue = 1;
    	newVertexValue = sendMsgValue + ":"+divcount;   		  	
        vertex.setVertexValue(newVertexValue);
        context.updateVertex(vertex);
        LOG.info("this cout is "+count+"this divcount is "+divcount);
    
    }
    outgoingEdges = context.getOutgoingEdges();
    SREdge edge1 = (SREdge)outgoingEdges.next();
    SREdge edge2 = (SREdge)outgoingEdges.next();
    String[] keys1= edge1.getVertexID().split(" ");
    String[] keys2= edge2.getVertexID().split(" ");
    //���ڶ������ϵĳ��߶��������ϳ��µĶ��㣬��������Ϣ��
    for(int i=0;i<keys1.length;i++){
    	for(int j=0;j<keys2.length;j++){
    		if(Integer.parseInt(keys1[i])<=Integer.parseInt(keys2[j])){
    			  msg =  (SRmessage) context.getMessage();
    		      msg.setContent(sendMsgValue);
    		      msg.setMessageId(new String(keys1[i]+"_"+keys2[j]));
    		      context.send(msg);
    		}else{
    			msg =  (SRmessage) context.getMessage();
  		        msg.setContent(sendMsgValue);
  		        msg.setMessageId(new String(keys2[j]+"_"+keys1[i]));
  		        context.send(msg);
    		}
    	}
    }
    return;
  }

  @Override
  public void initBeforeSuperStep(SuperStepContextInterface arg0) {
  }
}
