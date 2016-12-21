package com.chinamobile.bcbsp.examples.lpclustering;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;

/**
 * BSPLPCluster.java implements {@link BSP}.
 * Implement a simple but efficient clustering method 
 * based on the label propagation algorithm (LPA). This is used to find 
 * clusters. The details of LPA can refer to Usha Nandini Raghavan et al. 
 * "Near linear time algorithm to detect community structures in large-scale networks".
 */
public class BSPLPCluster extends BSP<LPClusterMessage>{
	  /** New vertex value */
	  public static final Log LOG = LogFactory.getLog(BSPLPCluster.class);
	
	  private Iterator<Edge> outgoingEdges;
	
	  private LPClusterEdge edge;
	  /** New vertex value */
	  private LPClusterMessage msg;
	  /** New vertex value */
	  private int failCounter = 0;
	  
	  private int sendMsgValue = 0;
	  @Override
	  public void setup(Staff staff) {
	    this.failCounter = staff.getFailCounter();
	    // LOG.info("Test FailCounter#################$$: " + this.failCounter);
	  }
	@Override
	public void initBeforeSuperStep(SuperStepContextInterface arg0) {
	}
	@Override
	public void compute(Iterator<LPClusterMessage> messages,
			BSPStaffContextInterface context) throws Exception {
		  HashMap<Integer, Integer>hm=new HashMap<Integer, Integer>();
		  LPClusterVertex vertex = (LPClusterVertex) context.getVertex();
		  int vertexLabel=vertex.getVertexID();
		  int temp;
		  int maxCount=1;
		  int count;

		  if (context.getCurrentSuperStepCounter() == 0) {
			  sendMsgValue = vertexLabel; // old vertex value
		  }else{
			  //get message,set them into hashmap
			    while (messages.hasNext()) {
			    	temp = ((LPClusterMessage) (messages.next())).getContent();
			    	if(hm.containsKey(temp)){
			    		count=hm.get(temp);
			    		hm.put(temp,count++);
			    	}else{
			    		hm.put(temp,1);
			    	}
			    }
			    
			    //select maxCount label
			    Set set_entry=hm.entrySet();
			    Iterator iterator=set_entry.iterator();
			    //int minLabel=vertexLabel;
			    while(iterator.hasNext()){
			    	Entry entry=(Entry)iterator.next();
			    	int tempLabel=Integer.parseInt(entry.getKey().toString());
			    	int tempCount=Integer.parseInt(entry.getValue().toString());
			    	if(tempCount>maxCount){
			    		maxCount=tempCount;
			    		//vertexLabel=tempLabel;
			    		vertexLabel=tempLabel;
			    	}else if(tempCount==maxCount){
			    		if(tempLabel<vertexLabel){
			    		//maxCount=tempCount;
			    		//vertexLabel=tempLabel;
			    		vertexLabel=tempLabel;
			    		}
			    	}
			    }
			    sendMsgValue = vertexLabel;
			   // LOG.info(vertexLabel);
		  }
		  vertex.setVertexLabel(sendMsgValue);
	      context.updateVertex(vertex);
	      // Send new messages.
	      outgoingEdges = context.getOutgoingEdges();
	      while (outgoingEdges.hasNext()) {
	        edge = (LPClusterEdge) outgoingEdges.next();
	        // Note Improved For Message Reuse.
	        msg = (LPClusterMessage) context.getMessage();
	        msg.setContent(sendMsgValue);
	        msg.setMessageId(edge.getVertexID());
	        context.send(msg);
	      }
	      return;		  
	}

}
