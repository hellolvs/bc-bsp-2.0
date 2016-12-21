/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chinamobile.bcbsp.examples.hits;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * BSP program of PageRnak.
 *
 */
public class HitsBSPNew extends BSP<HitsMessage> {
  /** New vertex value */
  public static final Log LOG = LogFactory.getLog(HitsBSPNew.class);
//  /** New vertex value */
//  public static final double CLICK_RP = 0.0001;
//  /** New vertex value */
//  public static final double FACTOR = 0.15;
  // Variables of the Graph.
  /** New vertex value */
  private double newVertexValue = 0.0f;
  /**new Vertex hub value*/
  private double newVertexHubValue = 0.0f;
  /** New vertex value */
  private double receivedMsgValue = 0.0f;
  /** New vertex value */
  private double receivedMsgSum = 0.0f;
  /**reveived message value for normalization*/
  private double receicedMsgPowSum = 0.0f;
  /** New vertex value */
  private double sendMsgValue = 0.0f;
  /** New vertex value */
  private Iterator<SEdge> outgoingEdges;
  private List<SEdge> oedgesList = new ArrayList<SEdge>();
  /** incomeingedges*/

  /** New vertex value */
  private SEdge edge;
  /** New vertex value */
  private HitsMessage msg;
  /** New vertex value */
  private int failCounter = 0;
private Object receivedMsgPowSum;
//  /**vertex to get the outgoing edges*/
//  private HitsVertexLiteNew tempVertex = new HitsVertexLiteNew();
  @Override
  public void setup(Staff staff) {
    this.failCounter = staff.getFailCounter();
    // LOG.info("Test FailCounter#################$$: " + this.failCounter);
  }
  @Override
  public void initBeforeSuperStep(SuperStepContextInterface arg0) {
  }
  @Override
  public void compute(Iterator<HitsMessage> messages,
      BSPStaffContextInterface context) throws Exception {
//    // Receive messages sent to this Vertex.
//	  //LOG.info("Vertex is processed! PRBSPNew");
//	tempVertex = (HitsVertexLiteNew) context.getVertex();
    receivedMsgValue = 0.0f;
    receivedMsgSum = 0.0f;
    receicedMsgPowSum = 0.0f;
    ArrayList inArray = new ArrayList();
    
    //int i = 0;
    int s =0 ;
    if(!messages.hasNext()){
        LOG.info("feng-test: messages is null");
    }

    while (messages.hasNext()) {
      // Note Modified

    	s++;
     HitsMessage currentMsg = messages.next();
     String[] msgValue = currentMsg.getNewContent().split(" ");
     //LOG.info("feng-test: currentMsg.getSrcVertexID() "+currentMsg.getSrcVertexID());
     if(msgValue.length==2){
     inArray.add(Integer.parseInt(msgValue[1]));
      receivedMsgValue = Double.parseDouble(msgValue[0]);
      receicedMsgPowSum = receicedMsgPowSum+Math.pow(receivedMsgValue, 2);
      receivedMsgSum += receivedMsgValue;
     }else{
    	 throw new Exception(); //even superstep is the sum of hub,odd super is the sum of authority
     }
    }
    SVertex vertex = (SVertex) context.getVertex();
    String value = vertex.getVertexValue();
	String[] valueinfo = value.split(":");
	String av = valueinfo[0];
	String hv = valueinfo[1];
    // Handle received messages and Update vertex value.
    if (context.getCurrentSuperStepCounter() == 0) {	
      sendMsgValue = Double.valueOf(hv);
    } else{ 
    	if(context.getCurrentSuperStepCounter()%2!=0){

        	if(receicedMsgPowSum!=0){
            	newVertexValue = receivedMsgSum/Math.sqrt(receicedMsgPowSum);//without normalization
            	}else{
            	newVertexValue = receivedMsgSum;
            	}
      vertex.setVertexValue(newVertexValue+":"+hv);
      context.updateVertex(vertex);
      sendMsgValue = newVertexValue;
    }else{
    	if(receicedMsgPowSum!=0){
    	newVertexValue = receivedMsgSum/Math.sqrt(receicedMsgPowSum);//without normalization
    	}else{
    	newVertexValue = receivedMsgSum;
    	}
    	vertex.setVertexValue(av+":"+newVertexValue);
    	context.updateVertex(vertex);
    	sendMsgValue =newVertexValue;
    }

    }
    oedgesList = vertex.getAllEdges();
	if(context.getCurrentSuperStepCounter()==0){
		for(int i=0;i<oedgesList.size();i++){

	        msg = (HitsMessage) context.getMessage();
	        msg.setNewContent(Double.toString(sendMsgValue),vertex.getVertexID());
	        msg.setMessageId(Integer.parseInt(oedgesList.get(i).getVertexID()));
	        context.send(msg);
	}
	}else{
    if(context.getCurrentSuperStepCounter()%2==0){    	
    	for(int i=0;i<oedgesList.size();i++){
    	        
    	        // Note Improved For Message Reuse.
    	        msg = (HitsMessage) context.getMessage();
    	        //msg.setContent(sendMsgValue);
    	        msg.setNewContent(Double.toString(sendMsgValue),vertex.getVertexID());
    	        msg.setMessageId(Integer.parseInt(oedgesList.get(i).getVertexID()));
    	        context.send(msg);
    	}
    }else{
          for(int i=0;i<inArray.size();i++){
	        
	        // Note Improved For Message Reuse.
	        msg = (HitsMessage) context.getMessage();        
	        msg.setNewContent(Double.toString(sendMsgValue),vertex.getVertexID());
	        msg.setMessageId(Integer.parseInt(inArray.get(i).toString()));
	        context.send(msg);
    }
  }
	}
    return;
}
}