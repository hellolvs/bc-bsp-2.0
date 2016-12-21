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

package com.chinamobile.bcbsp.examples.bytearray.pagerank;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * BSP program of PageRnak.
 *
 */
public class PRBSPNew extends BSP<PageRankMessage> {
  /** New vertex value */
  public static final Log LOG = LogFactory.getLog(PRBSPNew.class);
  /** New vertex value */
  public static final double CLICK_RP = 0.0001;
  /** New vertex value */
  public static final double FACTOR = 0.15;
  // Variables of the Graph.
  /** New vertex value */
  private float newVertexValue = 0.0f;
  /** New vertex value */
  private float receivedMsgValue = 0.0f;
  /** New vertex value */
  private float receivedMsgSum = 0.0f;
  /** New vertex value */
  private float sendMsgValue = 0.0f;
  /** New vertex value */
  private Iterator<Edge> outgoingEdges;
  /** New vertex value */
  private PREdgeLiteNew edge;
  /** New vertex value */
  private PageRankMessage msg;
  /** New vertex value */
  private int failCounter = 0;
  @Override
  public void setup(Staff staff) {
    this.failCounter = staff.getFailCounter();
  }
  @Override
  public void initBeforeSuperStep(SuperStepContextInterface arg0) {
  }
  @Override
  public void compute(Iterator<PageRankMessage> messages,
      BSPStaffContextInterface context) throws Exception {
    // Receive messages sent to this Vertex.
	  //LOG.info("Vertex is processed! PRBSPNew");
    receivedMsgValue = 0.0f;
    receivedMsgSum = 0.0f;
    int i = 0;
    while (messages.hasNext()) {
      // Note Modified
    	i++;
      receivedMsgValue = ((PageRankMessage) (messages.next())).getContent();
      receivedMsgSum += receivedMsgValue;
    }
    PRVertexLiteNew vertex = (PRVertexLiteNew) context.getVertex();
    // Handle received messages and Update vertex value.
    if (context.getCurrentSuperStepCounter() == 0) {
      sendMsgValue = Float.valueOf(vertex.getVertexValue()) /
          context.getOutgoingEdgesNum(); // old vertex value
    } else {
      newVertexValue = (float) (CLICK_RP * FACTOR + receivedMsgSum *
          (1 - FACTOR));
      sendMsgValue = newVertexValue / context.getOutgoingEdgesNum();
      vertex.setVertexValue(newVertexValue);
      context.updateVertex(vertex);
    }
    // Send new messages.
    outgoingEdges = context.getOutgoingEdges();
    while (outgoingEdges.hasNext()) {
      edge = (PREdgeLiteNew) outgoingEdges.next();
      // Note Improved For Message Reuse.
      msg = (PageRankMessage) context.getMessage();
      msg.setContent(sendMsgValue);
      msg.setMessageId(edge.getVertexID());
      context.send(msg);
    }
    return;
  }
}
