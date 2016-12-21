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

package com.chinamobile.bcbsp.examples.connectedcomponent;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * BSP program of PageRnak.
 * 
 */
public class CCByteArrayBSP extends BSP<CCMessage> {

//	public static final Log LOG = LogFactory.getLog(CCByteArrayBSP.class);
//	public static final String CC_SUM = "aggregator.connected component.sum";
	private int vertexValue = 0;
	private int failCounter = 0;
	private int currentComponent = 0;
	private boolean changed = false;
	private Iterator<CCEdgeLiteNew> outgoingEdges;
	/** New vertex value */
	private CCEdgeLiteNew edge;
	/** New vertex value */
	private CCMessage msg;

	@Override
	public void setup(Staff staff) {
		this.failCounter = staff.getFailCounter();
	}

	@Override
	public void initBeforeSuperStep(SuperStepContextInterface arg0) {
	}

	public void compute(Iterator<CCMessage> messages,
			BSPStaffContextInterface context) throws Exception {

		CCVertexLiteNew vertex = (CCVertexLiteNew) context.getVertex();
		currentComponent = vertex.getVertexValue();
		outgoingEdges = vertex.getAllEdges().iterator();

		// First superstep is special, because we can simply look at the
		// neighbors
		if (context.getCurrentSuperStepCounter() == 0) {
			currentComponent = vertex.getVertexID();
			while (outgoingEdges.hasNext()) {
				edge = (CCEdgeLiteNew) outgoingEdges.next();
				int neighbor = edge.getVertexID();
				if (neighbor < currentComponent) {
					currentComponent = neighbor;
				}
			}
			// Only need to send value if it is not the own id
			if (currentComponent != vertex.getVertexValue()) {
				vertex.setVertexValue(currentComponent);
				Iterator<CCEdgeLiteNew> outgoingEdges1 = vertex.getAllEdges().iterator();
				while (outgoingEdges1.hasNext()) {
					CCEdgeLiteNew edge = (CCEdgeLiteNew) outgoingEdges1.next();
					int neighbor = edge.getVertexID();
					if (neighbor > currentComponent) {
						msg = (CCMessage) context.getMessage();
						msg.setContent(currentComponent);
						msg.setMessageId(edge.getVertexID());
						context.send(msg);
					}
				}
			}
            context.voltToHalt();
			return;
		}

		changed = false;
		// for a smaller id
		while (messages.hasNext()) {
			int candidateComponent = messages.next().getContent();
			if (candidateComponent < currentComponent) {
				currentComponent = candidateComponent;
				changed = true;
			}
		}

		// propagate new component id to the neighbors
		if (changed) {
			vertex.setVertexValue(currentComponent);
			sendMessageToAllEdges(context);
		}
        context.voltToHalt();
	}

	public void sendMessageToAllEdges(BSPStaffContextInterface context) {
		Iterator<CCEdgeLiteNew> outgoingEdges = context.getVertex().getAllEdges().iterator();
		while (outgoingEdges.hasNext()) {
			edge = (CCEdgeLiteNew) outgoingEdges.next();
			// Note Improved For Message Reuse.
			msg = (CCMessage) context.getMessage();
			msg.setContent(currentComponent);
			msg.setMessageId(edge.getVertexID());
			context.send(msg);
		}
	}
}
