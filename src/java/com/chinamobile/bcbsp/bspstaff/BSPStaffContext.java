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

package com.chinamobile.bcbsp.bspstaff;



import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.CommunicationFactory;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.ml.BSPPeer;
import com.chinamobile.bcbsp.ml.KeyValuePair;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * BSPStaffContext.java This class implements {@link BSPStaffContextInterface}.
 * Methods defined in the BSPStaffContextInterface can be used by users. While
 * other methods only can be invoked by {@link BSPStaff}.
 * @author WangZhigang
 * @version 1.0
 */

public class BSPStaffContext implements BSPStaffContextInterface {
  /**BSP job configuration.*/
  private BSPJob jobConf;
  /**The user defined vertex .*/
  @SuppressWarnings("unchecked")
  private Vertex vertex;
  /**The current super step number.*/
  @SuppressWarnings("unchecked")
  private int currentSuperStepCounter;
  /**The BSP message cache.*/
  private IMessage messagesCache;
  /**The aggregateValues.*/
  @SuppressWarnings("unchecked")
  private HashMap<String, AggregateValue> aggregateValues; // Aggregate values
                                                           // from the previous
                                                           // super step.
  /**True: the current vertex is active; False: is inactive.*/
  private boolean activeFlag;
  /**The communicator.*/
  private CommunicatorInterface commHandler;
  /** the key value pair for machine learning.*/
  private KeyValuePair currentKVPair;
  /** BSPPeer for machine learning.*/
  private BSPPeer currentPeer;
  /**checkPointFrequency for recovery*/
  private int checkPointFrequency;
  
  @SuppressWarnings("unchecked")
  public BSPStaffContext(BSPJob jobConf, Vertex aVertex,
      int currentSuperStepCounter) {
    this.jobConf = jobConf;
    this.vertex = aVertex;
    this.currentSuperStepCounter = currentSuperStepCounter;
    this.messagesCache = CommunicationFactory.createPagerankMessage();
    this.aggregateValues = new HashMap<String, AggregateValue>();
    this.activeFlag = true;
  }

  /** The constructor of BSPStaffContext
 * This is a context for processing one vertex.
 * @param ajobConf BSP job configuration
 * @param acurrentSuperStepCounter the current super step number*/
  public BSPStaffContext(BSPJob ajobConf, int acurrentSuperStepCounter) {
    this.jobConf = ajobConf;
    this.currentSuperStepCounter = acurrentSuperStepCounter;
    this.messagesCache = CommunicationFactory.createPagerankMessage();
    this.aggregateValues = new HashMap<String, AggregateValue>();
    this.activeFlag = true;
  }

  /**Update the vertex.
   * @param v a new vertex.*/
  public void refreshVertex(Vertex v) {
    this.vertex = v;
  }

  /**
   * Get the active state.
   * @return
   */
  public boolean getActiveFLag() {
    return this.activeFlag;
  }

  /**
   * Add an aggregate value (key-value).
   * @param key the key of aggregate value
   * @param value AggregateValue
   */
  @SuppressWarnings("unchecked")
  public void addAggregateValues(String key, AggregateValue value) {
    this.aggregateValues.put(key, value);
  }
  
  /**
   * Cleanup the MessagesCache.
   * @return <code>true</code> if the operation is successfull, else
   *         <code>false</code>.
   */
  public boolean cleanMessagesCache() {
    boolean success = false;
    this.messagesCache = null;
    success = true;

    return success;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Vertex getVertex() {
    return this.vertex;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void updateVertex(Vertex avertex) {
    this.vertex = avertex;
  }

  @Override
  public int getOutgoingEdgesNum() {
    return this.vertex.getEdgesNum();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<Edge> getOutgoingEdges() {
    return this.vertex.getAllEdges().iterator();
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  @Override
  public boolean removeEdge(Edge edge) {
    this.vertex.removeEdge(edge);
    return true;
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  @Override
  public boolean updateEdge(Edge edge) {
    this.vertex.updateEdge(edge);
    return true;
  }

  @Override
  public int getCurrentSuperStepCounter() {
    return this.currentSuperStepCounter;
  }

  @Override
  public BSPJob getJobConf() {
    return this.jobConf;
  }
  
  @Override
  public void send(IMessage msg)  {
    try {
      if (jobConf.getComputeState() != 0) {
        this.commHandler.sendPartition(msg); 
      } else {
        this.commHandler.send(msg);
      }
    } catch (IOException e) {
      throw new RuntimeException("Send message exception:", e);
    }
  }

  @Override
  public void send(BSPMessage msg) {
    send((IMessage) msg);
  }

  @Override
  @SuppressWarnings("unchecked")
  public AggregateValue getAggregateValue(String name) {
    return this.aggregateValues.get(name);
  }

  @Override
  public void voltToHalt() {
    this.activeFlag = false;
  }

  // Note Use Singleton Message Instead Of New A Lot Of Ones.
  @SuppressWarnings("rawtypes")
  @Override
  public IMessage getMessage() {
    return this.messagesCache;
  }

  @Override
  public void setCommHandler(CommunicatorInterface handler) {
    this.commHandler = handler;
  }

  /**Update the super step number.*/
  public void refreshSuperStep(int superstep) {
    this.currentSuperStepCounter = superstep;
  }

@Override
public CommunicatorInterface getCommunicator() {//just for test
	// TODO Auto-generated method stub
	return this.commHandler;
}

/**
 * Update the peer for machine learning. 
 * @param peer
 */
public void updatePeer(BSPPeer peer) {
  this.currentPeer = peer;
}

public BSPPeer getCurrentPeer() {
  return currentPeer;
}
/*Biyahui added*/
public int getCheckPointFrequency() {
	return checkPointFrequency;
}

public void setCheckPointFrequency(int checkPointFrequency) {
	this.checkPointFrequency = checkPointFrequency;
}
}
