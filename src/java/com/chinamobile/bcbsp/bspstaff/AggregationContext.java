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
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.ml.BSPPeer;
import com.chinamobile.bcbsp.util.BSPJob;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


/**
 * Aggregation context.
 * @author Bai Qiushi
 * @version 1.0
 */
public class AggregationContext implements AggregationContextInterface {
  /**BSP job configuration.*/
  private BSPJob jobConf;
  /**The user defined vertex .*/
  private Vertex<?, ?, ? extends Edge<?, ?>> vertex;
  /**The list of user defined edge.*/
  private Iterator<Edge<?, ?>> outgoingEdgesClone;
  /**The current super step number.*/
  private int currentSuperStepCounter;
  /**The aggregateValues.*/
  @SuppressWarnings("rawtypes")
  private HashMap<String, AggregateValue> aggregateValues; // Aggregate values from the previous super step.
  /** The user defined peer . */
  private BSPPeer peer;                                                              
                                                         
  /**The construct of AggregationContext.
   * @param ajobConf BSP job configuration
   * @param aVertex the user defined vertex
   * @param acurrentSuperStepCounter the current super step*/
  @SuppressWarnings("unchecked")
  public AggregationContext(BSPJob ajobConf,
      Vertex<?, ?, ? extends Edge<?, ?>> aVertex, int acurrentSuperStepCounter) {
    this.jobConf = ajobConf;
    this.vertex = aVertex;
    this.outgoingEdgesClone = new ArrayList<Edge<?, ?>>(
        this.vertex.getAllEdges()).iterator();
    this.currentSuperStepCounter = acurrentSuperStepCounter;
    this.aggregateValues = new HashMap<String, AggregateValue>();
  }
  
  /**
   * The construct of AggregationContext for peer compute.
   * @param ajobConf
   *        BSP job configuration
   * @param aVertex
   *        the user defined vertex
   * @param acurrentSuperStepCounter
   *        the current super step
   */
  public AggregationContext(BSPJob ajobConf, BSPPeer apeer,
      int acurrentSuperStepCounter) {
    this.jobConf = ajobConf;
    this.peer = apeer;
    this.currentSuperStepCounter = acurrentSuperStepCounter;
    this.aggregateValues = new HashMap<String, AggregateValue>();
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
  public Iterator<Edge<?, ?>> getOutgoingEdges() {
    return this.outgoingEdgesClone;
  }

  @Override
  public int getOutgoingEdgesNum() {
    return this.vertex.getEdgesNum();
  }

  @Override
  public String getVertexID() {
    return this.vertex.getVertexID().toString();
  }

  @Override
  public String getVertexValue() {
    return this.vertex.getVertexValue().toString();
  }

  /**
   * Add an aggregate value (key-value).
   * @param  key the key of aggregate value
   * @param value AggregateValue
   */
  @SuppressWarnings("unchecked")
  public void addAggregateValues(String key, AggregateValue value) {
    this.aggregateValues.put(key, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public AggregateValue getAggregateValue(String name) {
    return this.aggregateValues.get(name);
  }

}
