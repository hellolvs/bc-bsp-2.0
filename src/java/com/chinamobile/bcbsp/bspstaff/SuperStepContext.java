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
import com.chinamobile.bcbsp.util.BSPJob;

import java.util.HashMap;

/**
 * SuperStepContext This class implements {@link SuperStepContextInterface}.
 * Methods defined in the SuperStepContextInterface can be used by users. While
 * other methods only can be invoked by {@link BSPStaff}.
 * @author Bai Qiushi
 * @version 1.0
 */
public class SuperStepContext implements SuperStepContextInterface {
  /**
   * The BSP Job configuration.
   */
  private BSPJob jobConf;
  /**
   * The current super step counter.
   */
  private int currentSuperStepCounter;
  /**
   * The Aggregate values from the previous super step.
   */
  @SuppressWarnings("unchecked")
  private HashMap<String, AggregateValue> aggregateValues; 
  /**
   * The constructor.
   * @param ajobConf The BSP Job configuration.
   * @param acurrentSuperStepCounter The current super step counter
   */
  @SuppressWarnings("unchecked")
  public SuperStepContext(BSPJob ajobConf, int acurrentSuperStepCounter) {
    this.jobConf = ajobConf;
    this.currentSuperStepCounter = acurrentSuperStepCounter;
    this.aggregateValues = new HashMap<String, AggregateValue>();
  }

  /**
   * Add an aggregate value (key-value).
   * @param key the aggregateValue name
   * @param  value the aggregateValue value.
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

  @Override
  public int getCurrentSuperStepCounter() {
    return this.currentSuperStepCounter;
  }

  @Override
  public BSPJob getJobConf() {
    return this.jobConf;
  }

}
