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

/**
 * SuperStepContextInterface This is a context for init before each super step.
 * @author Bai Qiushi
 * @version 1.0
 */
public interface SuperStepContextInterface {

  /**
   * Get the current superstep counter.
   * @return the current superstep counter
   */
  int getCurrentSuperStepCounter();

  /**
   * Get the BSP Job Configuration.
   * @return the BSP Job Configuration.
   */
  BSPJob getJobConf();
  
  /**
   * User interface to get an aggregate value aggregated from the previous super
   * step.
   * @param name
   * @return the aggregate value
   */
  @SuppressWarnings("unchecked") AggregateValue getAggregateValue(String name);
}
