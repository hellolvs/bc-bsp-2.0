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

package com.chinamobile.bcbsp.examples;

import com.chinamobile.bcbsp.api.Aggregator;

/**
 * SumOutEdgeNumAggregator An example implementation of Aggregator. To do the
 * sum operation on out edge number.
 */
public class SumOutEdgeNumAggregator extends
      Aggregator<AggregateValueOutEdgeNum> {
  /**
   * Implemented by the user.
   * @param aggValues
   *        AggregateValueOutEdgeNum
   * @return result the sum of aggregate value
   */
  @Override
  public AggregateValueOutEdgeNum aggregate(
      Iterable<AggregateValueOutEdgeNum> aggValues) {
    long sum = 0;
    for (AggregateValueOutEdgeNum aggValue : aggValues) {
      sum = sum + aggValue.getValue();
    }
    AggregateValueOutEdgeNum result = new AggregateValueOutEdgeNum();
    result.setValue(sum);
    return result;
  }
}
