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

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * AggregateValueOutEdgeNum An example implementation of AggregateValue.
 */
public class AggregateValueOutEdgeNum extends AggregateValue<Long, BSPMessage> {
  /**Define the number of outgoingEdge*/
  private Long outgoingEdgeNum;

  /**
   * Constructor
   */
  public AggregateValueOutEdgeNum() {
  }

  public Long getValue() {
    return this.outgoingEdgeNum;
  }
  public void setValue(Long value) {
    this.outgoingEdgeNum = value;
  }

  /**
   * Transform outgoingEdgeNum to string.
   * @return outgoingEdgeNum
   */
  public String toString() {
    return String.valueOf(this.outgoingEdgeNum);
  }

  /**
   * Transform outgoingEdgeNum to int.
   * @param outNum outgoingEdgeNum
   */
  public void initValue(String outNum) {
    this.outgoingEdgeNum = Long.valueOf(outNum);
  }

  /**
   * Get the count of outgoingEdgeNum.
   * @param messages BSPMessage
   * @param context AggregationContextInterface
   */
  public void initValue(Iterator<BSPMessage> messages,
      AggregationContextInterface context) {
    this.outgoingEdgeNum = (long) context.getOutgoingEdgesNum();
  }

  /**
   * Serialize the outgoingEdgeNum.
   * @param in outgoingEdgeNum
   */
  public void readFields(DataInput in) throws IOException {
    this.outgoingEdgeNum = in.readLong();
  }

  /**
   * Deserialize the outgoingEdgeNum.
   * @param out outgoingEdgeNum
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.outgoingEdgeNum);
  }
}
