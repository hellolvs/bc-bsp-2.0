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
 * AggregateValueVertexNum An example implementation of AggregateValue.
 */
public class AggregateValueVertexNum extends AggregateValue<Long, BSPMessage> {
  /** State the count of vertex */
  private Long vertexNum;

  public Long getValue() {
    return this.vertexNum;
  }

  /**
   * Transform vertexNum to long.
   * @param verNum vertexNum
   */
  public void initValue(String verNum) {
    this.vertexNum = Long.valueOf(verNum);
  }

  /**
   * Get the count of vertexNum.
   * @param messages BSPMessage
   * @param context AggregationContextInterface
   */
  public void initValue(Iterator<BSPMessage> messages,
      AggregationContextInterface context) {
    this.vertexNum = 1L;
  }

  public void setValue(Long value) {
    this.vertexNum = value;
  }

  /**
   * Deserialize the vertexNum.
   * @param in vertexNum
   */
  public void readFields(DataInput in) throws IOException {
    this.vertexNum = in.readLong();
  }

  /**
   * Serialize the vertexNum.
   * @param out vertexNum
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.vertexNum);
  }

  /**
   * Transform vertexNum to string.
   * @return vertexNum
   */
  public String toString() {
    return String.valueOf(this.vertexNum);
  }
}
