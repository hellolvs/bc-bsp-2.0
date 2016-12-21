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

import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Edge implementation for PageRank.
 */
public class SSPEdge extends Edge<Integer, Integer> {
  /** Init the ID of vertex */
  private int vertexID = 0;
  /** Init the edgeValue */
  private int edgeValue = 0;

  /**
   * Split the edgeData
   * @param edgeData complex edge data
   */
  public void fromString(String edgeData) throws Exception {
    StringTokenizer str = new StringTokenizer(edgeData, Constants.SPLIT_FLAG);
    if (str.countTokens() != 2) {
      throw new Exception();
    }
    this.vertexID = Integer.valueOf(str.nextToken());
    this.edgeValue = Integer.valueOf(str.nextToken());
  }

  public Integer getEdgeValue() {
    return this.edgeValue;
  }

  public Integer getVertexID() {
    return this.vertexID;
  }

  /**
   * Transform vertexID and edgeValue to string.
   * @return vertexID and edgeValue
   */
  public String intoString() {
    return this.vertexID + Constants.SPLIT_FLAG + this.edgeValue;
  }

  public void setEdgeValue(Integer edgeValue) {
    this.edgeValue = edgeValue;
  }

  public void setVertexID(Integer vertexID) {
    this.vertexID = vertexID;
  }

  /**
   * Deserialize vertexID and edgeValue.
   * @param in vertexID or edgeValue
   */
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readInt();
    this.edgeValue = in.readInt();
  }

  /**
   * Serialize vertexID and edgeValue.
   * @param out vertexID or edgeValue
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.vertexID);
    out.writeInt(edgeValue);
  }

  /**
   * Judge the different edge.
   * @param object the type of edge
   * @return true or false
   */
  public boolean equals(Object object) {
    SSPEdge edge = (SSPEdge) object;
    if (this.vertexID == edge.getVertexID()) {
      return true;
    }
    return false;
    //return this.vertexID == edge.getVertexID() ? true : false;
  }

  /**
   * Override hashCode().
   * @return vertexID
   */
  public int hashCode() {
    return Integer.valueOf(this.vertexID).hashCode();
  }
}
