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

import org.apache.hadoop.io.Text;

/**
 * Edge implementation with type of String.
 */
public class StringEdge extends Edge<String, String> {
  /** Init the ID of vertex */
  private String vertexID = null;
  /** Init the edgeValue */
  private String edgeValue = null;

  /**
   * Split the edgeData.
   * @param edgeData complex edge data
   */
  @Override
  public void fromString(String edgeData) throws Exception {
    StringTokenizer str = new StringTokenizer(edgeData, Constants.SPLIT_FLAG);
    if (str.countTokens() != 2) {
      throw new Exception();
    }
    this.vertexID = str.nextToken();
    this.edgeValue = str.nextToken();
  }

  @Override
  public String getEdgeValue() {
    return this.edgeValue;
  }

  @Override
  public String getVertexID() {
    return this.vertexID;
  }

  /**
   * Transform vertexID and edgeValue to string.
   * @return vertexID and edgeValue
   */
  @Override
  public String intoString() {
    return this.vertexID + Constants.SPLIT_FLAG + this.edgeValue;
  }

  @Override
  public void setEdgeValue(String edgeValue) {
    this.edgeValue = edgeValue;
  }

  @Override
  public void setVertexID(String vertexID) {
    this.vertexID = vertexID;
  }

  /**
   * Deserialize vertexID and edgeValue.
   * @param in vertexID or edgeValue
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = Text.readString(in);
    this.edgeValue = Text.readString(in);
  }

  /**
   * Serialize vertexID and edgeValue.
   * @param out vertexID or edgeValue
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.vertexID);
    Text.writeString(out, this.edgeValue);
  }

  /**
   * Judge the different edge.
   * @param object the type of edge
   * @return true or false
   */
  @Override
  public boolean equals(Object object) {
    StringEdge edge = (StringEdge) object;
    if (this.vertexID.equals(edge.getVertexID())) {
      return true;
    }
    return false;
   // return this.vertexID.equals(edge.getVertexID()) ? true : false;
  }

  /**
   * Override hashCode().
   * @return vertexID
   */
  public int hashCode() {
    return Integer.valueOf(this.vertexID).hashCode();
  }
}
