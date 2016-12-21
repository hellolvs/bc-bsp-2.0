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

package com.chinamobile.bcbsp.examples.kmeans;

import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * KMEdge Implementation of Edge for K-Means.
 */
public class KMEdge extends Edge<Byte, Float> {
  /** vertex ID */
  private byte vertexID = 0;
  /** edge value */
  private float edgeValue = 0;

  @Override
  public void fromString(String edgeData) {
    String[] buffer = edgeData.split(Constants.SPLIT_FLAG);
    this.vertexID = Byte.valueOf(buffer[0]);
    this.edgeValue = Float.valueOf(buffer[1]);
  }

  @Override
  public Float getEdgeValue() {
    return this.edgeValue;
  }

  @Override
  public Byte getVertexID() {
    return this.vertexID;
  }

  @Override
  public String intoString() {
    return this.vertexID + Constants.SPLIT_FLAG + this.edgeValue;
  }

  @Override
  public void setEdgeValue(Float arg0) {
    this.edgeValue = arg0;
  }

  @Override
  public void setVertexID(Byte arg0) {
    this.vertexID = arg0;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readByte();
    this.edgeValue = in.readFloat();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(this.vertexID);
    out.writeFloat(edgeValue);
  }

  @Override
  public boolean equals(Object object) {
    KMEdge edge = (KMEdge) object;
    return this.vertexID == edge.getVertexID();
  }

  /**
   * Override hashCode().
   * @return vertexID
   */
  public int hashCode() {
    return Integer.valueOf(this.vertexID).hashCode();
  }
}
