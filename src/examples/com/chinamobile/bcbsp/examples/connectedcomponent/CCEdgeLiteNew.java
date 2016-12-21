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

package com.chinamobile.bcbsp.examples.connectedcomponent;

import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * PageRank edgeValue.
 */

public class CCEdgeLiteNew extends Edge<Integer, Byte> {
  /** vertex ID */
  private int vertexID = 0;

  @Override
  public void fromString(String edgeData) throws Exception {
    StringTokenizer str = new StringTokenizer(edgeData, Constants.SPLIT_FLAG);
    if (str.countTokens() != 2) {
      throw new Exception();
    }
    this.vertexID = Integer.valueOf(str.nextToken());
  }

  @Override
  public Byte getEdgeValue() {
    return 0;
  }

  @Override
  public Integer getVertexID() {
    return this.vertexID;
  }

  @Override
  public String intoString() {
    return this.vertexID + Constants.SPLIT_FLAG + "0";
  }

  @Override
  public void setEdgeValue(Byte edgeValue) {
  }

  @Override
  public void setVertexID(Integer vertexID) {
    this.vertexID = vertexID;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.vertexID);
  }

  @Override
  public boolean equals(Object object) {
    CCEdgeLiteNew edge = (CCEdgeLiteNew) object;
    return this.vertexID == edge.getVertexID();
  }
}
