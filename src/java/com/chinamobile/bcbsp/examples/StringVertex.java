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

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

/**
 * Vertex implementation with type of String.
 */
public class StringVertex extends Vertex<String, String, StringEdge> {
  /** Init the ID of vertex */
  private String vertexID = null;
  /** Init the vertexValue */
  private String vertexValue = null;
  /** Init the edgesList */
  private List<StringEdge> edgesList = new ArrayList<StringEdge>();

  /**
   * Add edge to edgesList.
   * @param edge
   *        PREdge
   */
  @Override
  public void addEdge(StringEdge edge) {
    this.edgesList.add(edge);
  }

  /**
   * Split the vertexData.
   * @param vertexData
   *        complex vertex data
   */
  @Override
  public void fromString(String vertexData) throws Exception {
    String[] buffer = new String[2];
    StringTokenizer str = new StringTokenizer(vertexData,
        Constants.KV_SPLIT_FLAG);
    if (str.countTokens() != 2) {
      throw new Exception();
    }
    buffer[0] = str.nextToken();
    buffer[1] = str.nextToken();
    str = new StringTokenizer(buffer[0], Constants.SPLIT_FLAG);
    if (str.countTokens() != 2) {
      throw new Exception();
    }
    this.vertexID = str.nextToken();
    this.vertexValue = str.nextToken();
    // There has edges.
    if (buffer.length > 1) {
      str = new StringTokenizer(buffer[1], Constants.SPACE_SPLIT_FLAG);
      while (str.hasMoreTokens()) {
        StringEdge edge = new StringEdge();
        edge.fromString(str.nextToken());
        this.edgesList.add(edge);
      }
    }
  }

  @Override
  public List<StringEdge> getAllEdges() {
    return this.edgesList;
  }

  @Override
  public String getVertexID() {
    return this.vertexID;
  }

  @Override
  public String getVertexValue() {
    return this.vertexValue;
  }

  /**
   * Transform vertexID and vertexValue to String
   * @return buffer is used to store vertexID and vertexValue
   */
  @Override
  public String intoString() {
    String buffer = vertexID + Constants.SPLIT_FLAG + vertexValue;
    buffer = buffer + Constants.KV_SPLIT_FLAG;
    int numEdges = edgesList.size();
    if (numEdges != 0) {
      buffer = buffer + edgesList.get(0).intoString();
    }
    for (int i = 1; i < numEdges; i++) {
      buffer = buffer + Constants.SPACE_SPLIT_FLAG +
          edgesList.get(i).intoString();
    }
    return buffer;
  }

  /**
   * Delete edge from edgesList
   * @param edge
   *        PREdge
   */
  @Override
  public void removeEdge(StringEdge edge) {
    this.edgesList.remove(edge);
  }

  @Override
  public void setVertexID(String vertexID) {
    this.vertexID = vertexID;
  }

  @Override
  public void setVertexValue(String vertexValue) {
    this.vertexValue = vertexValue;
  }

  /**
   * Update edge from edgesList
   * @param edge
   *        PREdge
   */
  @Override
  public void updateEdge(StringEdge edge) {
    removeEdge(edge);
    this.edgesList.add(edge);
  }

  /**
   * Deserialize vertexID and edgeValue,edgesList
   * @param in
   *        vertexID or edgeValue
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = Text.readString(in);
    this.vertexValue = Text.readString(in);
    this.edgesList.clear();
    int numEdges = in.readInt();
    StringEdge edge;
    for (int i = 0; i < numEdges; i++) {
      edge = new StringEdge();
      edge.readFields(in);
      this.edgesList.add(edge);
    }
  }

  /**
   * Serialize vertexID and edgeValue,edgesList
   * @param out
   *        vertexID or edgeValue
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.vertexID);
    Text.writeString(out, this.vertexValue);
    out.writeInt(this.edgesList.size());
    for (StringEdge edge : edgesList) {
      edge.write(out);
    }
  }

  /**
   * Override hashCode().
   * @return vertexID
   */
  @Override
  public int hashCode() {
    return vertexID.hashCode();
  }

  @Override
  public int getEdgesNum() {
    return this.edgesList.size();
  }
}
