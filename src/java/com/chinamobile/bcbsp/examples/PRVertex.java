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

/**
 * Vertex implementation for PageRank.
 */
public class PRVertex extends Vertex<Integer, Float, PREdge> {
  /** Init the ID of vertex */
  private int vertexID = 0;
  /** Init the vertexValue */
  private float vertexValue = 0.0f;
  /** Init the edgesList */
  private List<PREdge> edgesList = new ArrayList<PREdge>();

  /**
   * Add edge to edgesList.
   * @param edge
   *        PREdge
   */
  public void addEdge(PREdge edge) {
    this.edgesList.add(edge);
  }

  /**
   * Split the vertexData.
   * @param vertexData
   *        complex vertex data
   */
  public void fromString(String vertexData) throws Exception {
    String[] buffer = new String[2];
    StringTokenizer str = new StringTokenizer(vertexData,
        Constants.KV_SPLIT_FLAG);
    if (str.hasMoreElements()) {
      buffer[0] = str.nextToken();
    } else {
      throw new Exception();
    }
    if (str.hasMoreElements()) {
      buffer[1] = str.nextToken();
    }
    str = new StringTokenizer(buffer[0], Constants.SPLIT_FLAG);
    if (str.countTokens() != 2) {
      throw new Exception();
    }

    this.vertexID = Integer.valueOf(str.nextToken());
    this.vertexValue = Float.valueOf(str.nextToken());
    // There has edges.
    if (buffer[1].length() > 1) {
      str = new StringTokenizer(buffer[1], Constants.SPACE_SPLIT_FLAG);
      while (str.hasMoreTokens()) {
        PREdge edge = new PREdge();
        edge.fromString(str.nextToken());
        this.edgesList.add(edge);
      }
    }
  }

  public List<PREdge> getAllEdges() {
    return this.edgesList;
  }

  public int getEdgesNum() {
    return this.edgesList.size();
  }

  public Integer getVertexID() {
    return this.vertexID;
  }

  public Float getVertexValue() {
    return this.vertexValue;
  }

  /**
   * Transform vertexID and vertexValue to String.
   * @return buffer is used to store vertexID and vertexValue
   */
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
   * Delete edge from edgesList.
   * @param edge
   *        PREdge
   */
  public void removeEdge(PREdge edge) {
    this.edgesList.remove(edge);
  }

  public void setVertexID(Integer vertexID) {
    this.vertexID = vertexID;
  }

  public void setVertexValue(Float vertexValue) {
    this.vertexValue = vertexValue;
  }

  /**
   * Update edge from edgesList.
   * @param edge
   *        PREdge
   */
  public void updateEdge(PREdge edge) {
    removeEdge(edge);
    this.edgesList.add(edge);
  }

  /**
   * Deserialize vertexID and edgeValue,edgesList.
   * @param in
   *        vertexID or edgeValue
   */
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readInt();
    this.vertexValue = in.readFloat();
    this.edgesList.clear();
    int numEdges = in.readInt();
    PREdge edge;
    for (int i = 0; i < numEdges; i++) {
      edge = new PREdge();
      edge.readFields(in);
      this.edgesList.add(edge);
    }
  }

  /**
   * Serialize vertexID and edgeValue,edgesList.
   * @param out
   *        vertexID or edgeValue
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.vertexID);
    out.writeFloat(this.vertexValue);
    out.writeInt(this.edgesList.size());
    for (PREdge edge : edgesList) {
      edge.write(out);
    }
  }

  /**
   * Override hashCode().
   * @return vertexID
   */
  public int hashCode() {
    return Integer.valueOf(this.vertexID).hashCode();
  }
}
