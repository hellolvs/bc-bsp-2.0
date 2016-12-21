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

package com.chinamobile.bcbsp.io.titan;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;

/**
 * TitanRecordWriter implements how to write data to Titan database.
 * @author Zhicheng Liu 2013/4/25
 */
public class TitanRecordWriter extends RecordWriter<Text, Text> {
  /** log handle in recordwriter */
  private static final Log LOG = LogFactory.getLog(TitanRecordWriter.class);
  /** Titan client */
  private RexsterClient client;

  /**
   * record construce method.
   * @param client
   *        titan client to set.
   */
  public TitanRecordWriter(RexsterClient client) {
    this.client = client;
  }

  @Override
  public void close(BSPJob job) throws IOException, InterruptedException {
    client.close();
  }

  @Override
  public void write(Text key, Text value) throws IOException,
      InterruptedException {
    if (key == null) {
      return;
    }
    String[] vertexInfo = key.toString().split(":");
    String vertexID = vertexInfo[0];
    String vertexValue = vertexInfo[1];
    if (value == null) {
      try {
        if (!hasVertex(vertexID)) {
          client.execute("g.addVertex([vertexID:'" + vertexID + "', value:'" +
              vertexValue + "'])");
        } else {
          client.execute("g.V('vertexID','" + vertexID +
              "').sideEffect{it.value = '" + vertexValue + "'}");
        }
      } catch (RexProException e) {
        LOG.error("Can not write record to database!");
        return;
      }
      return;
    }
    String[] strs = value.toString().split(" ");
    String[] outgoingVertexIDs = new String[strs.length];
    String[] weights = new String[strs.length];
    for (int i = 0; i < strs.length; i++) {
      String[] str = strs[i].split(":");
      outgoingVertexIDs[i] = str[0];
      weights[i] = str[1];
    }
    try {
      if (!hasVertex(vertexID)) {
        client.execute("g.addVertex([vertexID:'" + vertexID + "', value:'" +
            vertexValue + "'])");
      } else {
        client.execute("g.V('vertexID','" + vertexID +
            "').sideEffect{it.value = '" + vertexValue + "'}");
      }
      for (int i = 0; i < outgoingVertexIDs.length; i++) {
        if (!hasVertex(outgoingVertexIDs[i])) {
          client.execute("g.addVertex([vertexID:'" + outgoingVertexIDs[i] +
              "', value:''])");
        } /*
           * else { client.execute("g.V('vertexID','" + outgoingVertexIDs[i] +
           * "')"); }
           */
        client.execute("g.addEdge(g.V('vertexID','" + vertexID +
            "').next(), g.V('vertexID','" + outgoingVertexIDs[i] +
            "').next(), 'outgoing', [weight:" + weights[i] + "])");
      }
    } catch (RexProException e) {
      LOG.error("Can not write record to database!");
      return;
    }
  }

  @Override
  public void write(Text keyValue) throws IOException,
      InterruptedException {
    Text key = new Text();
    Text value = new Text();
    StringTokenizer str1 = new StringTokenizer(keyValue.toString(), "\t");
    if (str1.hasMoreElements()) {
      key.set(str1.nextToken());
    }
    if (str1.hasMoreElements()) {
      value.set(str1.nextToken());
    }
    if (key == new Text()) {
      return;
    }
    String[] vertexInfo = key.toString().split(":");
    String vertexID = vertexInfo[0];
    String vertexValue = vertexInfo[1];
    if (value == new Text()) {
      try {
        if (!hasVertex(vertexID)) {
          client.execute("g.addVertex([vertexID:'" + vertexID + "', value:'" +
              vertexValue + "'])");
        } else {
          client.execute("g.V('vertexID','" + vertexID +
              "').sideEffect{it.value = '" + vertexValue + "'}");
        }
      } catch (RexProException e) {
        LOG.error("Can not write record to database!");
        return;
      }
      return;
    }
    String[] strs = value.toString().split(" ");
    String[] outgoingVertexIDs = new String[strs.length];
    String[] weights = new String[strs.length];
    for (int i = 0; i < strs.length; i++) {
      String[] str = strs[i].split(":");
      outgoingVertexIDs[i] = str[0];
      weights[i] = str[1];
    }
    try {
      if (!hasVertex(vertexID)) {
        client.execute("g.addVertex([vertexID:'" + vertexID + "', value:'" +
            vertexValue + "'])");
      } else {
        client.execute("g.V('vertexID','" + vertexID +
            "').sideEffect{it.value = '" + vertexValue + "'}");
      }
      for (int i = 0; i < outgoingVertexIDs.length; i++) {
        if (!hasVertex(outgoingVertexIDs[i])) {
          client.execute("g.addVertex([vertexID:'" + outgoingVertexIDs[i] +
              "', value:''])");
        } /*
           * else { client.execute("g.V('vertexID','" + outgoingVertexIDs[i] +
           * "')"); }
           */
        client.execute("g.addEdge(g.V('vertexID','" + vertexID +
            "').next(), g.V('vertexID','" + outgoingVertexIDs[i] +
            "').next(), 'outgoing', [weight:" + weights[i] + "])");
      }
    } catch (RexProException e) {
      LOG.error("Can not write record to database!");
      return;
    }
  }

  /**
   *  Check whether there is a vertex which having given id in Titan database.
   * @param id
   *        vertexID to have.
   * @return if have true,
   *         else false.
   */
  private boolean hasVertex(String id) {
    List<Map<String, Object>> result = null;
    try {
      result = client.execute("g.V('vertexID','" + id + "').vertexID");
    } catch (RexProException e) {
      LOG.error("RexProException: Can not read data from database");
    } catch (IOException e) {
      LOG.error("IOException: Can not read data from database");
    }
    if (result.toString().length() <= 2) {
      return false;
    } else {
      return true;
    }
  }
}
