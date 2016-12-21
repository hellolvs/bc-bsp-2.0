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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import com.chinamobile.bcbsp.io.RecordReader;
import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;

/**
 * TitanRecordReader implements how to read data from Titan database.
 * @author Zhicheng Liu 2013/4/25
 */
public class TitanRecordReader extends RecordReader<Text, Text> {
  /**log handle*/
  private static final Log LOG = LogFactory.getLog(TitanRecordReader.class);
  /**Titan client*/
  private RexsterClient client;
  /**hadoop IO key*/
  private Text key;
  /**hadoop IO value*/
  private Text value;
  /**file firstVertex ID*/
  private long firstVertexID;
  /**file lastVertex ID*/
  private long lastVertexID;
  /**current handle vertex ID*/
  private long currentVertexID;
  /**
   * Titan record reader construct method
   */
  public TitanRecordReader() {
  }

  /**
   * Initialze current vertex ID.  
   */
  public void init() {
    currentVertexID = firstVertexID;
  }

  /**
   * set titan client.
   * @param client
   *        RexsterClient to set.
   */
  public void setClient(RexsterClient client) {
    this.client = client;
  }

  /**
   * set the first VertexID.
   * @param id
   *        vertex id to set.
   */
  public void setFirstVertexID(long id) {
    firstVertexID = id;
  }

  /**
   * set last vertex ID.
   * @param id
   *        vertex ID to set.
   */
  public void setLastVertexID(long id) {
    lastVertexID = id;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void initialize(InputSplit input, Configuration conf)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    while (currentVertexID <= lastVertexID) {
      if ((key = getKey(currentVertexID)) != null) {
        value = getValue(currentVertexID);
        currentVertexID++;
        return true;
      }
      currentVertexID++;
    }
    return false;
  }

  /**
   * get vertexID for hadoop IO
   * @param vertexID
   *        vertexID to get
   * @return if get from titan,return hadoop text
   *         else null.
   */
  private Text getKey(long vertexID) {
    List<Map<String, Object>> result = null;
    try {
      result = client.execute("g.V('vertexID','" + vertexID + "').map");
      if (result.toString().length() == 2) {
        return null;
      }
    } catch (RexProException e) {
      LOG.error("Client of database collapse!");
      return null;
    } catch (IOException e) {
      LOG.error("Database can not read record!");
      return null;
    }
    String vertex = result.toString();
    String[] tmp = vertex.split(",");
    String value = tmp[0].split("=")[1];
    String id = tmp[1].split("=")[1].split("\\}\\]")[0];
    return new Text(id + ":" + value);
  }

  /**
   * get vertex value from titan for hadoop IO
   * @param vertexID
   *        vertexID to get
   * @return vertex value for hadoop IO
   */
  private Text getValue(long vertexID) {
    List<Map<String, Object>> result = null;
    try {
      result = client.execute("g.V('vertexID','" + vertexID +
          "').out('outgoing').vertexID");
      if (result.toString().length() == 2) {
        return null;
      }
    } catch (RexProException e) {
      LOG.error("Client of database collapse!");
      return null;
    } catch (IOException e) {
      LOG.error("Database can not read record!");
      return null;
    }
    String str = result.toString();
    String[] outgoingVertexIDs;
    if (str.indexOf(",") == -1) {
      outgoingVertexIDs = new String[1];
      outgoingVertexIDs[0] = str.split("\\[")[1].split("\\]")[0];
    } else {
      String[] vertexInfo = str.split(", ");
      outgoingVertexIDs = new String[vertexInfo.length];
      outgoingVertexIDs[0] = vertexInfo[0].split("\\[")[1];
      for (int i = 1; i < vertexInfo.length - 1; i++) {
        outgoingVertexIDs[i] = vertexInfo[i];
      }
      outgoingVertexIDs[vertexInfo.length - 1] =
          vertexInfo[vertexInfo.length - 1]
          .split("\\]")[0];
    }
    try {
      result = client.execute("g.V('vertexID','" + vertexID + "').outE.weight");
    } catch (RexProException e) {
      LOG.error("Client of database collapse!");
      return null;
    } catch (IOException e) {
      LOG.error("Database can not read record!");
      return null;
    }
    str = result.toString();
    String[] outgoingVertexWeight;
    if (str.indexOf(",") == -1) {
      outgoingVertexWeight = new String[1];
      outgoingVertexWeight[0] = str.split("\\[")[1].split("\\]")[0];
    } else {
      String[] weightInfo = str.split(", ");
      outgoingVertexWeight = new String[weightInfo.length];
      outgoingVertexWeight[0] = weightInfo[0].split("\\[")[1];
      for (int i = 1; i < weightInfo.length - 1; i++) {
        outgoingVertexWeight[i] = weightInfo[i];
      }
      outgoingVertexWeight[weightInfo.length - 1] =
          weightInfo[weightInfo.length - 1]
          .split("\\]")[0];
    }
    StringBuffer edge = new StringBuffer();
    for (int i = 0; i < outgoingVertexIDs.length; i++) {
      edge.append(outgoingVertexIDs[i] + ":" + outgoingVertexWeight[i] + " ");
    }
    return new Text(edge.toString());
  }
}
