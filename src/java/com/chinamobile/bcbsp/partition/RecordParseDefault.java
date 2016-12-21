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

package com.chinamobile.bcbsp.partition;

import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * RecordParseDefault This class is used to parse a record as a HeadNode object.
 */
public class RecordParseDefault extends RecordParse {
  /**The log of the class.*/
  private static final Log LOG = LogFactory.getLog(RecordParseDefault.class);
  /**The vertex to be Parse.*/
  private Class<? extends Vertex<?, ?, ?>> vertexClass;
  /**
   * @param key The key of the vertex
   * @param value The value of the vertex
   * @return The vertex of the key and value
   */
  public Vertex recordParse(String key, String value) {
    Vertex vertex = null;
    try {
      vertex = vertexClass.newInstance();
      vertex.fromString(key + Constants.KV_SPLIT_FLAG + value);
    } catch (Exception e) {
      LOG.error("RecordParse", e);
      return null;
    }
    return vertex;
  }
  /**
   * This method is used to parse a record and obtain VertexID .
   * @param key The key of the vertex record
   * @return the vertex id
   */
  public Text getVertexID(Text key) {
    try {
      StringTokenizer str = new StringTokenizer(key.toString(),
          Constants.SPLIT_FLAG);
      if (str.countTokens() != 2) {
        return null;
      }
      return new Text(str.nextToken());
    } catch (Exception e) {
      return null;
    }
  }
  /**
   * This method is used to initialize the RecordParseDefault.
   * @param job The Bsp job.
   */
  public void init(BSPJob job) {
    this.vertexClass = job.getVertexClass();
  }
}
