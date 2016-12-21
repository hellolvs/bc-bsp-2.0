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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import com.chinamobile.bcbsp.io.InputFormat;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.util.BSPJob;
import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;

/**
 * TitanInputFormat implements input split strategy of table in Titan database.
 * @author Zhicheng Liu 2013/4/25
 */
public class TitanInputFormat extends InputFormat<Text, Text> {
  /**log handle*/
  private static final Log LOG = LogFactory.getLog(TitanInputFormat.class);
  /**HBase property handle*/
  private HTable hTable;
  /**Titan client*/
  private RexsterClient client;
  /**configuration handle*/
  private Configuration conf;

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit input,
      BSPJob job) throws IOException, InterruptedException {
    /*Get the record reader for every given split. */
    TitanTableSplit split = (TitanTableSplit) input;
    TitanRecordReader reader = null;
    try {
      reader = new TitanRecordReader();
      reader.setClient(client);
      reader.setFirstVertexID(split.getFirstVertexID());
      reader.setLastVertexID(split.getLastVertexID());
      reader.init();
    } catch (Exception e) {
      LOG.error("Can not create a RecordReader for Titan");
      return null;
    }
    return reader;
  }

  @Override
  public List<InputSplit> getSplits(BSPJob job) throws IOException,
      InterruptedException {
    /*Get the input splits for given table in Titan database.*/
    ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
    int splitNum = hTable.getRegionLocations().size();
    String ids = null;
    try {
      ids = client.execute("g.V.vertexID").toString();
    } catch (NumberFormatException e) {
      LOG.error("Can not get the number of vertex in database!");
      return null;
    } catch (RexProException e) {
      LOG.error("Client of database collapse!");
      return null;
    }
    String[] vertexIDs = ids.split(", ");
    vertexIDs[0] = vertexIDs[0].split("\\[")[1];
    vertexIDs[vertexIDs.length - 1] = vertexIDs[vertexIDs.length - 1]
        .split("\\]")[0];
    long smallestVertexID = 0;
    long largestVertexID = 0;
    for (int i = 0; i < vertexIDs.length; i++) { // test only
      if (Long.parseLong(vertexIDs[i]) < smallestVertexID) {
        smallestVertexID = Long.parseLong(vertexIDs[i]);
      }
      if (Long.parseLong(vertexIDs[i]) > largestVertexID) {
        largestVertexID = Long.parseLong(vertexIDs[i]);
      }
    }
    long blockLength = largestVertexID / splitNum;
    long index = smallestVertexID;
    while (index <= largestVertexID) {
      TitanTableSplit split = new TitanTableSplit();
      split.setTableName(conf.get("TITAN_INPUT_TABLE_NAME"));
      split.setFirstVertexID(index);
      index += blockLength;
      if (index > largestVertexID) {
        index = largestVertexID;
      }
      split.setLastVertexID(index);
      splits.add(split);
      index++;
    }
    return splits;
  }

  @Override
  public void initialize(Configuration configuration) {
  /*The configuration is Initialization "titan.xml"*/
    conf = new Configuration(configuration);
    conf.set("TITAN_SERVER_ADDRESS",
        configuration.get("titan.server.address", "localhost"));
    conf.set("TITAN_INPUT_TABLE_NAME",
        configuration.get("titan.input.table.name", "graph"));
    conf.set("HBASE_MASTER_ADDRESS",
        configuration.get("hbase.master.address", "localhost"));
    conf.set("HBASE_INPUT_TABLE_NAME",
        configuration.get("hbase.input.table.name", "titan"));
    try {
      client = RexsterClientFactory.open(conf.get("TITAN_SERVER_ADDRESS"),
          conf.get("TITAN_INPUT_TABLE_NAME"));
    } catch (Exception e) {
      LOG.error("The client of database can not obtain!");
      return;
    }
    conf = HBaseConfiguration.create(conf);
    try {
      hTable = new HTable(conf, conf.get("HBASE_INPUT_TABLE_NAME"));
    } catch (IOException e) {
      LOG.error("The table in HBase can not obtain");
      return;
    }
  }
}
