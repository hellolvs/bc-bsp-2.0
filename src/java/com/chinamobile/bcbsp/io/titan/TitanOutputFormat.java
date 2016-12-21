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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;

/**
 * TitanOutputFormat implements the write strategy for BSP system to Titan
 * database.
 * @author Zhicheng Liu 2013/4/25
 */
public class TitanOutputFormat extends OutputFormat<Text, Text> {
  /**log handle in outputformat class*/
  private static final Log LOG = LogFactory.getLog(TitanOutputFormat.class);
  /**Titan client*/
  private RexsterClient client;
  /**configuration handle*/
  private Configuration conf;
  /**
   * construct method
   */
  public TitanOutputFormat() {
  }

  @Override
  public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
      StaffAttemptID staffID, Path path) throws IOException,
      InterruptedException {
    return new TitanRecordWriter(client);
  }

  @Override
  public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
      StaffAttemptID staffID) throws IOException, InterruptedException {
    return new TitanRecordWriter(client);
  }

  @Override
  public void initialize(Configuration configuration) {
    conf = new Configuration(configuration);
    conf.set("TITAN_SERVER_ADDRESS",
        configuration.get("titan.server.address", "localhost"));
    conf.set("TITAN_OUTPUT_TABLE_NAME",
        configuration.get("titan.output.table.name", "graph"));
    conf.set("HBASE_MASTER_ADDRESS",
        configuration.get("hbase.master.address", "localhost"));
    conf.set("HBASE_OUTPUT_TABLE_NAME",
        configuration.get("hbase.output.table.name", "titan"));
    try {
      client = RexsterClientFactory.open(conf.get("TITAN_SERVER_ADDRESS"),
          conf.get("TITAN_OUTPUT_TABLE_NAME"));
    } catch (Exception e) {
      LOG.error("The client of database can not obtain!");
      return;
    }
  }
}
