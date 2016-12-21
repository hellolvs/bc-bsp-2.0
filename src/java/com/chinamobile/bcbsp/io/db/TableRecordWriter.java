/**
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

package com.chinamobile.bcbsp.io.db;

import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBPut;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPTable;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl.BSPHBHConnectionManagerImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl.BSPHBPutImpl;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;
import org.mortbay.log.Log;
/**
 * Writes the output to an HBase table.
 *
 * @param <Text>
 *        The type of the key.
 */
public class TableRecordWriter<Text> extends RecordWriter<Text, Text> {
  //private HTable table;
  /** The table to write to. */
  private HTable table;

  /**
   * Instantiate a TableRecordWriter with the HBase HClient for writing.
   *
   * @param table
   *        The table to write to.
   */
  // public TableRecordWriter(HTable table) {
  // this.table = table;
  // }
  public TableRecordWriter(HTable table) {
    this.table = table;
  }

  /**
   * Closes the writer, in this case flush table commits.
   *
   * @param job The current job BSPJob.
   */
  @Override
  public void close(BSPJob job) throws IOException {
    table.flushCommits();
    // The following call will shutdown all connections to the cluster
    // from
    // this JVM. It will close out our zk session otherwise zk wil log
    // expired sessions rather than closed ones. If any other HTable
    // instance
    // running in this JVM, this next call will cause it damage.
    // Presumption
    // is that the above this.table is only instance.
    // HConnectionManager.deleteAllConnections(true);
    HConnectionManager.deleteAllConnections(true);
  }

  /**
   * Writes a key/value pair into the table.
   *
   * @param key
   *        The key.
   * @param value
   *        The value.
   * @throws IOException
   *         When writing fails.
   * @see com.chinamobile.bcbsp.io.RecordWriter#write(java.lang.Object,
   *      java.lang.Object)
   */
  @Override
  public void write(Text key, Text value) throws IOException {
    Log.info("key = " + key.toString());
    Log.info("value = " + value.toString());
//    String key = "";
//    String value1 = "";
//    LOG.info("keyValue = " +keyValue);
//    StringTokenizer str = new StringTokenizer(keyValue.toString(), "\t");
//    if (str.hasMoreElements()) {
//      key = str.nextToken();
//      LOG.info("key = " +key);
//    }
//    if (str.hasMoreElements()) {
////      value.set(str.nextToken());
//      value1 = str.nextToken();
//      LOG.info("Value = " +value1);
//    }
//    Put put=new Put(key.toString().getBytes());
////    BSPHBPut put = new BSPHBPutImpl(key.toString().getBytes());
//    put.add("BorderNode".getBytes(), "nodeData".getBytes(), value1.getBytes());
    Put put=new Put(key.toString().getBytes());
    put.add("BorderNode".getBytes(),"nodeData".getBytes(), value.toString().getBytes());
    this.table.put(put);
  }

  @Override
  public void write(Text keyValue) throws IOException,
      InterruptedException {
    String key = "";
    String value = "";
    StringTokenizer str = new StringTokenizer(keyValue.toString(), "\t");
    if (str.hasMoreElements()) {
      key = str.nextToken();  
    }
    Put put=new Put(key.toString().getBytes());
    if (str.hasMoreElements()) {
//      value.set(str.nextToken());
      value = str.nextToken(); 
    }
    put.add("BorderNode".getBytes(),"nodeData".getBytes(), value.toString().getBytes());
    this.table.put(put);
  }
}
