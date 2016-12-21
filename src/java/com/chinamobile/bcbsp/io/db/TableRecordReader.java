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

import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBScan;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPTable;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.mortbay.log.Log;

/**
 * Iterate over an HBase table data, return (ImmutableBytesWritable, Result)
 * pairs.
 */
public class TableRecordReader extends RecordReader<Text, Text> {
  /** Define a TableRecordReaderImpl */
  private TableRecordReaderImpl recordReaderImpl = new TableRecordReaderImpl();
  /** Define a variable, key is Vertex */
  private Text key = new Text();
  /** Define a variable, key is outEdge */
  private Text value = new Text();

  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow
   *        The first row to start at.
   * @throws IOException
   *         When restarting fails.
   */
  public void restart(byte[] firstRow) throws IOException {
    this.recordReaderImpl.restart(firstRow);
  }

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   *
   * @throws IOException
   *         When restarting the scan fails.
   */
  public void init() throws IOException {
    this.recordReaderImpl.init();
  }

  /**
   * Sets the HBase table.
   *
   * @param table
   *        The {@link HTable} to scan.
   */
  // public void setHTable(HTable htable) {
  // this.recordReaderImpl.setHTable(htable);
  // }
  public void setHTable(HTable table) {
    this.recordReaderImpl.setTable(table);
  }

  /**
   * Sets the scan defining the actual details like columns etc.
   *
   * @param scan
   *        The scan to set.
   */
  // public void setScan(Scan scan) {
  // this.recordReaderImpl.setScan(scan);
  // }
  public void setScan(Scan scan) {
    this.recordReaderImpl.setScan(scan);
  }

  /**
   * Closes the split.
   *
   * @see org.apache.hadoop.mapreduce.RecordReader#close()
   */
  @Override
  public void close() {
    this.recordReaderImpl.close();
  }

  /**
   * Returns the current key.
   *
   * @return The current key.
   * @throws IOException
   * @throws InterruptedException
   *         When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
   */
  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
//    StringTokenizer str = new StringTokenizer(new String(this.recordReaderImpl
//        .getCurrentKey().get()), "\t");
//    if (str.hasMoreElements()) {
//      key.set(str.nextToken());
//      Log.info("key = " + key);
//    }
//    if (str.hasMoreElements()) {
//      value.set(str.nextToken());
//      Log.info("value = " + value);
//    }
    key.set(new String(this.recordReaderImpl.getCurrentKey().get()));
    return key;
  }

  /**
   * Returns the current value.
   *
   * @return The current value.
   * @throws IOException
   *         When the value is faulty.
   * @throws InterruptedException
   *         When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
   */
  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    value.set(new String(this.recordReaderImpl.getCurrentValue().getValue(
        "BorderNode".getBytes(), "nodeData".getBytes())));
    return value;
  }

  /**
   * Initializes the reader.
   *
   * @param inputsplit
   *        The split to work with.
   * @param conf
   *        The current task conf.
   * @throws IOException
   *         When setting up the reader fails.
   * @throws InterruptedException
   *         When the job is aborted.
   */
  @Override
  public void initialize(InputSplit inputsplit, Configuration conf)
      throws IOException, InterruptedException {
  }

  /**
   * Positions the record reader to the next record.
   *
   * @return <code>true</code> if there was another record.
   * @throws IOException
   *         When reading the record failed.
   * @throws InterruptedException
   *         When the job was aborted.
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return this.recordReaderImpl.nextKeyValue();
  }

  /**
   * The current progress of the record reader through its data.
   *
   * @return A number between 0.0 and 1.0, the fraction of the data read.
   */
  @Override
  public float getProgress() {
    return this.recordReaderImpl.getProgress();
  }
}
