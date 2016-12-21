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

package com.chinamobile.bcbsp.io;

import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * OutputFormat This is an abstract class. All user-defined OutputFormat class
 * must implement the methods:getRecordWriter();
 */
public abstract class OutputFormat<K, V> {

  /**
   * Get the {@link RecordWriter} for the given staff.
   *
   * @param job
   *        the information about the current staff.
   * @param staffId
   *        the id of this staff attempt.
   * @return a {@link RecordWriter} to write the output for the job.
   * @throws IOException
   */
  public abstract RecordWriter<K, V> getRecordWriter(BSPJob job,
      StaffAttemptID staffId) throws IOException, InterruptedException;

  /**
   * Get the {@link RecordWriter} for the given staff.
   *
   * @param job
   *        the information about the current staff.
   * @param staffId
   *        the id of this staff attempt.
   * @param writePath
   *        output path.
   * @return a {@link RecordWriter} to write the output for the job.
   * @throws IOException
   */
  public abstract RecordWriter<Text, Text> getRecordWriter(BSPJob job,
      StaffAttemptID staffId, Path writePath) throws IOException,
      InterruptedException;

  /**
   * This method is only used to write data into HBase. If the data is wrote
   * into the DFS you do not cover it. This method is primarily used to
   * initialize the HBase table.
   *
   * @param otherConf The current job configuration file.
   */
  public void initialize(Configuration otherConf) {
  }
}
