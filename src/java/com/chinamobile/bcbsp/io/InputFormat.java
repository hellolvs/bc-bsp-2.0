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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * InputFormat This is an abstract class. All user-defined InputFormat class
 * must implement two methods:getSplits() and createRecordReader();
 */
public abstract class InputFormat<K, V> {

  /**
   * This method is used for generating splits according to the input data. The
   * list of split will be used by JobInProgress, SimpleTaskScheduler and Staff.
   *
   * @param job The current BSPJob job.
   * @return input splits
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract List<InputSplit> getSplits(BSPJob job) throws IOException,
      InterruptedException;

  /**
   * This method will return a user-defined RecordReader for reading data from
   * the original storage. It is used in Staff.
   *
   * @param split The split to work with.
   * @param job The current BSPJob job.
   * @return The newly created record reader.
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract RecordReader<K, V> createRecordReader(InputSplit split,
      BSPJob job) throws IOException, InterruptedException;

  /**
   * This method is only used to read data from HBase. If the data is read from
   * the DFS you do not cover it. This method is primarily used to initialize
   * the HBase table and set Scan
   *
   * @param configuration The current job configuration file.
   */
  public void initialize(Configuration configuration) {
  }

}
