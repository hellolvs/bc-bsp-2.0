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

import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordWriter;
//import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPTable;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Convert BCBSP Job output and write it to an HBase table.
 *
 * @param <Text>
 *        A type of sign
 */
public class TableOutputFormatBase<Text> extends OutputFormat<Text, Text> {
  /** Define a BSPTable */
  private HTable table;

  /**
   * Creates a new record writer.
   *
   * @param job
   *        The current job BSPJob.
   * @param staffId
   *        the unique name for this staff attempt.
   * @return The newly created writer instance.
   * @throws IOException
   *         When creating the writer fails.
   * @throws InterruptedException
   *         When the jobs is can celled.
   */
  @Override
  public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
      StaffAttemptID staffId) throws IOException, InterruptedException {
    return new TableRecordWriter<Text>(this.table);
  }

  /**
   * Set the {@link Path} of the output directory for the BC-BSP job.
   *
   * @param job
   *        The job configuration
   * @param outputDir
   *        the {@link Path} of the output directory for the BC-BSP job.
   * @throws InterruptedException
   * @throws IOException
   */
  public static void setOutputPath(BSPJob job, Path outputDir) {
  }

  /**
   * Get the {@link Path} to the output directory for the BC-BSP job.
   *
   * @param job
   *        The current job BSPJob.
   * @param staffId
   *        the unique name for this staff attempt.
   * @return the {@link Path} to the output directory for the BC-BSP job.
   */
  public static Path getOutputPath(BSPJob job, StaffAttemptID staffId) {
    return null;
  }

  /**
   * Checks if the output target exists.
   *
   * @param job
   *        The current job.
   * @param path
   *        The current output path.
   * @throws IOException
   *         When the check fails.
   * @throws InterruptedException
   *         When the job is aborted.
   * @see org.apache.hadoop.mapreduce.OutputFormat#checkOutputSpecs
   *        (org.apache.hadoop.mapreduce.JobContext)
   */
  public static void checkOutputSpecs(BSPJob job, Path path)
      throws IOException, InterruptedException {
    // TODO Check if the table exists?
  }

  public HTable getTable() {
    return table;
  }

  public void setTable(HTable table) {
    this.table = table;
  }

  @Override
  public RecordWriter<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text>
  getRecordWriter(
      BSPJob job, StaffAttemptID staffId, Path writePath) throws IOException,
      InterruptedException {
    return null;
  }
}
