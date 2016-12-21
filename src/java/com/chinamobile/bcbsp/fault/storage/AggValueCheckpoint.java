/**
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

package com.chinamobile.bcbsp.fault.storage;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * a AggValueCheckpoint is used to backup the aggValues during the local compute
 * @author Feng
 */
public class AggValueCheckpoint {
  /**handle log file in the class*/
  private static final Log LOG = LogFactory.getLog(AggValueCheckpoint.class);
  /**AggValueCheckpoint construct method
   * @param job
   *        job's aggregate values to write.
   */
  public AggValueCheckpoint(BSPJob job) {
  }
  /**
   * write the aggVlaues of staffs during local compute.
   * @param aggValues
   *        aggregate values
   * @param writeAggPath
   *        aggregate values write path.
   * @param job
   *        job to write
   * @param staff
   *        staff aggregate values to write
   * @return write result.
   * @throws IOException exceptions during write into hdfs.
   */
  public boolean writeAggCheckPoint(String aggValues, Path writeAggPath,
      BSPJob job, Staff staff) throws IOException {
    LOG.info("The aggValue Checkpoint write path is : " +
        writeAggPath.toString());
    try {
      OutputFormat outputformat = (OutputFormat) ReflectionUtils.newInstance(
          job.getConf().getClass(Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS,
              OutputFormat.class), job.getConf());
      outputformat.initialize(job.getConf());
      RecordWriter output = outputformat.getRecordWriter(job,
          staff.getStaffAttemptId(), writeAggPath);
      output.write(new Text(Constants.SSC_SPLIT_FLAG), new Text(aggValues));
      output.close(job);
    } catch (Exception e) {
      LOG.error("Exception has happened and been catched!", e);
      return false;
    }
    return true;
  }
}
