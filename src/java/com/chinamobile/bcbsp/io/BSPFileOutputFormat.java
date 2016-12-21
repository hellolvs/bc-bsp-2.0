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

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPHdfs;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.Constants;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * BSPFileOutputFormat This class is used for writing on the file system, such
 * as HDFS.
 */
public abstract class BSPFileOutputFormat<K, V> extends OutputFormat<K, V> {
  /** Define LOG for outputting log information. */
  private static final Log LOG = LogFactory.getLog(BSPFileOutputFormat.class);

  /**
   * If the output directory has existed, then delete it.
   *
   * @param job
   *        the current BSPJob job.
   * @param outputDir
   *        the {@link Path} of the output directory for the BC-BSP job.
   */
  public static void checkOutputSpecs(BSPJob job, Path outputDir) {
    try {
      // FileSystem fileSys = FileSystem.get(job.getConf());
      // alter by gtt
      BSPHdfs checkout = new BSPHdfsImpl();
      // if (fileSys.exists(outputDir)) {
      // fileSys.delete(outputDir, true);
      // }
      if (checkout.hdfscheckOutputSpecs(job).exists(outputDir)) {
        checkout.hdfscheckOutputSpecs(job).delete(outputDir, true);
      }
    } catch (IOException e) {
      LOG.error("[checkOutputSpecs]", e);
    }
  }

  /**
   * Set the {@link Path} of the output directory for the BC-BSP job.
   *
   * @param job
   *        the current BSPJob job.
   * @param outputDir
   *        the {@link Path} of the output directory for the BC-BSP job.
   */
  public static void setOutputPath(BSPJob job, Path outputDir) {
    Configuration conf = job.getConf();
    checkOutputSpecs(job, outputDir);
    conf.set(Constants.USER_BC_BSP_JOB_OUTPUT_DIR, outputDir.toString());
  }

  /**
   * Get the {@link Path} to the output directory for the BC-BSP job.
   *
   * @param job
   *        the current BSPJob job.
   * @param staffId
   *        id of this staff attempt.
   * @return the {@link Path} to the output directory for the BC-BSP job.
   */
  public static Path getOutputPath(BSPJob job, StaffAttemptID staffId) {
    String name = job.getConf().get(Constants.USER_BC_BSP_JOB_OUTPUT_DIR) +
        "/" + "staff-" + staffId.toString().substring(26, 32);
    return name == null ? null : new Path(name);
  }

  /**
   * Get the {@link Path} to the output directory for the BC-BSP job.
   *
   * @param staffId
   *        id of this staff attempt
   * @param writePath
   *        the {@link Path} of the checkpoint directory for the BC-BSP job.
   * @return the checkpoint directory name for the BC-BSP job.
   */
  public static Path getOutputPath(StaffAttemptID staffId, Path writePath) {
    String name = writePath.toString() + "/" + staffId.toString() +
        "/checkpoint.cp";
    return name == null ? null : new Path(name);
  }
}
