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

package com.chinamobile.bcbsp.pipes;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.client.BSPJobClient;
import com.chinamobile.bcbsp.io.BSPFileInputFormat;
import com.chinamobile.bcbsp.io.BSPFileOutputFormat;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**Class Submitter to start a c++ job.*/
public class Submitter extends Configured implements Tool {
  /**A log variable for user to write log.*/
  protected static final Log LOG = LogFactory.getLog(Submitter.class);
  /**The method to parse the arguments.
   * @param args the parameters user input
   * @throws Exception e
   * @return 0*/
  public int run(String[] args) throws Exception {
    int exitCode = -1;
    BSPConfiguration conf = new BSPConfiguration();
    LOG.info("after new conf bcbsp.log.dir is: " + conf.get("bcbsp.log.dir"));
    if (args.length < 1) {
      displayUsage();
      return exitCode;
    }
    String cmd = args[0];
    Path userJobConfig = null;
    if ("-submit".equals(cmd)) {
      if (args.length == 6) {
        userJobConfig = new Path(args[5]);
        conf.addResource(userJobConfig);
      } else {
        displayUsage();
        return exitCode;
      }
    }
    conf.set(Constants.USER_BC_BSP_JOB_EXE, new Path(args[1]).toString());
    conf.set(Constants.USER_BC_BSP_JOB_TYPE, "C++");

    BSPJob bsp = new BSPJob(conf);
    bsp.getClass();
    bsp.setGraphDataVersion(1);
    bsp.getClass();
    bsp.setMessageQueuesVersion(1);

    bsp.setJobExe(args[1]);
    bsp.setNumSuperStep(Integer.parseInt(args[2]));
    bsp.setNumBspStaff(2);
    bsp.setNumPartition(2);

    Class inputClass = Class.forName(conf.get("job.inputformat.class",
        "com.chinamobile.bcbsp.io.KeyValueBSPFileInputFormat"));
    Class outputClass = Class.forName(conf.get("job.outputformat.class",
        "com.chinamobile.bcbsp.io.TextBSPFileOutputFormat"));

    bsp.setInputFormatClass(inputClass);
    bsp.setOutputFormatClass(outputClass);
    BSPFileInputFormat.addInputPath(bsp, new Path(args[3]));
    BSPFileOutputFormat.setOutputPath(bsp, new Path(args[4]));

    bsp.setPartitionerClass((Class<? extends Partitioner>) Class.forName(conf
        .get("job.partitioner.class",
            "com.chinamobile.bcbsp.partition.HashPartitioner")));
    bsp.setRecordParse((Class<? extends RecordParse>) Class.forName(conf.get(
        "job.recordparse.class",
        "com.chinamobile.bcbsp.partition.RecordParseDefault")));
    LOG.info("BEFORE bspjobclient bcbsp.log.dir is : "
        + bsp.getConf().get("bcbsp.log.dir"));
    BSPJobClient.runJob(bsp);
    return 0;
  }
  /**Display the usage.*/
  public void displayUsage() {
    System.err
        .println("Usage:bcbsp pipes -submit <FileName.exe> <nSupersteps>"
            + " <DataInputPath> <DataOutputPath> <JobConfigurationFilePath>");
  }
  /**Get class according the class name.
   * @param key the key
   * @param conf BSP job configuration
   * @param cls class type
   * @param <InterfaceType> the interface type
   * @return a class
   * @throws ClassNotFoundException if didn't found the class*/
  @SuppressWarnings("unused")
  private static <InterfaceType> Class<? extends InterfaceType> getClass(
      String key, BSPConfiguration conf, Class<InterfaceType> cls)
      throws ClassNotFoundException {
    return conf.getClassByName(key).asSubclass(cls);
  }
  /**
   * The main function to start the BSP job.
   * @throws Exception e
   * @param args the arguments */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Submitter(), args);
    System.exit(res);
  }
}
