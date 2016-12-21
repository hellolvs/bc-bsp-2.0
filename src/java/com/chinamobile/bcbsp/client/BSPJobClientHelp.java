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

package com.chinamobile.bcbsp.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Help user input right commands for running jobs.
 */
public class BSPJobClientHelp implements Tool {
  /**
   * Set job configuration information.
   * @param conf Configuration
   */
  @Override
  public void setConf(Configuration conf) {
  }

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      return -1;
    }
    String cmd = args[0];
    if ("jar".equals(cmd)) {
      System.out.print("\n");
      System.out
          .println(" COMMAND FORMAT:");
      System.out
          .println("     ========================================");
      System.out
          .println("       bcbsp jar <filename>.jar [arguments]");
      System.out
          .println("     ========================================");
      System.out
          .println(" Explanation:Submit a jar file to run a job on BCBSP");
      System.out.print("\n");
    } else if ("job".equals(cmd) && args.length == 1) {
      System.out.print("\n");
      System.out
          .println(" COMMAND FORMAT: ");
      System.out
          .println("     ========================================");
      System.out
          .println("        -list ");
      System.out
          .println("        -kill ");
      System.out
          .println("        -list-staffs ");
      System.out
          .println("        -setcheckpoint ");
      System.out
          .println("     ========================================");
      System.out.print("\n");
    } else if ("job".equals(cmd) && args.length == 2) {
      if (args[1].equals("-list")) {
        System.out.print("\n");
        System.out
            .println(" COMMAND FORMAT: ");
        System.out
            .println("     ========================================");
        System.out
            .println("       bcbsp job -list [all]");
        System.out
            .println("     ========================================");
        System.out
            .println(" Explanation:list current jobs or all history jobs");
        System.out.print("\n");
      }
      if (args[1].equals("-kill")) {
        System.out.print("\n");
        System.out
            .println(" COMMAND FORMAT: ");
        System.out
            .println("     ========================================");
        System.out
            .println("       bcbsp job -kill <job-id>");
        System.out
            .println("     ========================================");
        System.out
            .println(" Explanation:kill the job with JobID <job-id>");
        System.out.print("\n");
      }
      if (args[1].equals("-list-staffs")) {
        System.out.print("\n");
        System.out
            .println(" COMMAND FORMAT: ");
        System.out
            .println("     ========================================");
        System.out
            .println("       bcbsp job -list-staffs <job-id>");
        System.out
            .println("     ========================================");
        System.out
            .println(" Explanation:list all the TaskIDs of" +
            " the job with JobID <job-id>");
        System.out.print("\n");
      }
      if (args[1].equals("-setcheckpoint")) {
        System.out.print("\n");
        System.out
            .println(" COMMAND FORMAT: ");
        System.out
            .println("     ========================================");
        System.out
            .println("       bcbsp job -setcheckpoint <interval> <job-id>");
        System.out
            .println("     ========================================");
        System.out
            .println(" Explanation:set checkpoint interval " +
            "for the job with JobID <job-id>");
        System.out.print("\n");
      }
    } else if ("admin".equals(cmd) && args.length == 1) {
      System.out.print("\n");
      System.out
          .println(" COMMAND FORMAT: ");
      System.out
          .println("     ========================================");
      System.out
          .println("        -controller ");
      System.out
          .println("        -workers ");
      System.out
          .println("     ========================================");
      System.out.print("\n");
    } else if ("admin".equals(cmd) && args.length == 2) {
      if (args[1].equals("-master")) {
        System.out.print("\n");
        System.out
            .println(" COMMAND FORMAT: ");
        System.out
            .println("     ========================================");
        System.out
            .println("       bcbsp admin -controller");
        System.out
            .println("     ========================================");
        System.out
            .println(" Explanation:report controller's hostname,ip,state");
        System.out.print("\n");
      }
      if (args[1].equals("-workers")) {
        System.out.print("\n");
        System.out
            .println(" COMMAND FORMAT: ");
        System.out
            .println("     ========================================");
        System.out
            .println("       bcbsp admin -workers");
        System.out
            .println("     ========================================");
        System.out
            .println(" Explanation:report Worker's state");
        System.out.print("\n");
      }
    } else {
      System.out
          .println("COMMAND LIST");
      System.out
          .println("     ========================================");
      System.out
          .println("     bcbsp help job");
      System.out
          .println("     bcbsp help jar  ");
      System.out
          .println("     bcbsp help admin ");
      System.out
          .println("     ========================================");
    }
    return 0;
  }

  /**
   * Run BSPJobClientHelp for helping user inputing right command.
   * @param args command parameters
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new BSPJobClientHelp(), args);
    System.exit(res);
  }
}
