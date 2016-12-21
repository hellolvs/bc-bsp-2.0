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

package com.chinamobile.bcbsp.bspstaff;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.RunJar;

/**
 * StaffRunner Base class that runs a staff in a child process.
 * @author
 * @version
 */

public class StaffRunner extends Thread {
  /**
   * The log in log4j,to write logs.
   */
  public static final Log LOG = LogFactory.getLog(StaffRunner.class);
  /**
   * if the staff should be killed.
   */
  private boolean killed = false;
  /**
   * Get if the staff should be killed.
   * @return true:the staff should be killed
   */
  public boolean isKilled() {
    return killed;
  }
  /**
   * Set if the staff should be killed.
   * @param killed set if the staff should be killed
   */
  public void setKilled(boolean killed) {
    this.killed = killed;
  }

  /**
   * The staff process.
   */
  private Process process;
  /**
   * The current BSP staff.
   */
  private Staff staff;
  /**
   * The current BSP Job configuration.
   */
  private BSPJob conf;
  /**
   * The current BSP worker manager.
   */
  private WorkerManager workerManager;
  /**
   * The fault step.
   */
  private int faultSSStep = 0;
  /**
   * Constructor of Staff runner.
   * @param bspStaff the current BSP staff
   * @param workerManager the current worker manager
   * @param conf the current BSP job configuration
   */
  public StaffRunner(BSPStaff bspStaff, WorkerManager workerManager
      , BSPJob conf) {
    this.staff = bspStaff;
    this.conf = conf;
    this.workerManager = workerManager;
  }
  /**
   * Get the current BSP staff.
   * @return the current BSP staff.
   */
  public Staff getStaff() {
    return staff;
  }
  /**
   * Get the fault step.
   * @return  the fault step
   */
  public int getFaultSSStep() {
    return faultSSStep;
  }
  /**
   * Set the fault step.
   * @param faultSSStep the fault step.
   */
  public void setFaultSSStep(int faultSSStep) {
    this.faultSSStep = faultSSStep;
  }

  /**
   * Called to assemble this staff's input. This method is run in the parent
   * process before the child is spawned. It should not execute user code, only
   * system code.
   * @return true: the staff is prepared
   * @throws IOException e
   */
  public boolean prepare() throws IOException {
    return true;
  }
  /**
   * Start to run a BSP staff.
   */
  public void run() {
    try {
      String sep = System.getProperty("path.separator");
      File workDir = new File(new File(staff.getJobFile()).getParent(), "work");
      boolean isCreated = workDir.mkdirs();
      if (!isCreated) {
        LOG.debug("StaffRunner.workDir : " + workDir);
      }
      StringBuffer classPath = new StringBuffer();
      classPath.append(System.getProperty("java.class.path"));
      classPath.append(sep);
      if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(this.conf.getJobType())) {
        String exe = conf.getJobExe();
        if (exe != null) {
          classPath.append(sep);
          classPath.append(exe);
          classPath.append(sep);
          classPath.append(workDir);
        }
      } else {
        String jar = conf.getJar();
        // if jar exists, it into workDir
        if (jar != null) {
          RunJar.unJar(new File(jar), workDir);
          File[] libs = new File(workDir, "lib").listFiles();
          if (libs != null) {
            for (int i = 0; i < libs.length; i++) {
              // add libs from jar to classpath
              classPath.append(sep);
              classPath.append(libs[i]);
            }
          }
          classPath.append(sep);
          classPath.append(new File(workDir, "classes"));
          classPath.append(sep);
          classPath.append(workDir);
        }
      }
      // Build exec child jmv args.
      Vector<String> vargs = new Vector<String>();
      File jvm = new File(new File(System.getProperty("java.home"), "bin"),
          "java");
      vargs.add(jvm.toString());

      // bsp.child.java.opts
      String javaOpts = conf.getConf().get("bsp.child.java.opts", "-Xmx200m");
      javaOpts = javaOpts.replace("@taskid@", staff.getStaffID().toString());
      String[] javaOptsSplit = javaOpts.split(" ");
      for (int i = 0; i < javaOptsSplit.length; i++) {
        vargs.add(javaOptsSplit[i]);
      }

      // Add classpath.
      vargs.add("-classpath");
      vargs.add(classPath.toString());

      // Setup the log4j prop
      long logSize = StaffLog.getStaffLogLength(((BSPConfiguration) conf
          .getConf()));
      vargs.add("-Dbcbsp.log.dir="
          + new File(System.getProperty("bcbsp.log.dir")).getAbsolutePath());
      vargs.add("-Dbcbsp.root.logger=INFO,TLA");

      LOG.info("debug: staff ID is " + staff.getStaffID());

      vargs.add("-Dbcbsp.tasklog.taskid=" + staff.getStaffID());
      vargs.add("-Dbcbsp.tasklog.totalLogFileSize=" + logSize);

      // Add main class and its arguments
      vargs.add(WorkerManager.Child.class.getName());
      InetSocketAddress addr = workerManager.getStaffTrackerReportAddress();

      vargs.add(addr.getHostName());
      vargs.add(Integer.toString(addr.getPort()));
      vargs.add(staff.getStaffID().toString());
      vargs.add(Integer.toString(getFaultSSStep()));
      vargs.add(workerManager.getHostName());
      vargs.add(this.conf.getJobType());

      // Run java
      runChild((String[]) vargs.toArray(new String[0]), workDir);
    } catch (Exception e) {
      LOG.error("[run]", e);
    }
  }

  /**
   * Run the child process.
   * @throws Exception e
   */
  private void runChild(String[] args, File dir) throws Exception {
    this.process = Runtime.getRuntime().exec(args, null, dir);
    try {
      int exit_code = process.waitFor();
      if (!killed && exit_code != 0) {
        throw new Exception("Staff process exit with nonzero status of "
            + exit_code + ".");
      }

    } catch (InterruptedException e) {
      throw new IOException(e.toString());
    } finally {
      kill();
    }
  }

  /**
   * Kill the child process.
   */
  public void kill() {
    if (process != null) {
      process.destroy();
    }
    killed = true;
  }
}
