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

package com.chinamobile.bcbsp.util;

import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * One "worker thread". Used to send data.
 */
public class ThreadSignle extends Thread {
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(ThreadSignle.class);
  /** the number of a thread */
  private int threadNumber;
  /** the status of the thread */
  private boolean status = false;
  /** The default a BytesWritable types of data */
  private BytesWritable data;
  /** agent worker */
  private WorkerAgentForStaffInterface worker = null;
  /** id of the job. */
  private BSPJobID jobId = null;
  /** id of this staff attempt */
  private StaffAttemptID taskId = null;
  /** This staff belongs to which partition */
  private int belongPartition = -1;
  /** A sign which difference between a C++ program and a Java program
   * 0 is Java program, 1 is C++ program
   */
  private int dataType = 0;

  /**
   * @param g A thread group
   * @param sn the number of a thread
   */
  public ThreadSignle(ThreadGroup g, int sn) {
    super(g, "Thread #" + sn);
    threadNumber = sn;
  }

  /**
   * @see java.lang.Thread#run()
   */
  public void run() {
    while (true) {
      while (!this.isStatus()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOG.error("[run]", e);
          return;
        }
      }
      if (this.dataType == 0) {
        LOG.info("java putHeadNode");
        worker.putHeadNode(jobId, taskId, belongPartition, this.data);
      } else {
        LOG.info("c++ putHeadNode");
        worker.putHeadNode(jobId, taskId, belongPartition, this.data, "c++");
      }
      this.data = null;
      this.worker = null;
      this.jobId = null;
      this.taskId = null;
      this.belongPartition = -1;
      this.setStatus(false);
    }
  }

  public synchronized boolean isStatus() {
    return status;
  }

  public int getThreadNumber() {
    return this.threadNumber;
  }

  /**Kills specified thread. */
  protected void kill() {
    this.interrupt();
  }

  public synchronized void setStatus(boolean status) {
    this.status = status;
  }

  public void setData(BytesWritable data) {
    this.data = data;
  }

  public void setWorker(WorkerAgentForStaffInterface worker) {
    this.worker = worker;
  }

  public void setJobId(BSPJobID jobId) {
    this.jobId = jobId;
  }

  public void setTaskId(StaffAttemptID taskId) {
    this.taskId = taskId;
  }

  public void setBelongPartition(int belongPartition) {
    this.belongPartition = belongPartition;
  }

  public int getDataType() {
    return dataType;
  }

  /**
   * @param dataType 0 or 1, 0 is Java program, 1 is C++ program
   */
  public void setDataType(int dataType) {
    this.dataType = dataType;
    LOG.info("in setDataType type is " + this.dataType);
  }
}
