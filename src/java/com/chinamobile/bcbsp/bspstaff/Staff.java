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

import com.chinamobile.bcbsp.graph.GraphDataFactory;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

//import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Staff Base class for Staff.
 * @author
 * @version
 */
public abstract class Staff implements Writable {
  /**
   * The current BSP Job id.
   */
  private BSPJobID jobId;

  /**
   * The current job file.
   */
  private String jobFile;
  /**
   * The current job file path in local.
   */
  private String jobExeLocalPath = "no job";
  /**
   * The current Staff id.
   */
  private StaffAttemptID sid;
  /**
   * The current partition.
   */
  private int partition = 0;
  /**
   * Recovery counter.
   */
  private int failCounter = 0;
  /**
   * The graph data factory for graph data.
   */
  private GraphDataFactory graphDataFactory;

  // protected LocalDirAllocator lDirAlloc;
  // route table
  /**
   * HasshMap Key is partition id,value is the workerManager name and port.
   */
  private HashMap<Integer, String> partitionToWorkerManagerNameAndPort
  = new HashMap<Integer, String>();

  /**
   * Get new BSP Job Configuration and staff id.
   */
  public Staff() {
    setJobId(new BSPJobID());
    sid = new StaffAttemptID();
  }

  /**
   * Get the graph data factory.
   * @return GraphDataFactory
   */
  public GraphDataFactory getGraphDataFactory() {
    return graphDataFactory;
  }

  /**
   * The constructor.
   * @param jobId the current BSP job id
   * @param jobFile the current BSP job executable file
   * @param sid the current Staff id
   * @param partition the current partition id
   * @param splitClass The split class
   * @param split the split on HDFS
   */
  public Staff(BSPJobID jobId, String jobFile, StaffAttemptID sid,
      int partition, String splitClass, BytesWritable split) {
    this.setJobId(jobId);
    this.jobFile = jobFile;
    this.sid = sid;
    this.partition = partition;
  }

  /**
   * Set BSP Job file.
   * @param jobFile the current BSP job executable file
   */
  public void setJobFile(String jobFile) {
    this.jobFile = jobFile;
  }

  /**
   * Get BSP job file.
   * @return the current BSP job executable file.
   */
  public String getJobFile() {
    return jobFile;
  }

  /**
   * Get the current BSP job executable locally.
   * @return The current BSP job executable locally
   */
  public String getJobExeLocalPath() {
    return jobExeLocalPath;
  }

  /**
   * Set the current BSP job executable locally.
   * @param jobExeLocalPath the BSP job executable file locally
   */
  public void setJobExeLocalPath(String jobExeLocalPath) {
    this.jobExeLocalPath = jobExeLocalPath;
  }

  /**
   * Get the current Staff attempt id.
   * @return The current Staff attempt id.
   */
  public StaffAttemptID getStaffAttemptId() {
    return this.sid;
  }

  /**
   * Get the current Staff id.
   * @return the current Staff id.
   */
  public StaffAttemptID getStaffID() {
    return sid;
  }

  /**
   * Set the current Fail counter.
   * @param failCounter job fail times.
   */
  public void setFailCounter(int failCounter) {
    this.failCounter = failCounter;
  }

  /**
   * Get the current Fail counter.
   * @return job fail times.
   */
  public int getFailCounter() {
    return this.failCounter;
  }

  /**
   * Get the job name for this task.
   * @return the job name
   */
  public BSPJobID getJobID() {
    return getJobId();
  }

  /**
   * Get the index of this task within the job.
   * @return the integer part of the task id
   */
  public int getPartition() {
    return partition;
  }

  @Override
  public String toString() {
    return sid.toString();
  }

  /**
   * Set the Partition to workerManager name and port.
   * @param partitionToWorkerManagerNameAndPort partition
   */
  public void setPartitionToWorkerManagerNameAndPort(
      HashMap<Integer, String> partitionToWorkerManagerNameAndPort) {
    this.partitionToWorkerManagerNameAndPort = partitionToWorkerManagerNameAndPort;
  }

  /**
   * Get the Partition to workerManager name and port.
   * @return Partition to workerManager name and port
   */
  public HashMap<Integer, String> getPartitionToWorkerManagerNameAndPort() {
    return this.partitionToWorkerManagerNameAndPort;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    getJobId().write(out);
    Text.writeString(out, jobFile);
    sid.write(out);
    out.writeInt(partition);
    out.writeInt(this.failCounter);
    Text.writeString(out, jobExeLocalPath);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    getJobId().readFields(in);
    jobFile = Text.readString(in);
    sid.readFields(in);
    partition = in.readInt();
    this.failCounter = in.readInt();
    jobExeLocalPath = Text.readString(in);
  }

  /**
   * Run this staff as a part of the named job. This method is executed in the
   * child process.
   * @param umbilical
   *        for progress reports
   * @param job the BSP job configuration
   * @param task The current local staff
   * @param recovery True: the staff is a recovery staff;
   *                 false: the staff is the normal staff
   * @param migrateSuperStep The migrate super step counter
   * @param failCounter The fail counter.
   * @param hostName the current host name of the current node
   * @param changeWorkerState The worker state
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public abstract void run(BSPJob job, Staff task,
      WorkerAgentProtocol umbilical, boolean recovery,
      boolean changeWorkerState, int migrateSuperStep, int failCounter,
      String hostName) throws IOException, ClassNotFoundException,
      InterruptedException;

  /**
   * Run this staff as a part of the named job. This method is executed in the
   * child process. c++
   * @param umbilical
   *        for progress reports
   * @param task The current local staff
   * @param job the BSP job configuration
   * @param hostName the current host name of the current node
   * @param failCounter The fail counter.
   * @param recovery True: the staff is a recovery staff;
   *                 false: the staff is the normal staff
   * @param changeWorkerState The worker state
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public abstract void runC(BSPJob job, Staff task,
      WorkerAgentProtocol umbilical, boolean recovery,
      boolean changeWorkerState, int failCounter, String hostName)
      throws IOException, ClassNotFoundException, InterruptedException;

  /**
   * Create a BSP Staff runner.
   * @param groom the workerManager.
   * @return  BSP Staff runner
   */
  public abstract BSPStaffRunner createRunner(WorkerManager groom);

  /**
   * Set a staff done.
   * @param umbilical Protocol that staff child process uses to contact
   *  its parent process.
   * @throws IOException
   */
  public void done(WorkerAgentProtocol umbilical) throws IOException {
    umbilical.done(getStaffID(), true);
  }

  /**
   * Get BSP job configuration.
   * @return BSO job configuration
   */
  public abstract BSPJob getConf();

  /**
   * Set BSP job local configuration.
   * @param localJobConf The local configuration
   */
  public abstract void setConf(BSPJob localJobConf);
  // add by Chen for c++

  /**
   * Judge if the staff is a recovery staff.
   * @param job BSP job configuration
   * @param staff The BSP staff
   * @param workerAgent Protocol that staff child process uses to contact
   *  its parent process.
   * @return true:the staff is a recovery staff;false: the staff is not
   *  a recovery staff
   */
  public abstract boolean recovery(BSPJob job, Staff staff,
      WorkerAgentProtocol workerAgent);

  /**
   * Get the recovery times.
   * @return recovery times
   */
  public abstract int getRecoveryTimes();

  /**
   * Get the current BSP Job id.
   * @return the current BSP Job id
   */
  public BSPJobID getJobId() {
    return jobId;
  }

  /**
   * Set the current BSP Job id.
   * @param ajobId The current BSP Job name.
   */
  public void setJobId(BSPJobID ajobId) {
    this.jobId = ajobId;
  }

  /**
   * Get the current Staff id.
   * @return the current Staff id
   */
  public StaffAttemptID getSid() {
    return sid;
  }

  /**
   * Set the current Staff id.
   * @param asid The current Staff name
   */
  public void setSid(StaffAttemptID asid) {
    this.sid = asid;
  }

  /**
   * Set the partition id.
   * @param apartition  partition id
   */
  public void setPartition(int apartition) {
    this.partition = apartition;
  }

  /**
   * Set the Graph data factory.
   * @param agraphDataFactory Graph data factory.
   */
  public void setGraphDataFactory(GraphDataFactory agraphDataFactory) {
    this.graphDataFactory = agraphDataFactory;
  }
  
  /**
   * Run the java bspstaff for the partition-centre compute for machine learning or graph.
   * @param umbilical
   *        for progress reports
   * @param task The current local staff
   * @param job the BSP job configuration
   * @param hostName the current host name of the current node
   * @param failCounter The fail counter.
   * @param recovery True: the staff is a recovery staff;
   *                 false: the staff is the normal staff
   * @param changeWorkerState The worker state
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public abstract void runPartition(BSPJob job, Staff staff,
      WorkerAgentProtocol umbilical, boolean recovery,
      boolean changeWorkerState, int migrateStep, int failCounter2,
      String hostName);
  
}
