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

package com.chinamobile.bcbsp.workermanager;

import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.util.StaffStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Store workers' status information Encapsulate all of the information of
 * workers.
 */
public class WorkerManagerStatus implements Writable {
  /** Define Log variable for outputting messages */
  public static final Log LOG = LogFactory.getLog(WorkerManagerStatus.class);
  static {
    WritableFactories.setFactory(WorkerManagerStatus.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new WorkerManagerStatus();
          }
        });
  }
  /**
   * Define workerFaultList for storing worker's Type Level ,workerName
   * ,ExceptionMessage,JobName,StaffName.
   */
  private List<Fault> workerFaultList;
  /** The name of worker manager */
  private String workerManagerName;
  /** Define rpc */
  private String rpc;
  /** The list of staffStaus for reporting to BSPController */
  private List<StaffStatus> staffReports;
  /** The maximal number of staffs */
  private int maxStaffsCount;
  /** The count of running staffs */
  private int runningStaffsCount;
  /** The count of finished staffs */
  private int finishedStaffsCount;
  /** The count of failed staffs */
  private int failedStaffsCount;
  /**
   * Store the latest heartBeat interaction time of BSPController and
   * workerManager.
   */
  private volatile long lastSeen;
  /**
   * Store the time when BSPController and workerManager stop interacting.
   */
  private volatile long pauseTime;
  /** The name of host */
  private String host;
  /** The port of jetty server */
  private String httpPort;
  /** The IP address of worker */
  private String localIp;
  /** Judging that WorkerManagerStatus is fault or not */
  private boolean fault = false;

  /**
   * Constructor.
   * Init the list of StaffStatus and Fault.
   */
  public WorkerManagerStatus() {
    this.staffReports = new CopyOnWriteArrayList<StaffStatus>();
    this.workerFaultList = new ArrayList<Fault>();
  }

  /**
   * Constructor.
   * Call another constructor that has more parameters.
   * @param workerManagerName name of workerManager
   * @param staffReports list of link StaffStatus
   * @param maxStaffsCount the max count of staffs
   * @param runningStaffsCount  count of running staffs
   * @param finishedStaffsCount count of finished staffs
   * @param failedStaffsCount count of failed staffs
   * @param rpc server of rpc
   */
  public WorkerManagerStatus(String workerManagerName,
      List<StaffStatus> staffReports, int maxStaffsCount,
      int runningStaffsCount, int finishedStaffsCount, int failedStaffsCount,
      String rpc) {
    this(workerManagerName, staffReports, maxStaffsCount, runningStaffsCount,
        finishedStaffsCount, failedStaffsCount, rpc, new ArrayList<Fault>());
  }

  /**
   * Constructor.
   * Call another constructor that has more parameters.
   * @param workerManagerName name of workerManager
   * @param staffReports list of link StaffStatus
   * @param maxStaffsCount the max count of staffs
   * @param runningStaffsCount  count of running staffs
   * @param finishedStaffsCount count of finished staffs
   * @param failedStaffsCount count of failed staffs
   */
  public WorkerManagerStatus(String workerManagerName,
      List<StaffStatus> staffReports, int maxStaffsCount,
      int runningStaffsCount, int finishedStaffsCount, int failedStaffsCount) {
    this(workerManagerName, staffReports, maxStaffsCount, runningStaffsCount,
        finishedStaffsCount, failedStaffsCount, "");
  }

  /**
   * Constructor.
   * @param workerManagerName name of workerManager
   * @param staffReports list of link StaffStatus
   * @param maxStaffsCount the max count of staffs
   * @param runningStaffsCount  count of running staffs
   * @param finishedStaffsCount count of finished staffs
   * @param failedStaffsCount count of failed staffs
   * @param rpc server of rpc
   * @param workerFaultList list of fault
   */
  public WorkerManagerStatus(String workerManagerName,
      List<StaffStatus> staffReports, int maxStaffsCount,
      int runningStaffsCount, int finishedStaffsCount, int failedStaffsCount,
      String rpc, List<Fault> workerFaultList) {
    this.workerManagerName = workerManagerName;
    this.rpc = rpc;
    this.staffReports = new ArrayList<StaffStatus>(staffReports);
    this.maxStaffsCount = maxStaffsCount;
    this.runningStaffsCount = runningStaffsCount;
    this.finishedStaffsCount = finishedStaffsCount;
    this.failedStaffsCount = failedStaffsCount;
    this.workerFaultList = new ArrayList<Fault>(workerFaultList);
  }

  public void setStaffReports(List<StaffStatus> staffReports) {
    this.staffReports = new ArrayList<StaffStatus>(staffReports);
  }

  public void setMaxStaffsCount(int maxStaffsCount) {
    this.maxStaffsCount = maxStaffsCount;
  }

  public void setFinishedStaffsCount(int finishedStaffsCount) {
    this.finishedStaffsCount = finishedStaffsCount;
  }

  public void setWorkerFaultList(List<Fault> workerFaultList) {
    this.workerFaultList = new ArrayList<Fault>(workerFaultList);
  }

  public String getWorkerManagerName() {
    return this.workerManagerName;
  }

  public String getRpcServer() {
    return this.rpc;
  }

  public List<StaffStatus> getStaffReports() {
    return this.staffReports;
  }

  public int getMaxStaffsCount() {
    return this.maxStaffsCount;
  }

  public void setRunningStaffsCount(int runningStaffsCount) {
    this.runningStaffsCount = runningStaffsCount;
  }

  public int getRunningStaffsCount() {
    return this.runningStaffsCount;
  }

  public int getFinishedStaffsCount() {
    return this.finishedStaffsCount;
  }

  public void setFailedStaffsCount(int failedStaffsCount) {
    this.failedStaffsCount = failedStaffsCount;
  }

  public int getFailedStaffsCount() {
    return this.failedStaffsCount;
  }

  public void setLastSeen(long lastSeen) {
    this.lastSeen = lastSeen;
  }

  public long getLastSeen() {
    return lastSeen;
  }

  public void setPauseTime(long pauseTime) {
    this.pauseTime = pauseTime;
  }

  public long getPauseTime() {
    return this.pauseTime;
  }

  public List<Fault> getWorkerFaultList() {
    return workerFaultList;
  }

  public String getHost() {
    return this.host;
  }

  public String getHttpPort() {
    return this.httpPort;
  }

  public void setHttpPort(String httpPort) {
    this.httpPort = httpPort;
  }
  public void setHost(String host) {
    this.host = host;
  }

  public String getLocalIp() {
    return this.localIp;
  }

  public void setLocalIp(String localIp) {
    this.localIp = localIp;
  }

  /**
   * For BSPController to distinguish between different WorkerManagers, because
   * BSPController stores using WorkerManagerStatus as key.
   * @return the result of hashCode
   */
  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + workerManagerName.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object object) {
    WorkerManagerStatus wms = (WorkerManagerStatus) object;
    return wms.getWorkerManagerName()
        .equals(this.workerManagerName) ? true : false;
  }

  /*
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.workerManagerName = Text.readString(in);
    this.rpc = Text.readString(in);
    // add by cuiLI
    this.host = Text.readString(in);
    // this.host = "192.168.0.211";
    this.httpPort = Text.readString(in);
    this.localIp = Text.readString(in);
    int staffCount = in.readInt();
    StaffStatus status;
    this.staffReports.clear();
    for (int i = 0; i < staffCount; i++) {
      status = new StaffStatus();
      status.readFields(in);
      this.staffReports.add(status);
    }
    int workerFaultCount = in.readInt();
    Fault faultTemp;
    this.workerFaultList.clear();
    for (int i = 0; i < workerFaultCount; i++) {
      faultTemp = new Fault();
      faultTemp.readFields(in);
      this.workerFaultList.add(faultTemp);
    }
    this.maxStaffsCount = in.readInt();
    this.runningStaffsCount = in.readInt();
    this.finishedStaffsCount = in.readInt();
    this.failedStaffsCount = in.readInt();
  }

  /*
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.workerManagerName);
    Text.writeString(out, this.rpc);
    Text.writeString(out, this.host);
    Text.writeString(out, this.httpPort);
    Text.writeString(out, this.localIp);
    out.writeInt(this.staffReports.size());
    for (StaffStatus staffStatus : this.staffReports) {
      staffStatus.write(out);
    }
    out.writeInt(this.workerFaultList.size());
    for (Fault faultTemp : this.workerFaultList) {
      faultTemp.write(out);
    }
    out.writeInt(this.maxStaffsCount);
    out.writeInt(this.runningStaffsCount);
    out.writeInt(this.finishedStaffsCount);
    out.writeInt(this.failedStaffsCount);
  }

  /**
   * Judge the value of fault.
   * @return fault
   */
  public boolean isFault() {
    return this.fault;
  }

  /**
   * Set the value of fault.
   */
  public void setFault() {
    this.fault = true;
  }

  /**
   * For JUnit test.
   */
  public void setWorkerManagerName(String workerManagerName) {
    this.workerManagerName = workerManagerName;
  }
}
