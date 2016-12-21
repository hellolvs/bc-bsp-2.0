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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;

/**
 * JobStatus Describes the current status of a job.
 */
public class JobStatus implements Writable, Cloneable {
  /** Define LOG for outputting log information */
  public static final Log LOG = LogFactory.getLog(JobStatus.class);
  /** 1 the job is running */
  public static final int RUNNING = 1;
  /** 2 the job is succeeded */
  public static final int SUCCEEDED = 2;
  /** 3 the job is failed */
  public static final int FAILED = 3;
  /** 4 the job is prep */
  public static final int PREP = 4;
  /** 5 the job is killed */
  public static final int KILLED = 5;
  /** 6 the job is recovery */
  public static final int RECOVERY = 6;
  static {
    WritableFactories.setFactory(JobStatus.class, new WritableFactory() {
      public Writable newInstance() {
        return new JobStatus();
      }
    });
  }
  /** job state */
  public static enum State {
    /** job state value */
    RUNNING(1), SUCCEEDED(2), FAILED(3), PREP(4), KILLED(5), RECOVERY(6);
    /** Initialize a variable */
    private int s;
    /** Set job state
     *
     * @param s Job state value.
     */
    State(int s) {
      this.s = s;
    }
    /** Set job state
    *
    * @return Job state value.
    */
    public int value() {
      return this.s;
    }
  }
  /** id of the job. */
  private BSPJobID jobid;
  /** The current progress of the job */
  private long progress;
  /** Reset progress of the job */
  private long cleanupProgress;
  /** Updata progress of the job */
  private long setupProgress;
  /** runState in enum */
  private volatile State state;
  /** The current runState of the job */
  private int runState;
  /** The start time of the job */
  private long startTime;
//  private String schedulingInfo = "NA";
  /** user of the person who submitted the job. */
  private String user;
  /** The current superstepCount of the job */
  private long superstepCount;
  /** The finish time of the job */
  private long finishTime;
  /** A sign of recovery */
  private boolean recovery = false;
  /** mapcounter to update the status of counters */
  private MapWritable mapcounter = new MapWritable();
  /** mapStaffStatus to update staffstatus */
  private MapWritable mapStaffStatus = new MapWritable();

  /**
   * Construct
   */
  public JobStatus() {
  }

//  public JobStatus(BSPJobID jobid, String user, long progress, int runState) {
//    this(jobid, user, progress, 0, runState);
//  }

//  public JobStatus(BSPJobID jobid, String user, long progress,
//      long cleanupProgress, int runState) {
//    this(jobid, user, 0, progress, cleanupProgress, runState);
//  }

//  public JobStatus(BSPJobID jobid, String user, long setupProgress,
//      long progress, long cleanupProgress, int runState) {
//    this(jobid, user, 0, progress, cleanupProgress, runState, 0);
//  }

  /**
   * Construct
   *
   * @param jobid
   *        id of the job.
   * @param user
   *        user of the person who submitted the job.
   * @param setupProgress
   *        updata progress of the job.
   * @param progress
   *        the current progress of the job.
   * @param cleanupProgress
   *        reset progress of the job.
   * @param runState
   *        the current runState of the job.
   * @param superstepCount
   *        the current superstepCount of the job.
   */
  public JobStatus(BSPJobID jobid, String user, long setupProgress,
      long progress, long cleanupProgress, int runState, long superstepCount) {
    this.jobid = jobid;
    this.setupProgress = setupProgress;
    this.progress = progress;
    this.cleanupProgress = cleanupProgress;
    this.runState = runState;
    this.state = State.values()[runState - 1];
    this.superstepCount = superstepCount;
    this.user = user;
  }

  public BSPJobID getJobID() {
    return jobid;
  }

  /**
   * @return current progress of the job
   */
  public synchronized long progress() {
    return progress;
  }

  /**
   * update staff status.
   *
   * @param ss current statu of the staff.
   */
  public synchronized void updateStaffStatus(StaffStatus ss) {
    this.mapStaffStatus.put(ss.getStaffId(), ss);
  }

  /**
   * Set progress of the job.
   *
   * @param p current progress of the job.
   */
  public synchronized void setprogress(long p) {
    this.progress = p;
  }

  /**
   * cleanup progress of the job.
   *
   * @return cleanupProgress.
   */
  public synchronized long cleanupProgress() {
    return cleanupProgress;
  }

  synchronized void setCleanupProgress(int p) {
    this.cleanupProgress = p;
  }

  /**
   * @return Updata progress of the job.
   */
  public synchronized long setupProgress() {
    return setupProgress;
  }

  synchronized void setSetupProgress(long p) {
    this.setupProgress = p;
  }

  public JobStatus.State getState() {
    return this.state;
  }

  public void setState(JobStatus.State state) {
    this.state = state;
  }

  public synchronized int getRunState() {
    return runState;
  }

  public synchronized void setRunState(int state) {
    this.runState = state;
  }

  public synchronized long getSuperstepCount() {
    return superstepCount;
  }

  public synchronized void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public synchronized long getStartTime() {
    return startTime;
  }

  public synchronized void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public synchronized long getFinishTime() {
    return finishTime;
  }

  /**
   * @param user
   *        The username of the job
   */
  public synchronized void setUsername(String user) {
    this.user = user;
  }

  /**
   * @return the username of the job
   */
  public synchronized String getUsername() {
    return user;
  }

  public boolean isRecovery() {
    return recovery;
  }

  public void setRecovery(boolean recovery) {
    this.recovery = recovery;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      LOG.error("[clone]", cnse);
      throw new InternalError(cnse.toString());
    }
  }

//  public synchronized String getSchedulingInfo() {
//    return schedulingInfo;
//  }
//
//  public synchronized void setSchedulingInfo(String schedulingInfo) {
//    this.schedulingInfo = schedulingInfo;
//  }

  /**
   * Whether the job has been processed.
   * Job state SUCCEEDED, FAILED and KILLED is complete.
   *
   * @return true The job has been processed.
   */
  public synchronized boolean isJobComplete() {
    return runState == JobStatus.SUCCEEDED || runState ==
        JobStatus.FAILED || runState == JobStatus.KILLED;
  }

  /** serialize
   * write this object to out.
   *
   * @param out Writes to the output stream.
   */
  public synchronized void write(DataOutput out) throws IOException {
    jobid.write(out);
    out.writeLong(setupProgress);
    out.writeLong(progress);
    out.writeLong(cleanupProgress);
    out.writeInt(runState);
    WritableUtils.writeEnum(out, this.state);
    out.writeLong(startTime);
    out.writeLong(finishTime);
    Text.writeString(out, user);
//    Text.writeString(out, schedulingInfo);
    out.writeLong(superstepCount);
    out.writeBoolean(recovery);
  }

  /**
   * deserialize
   *
   * @param in Reads some bytes from an input.
   */
  public synchronized void readFields(DataInput in) throws IOException {
    this.jobid = new BSPJobID();
    jobid.readFields(in);
    this.setupProgress = in.readLong();
    this.progress = in.readLong();
    this.cleanupProgress = in.readLong();
    this.runState = in.readInt();
    this.state = WritableUtils.readEnum(in, State.class);
    this.startTime = in.readLong();
    this.finishTime = in.readLong();
    this.user = Text.readString(in);
//    this.schedulingInfo = Text.readString(in);
    this.superstepCount = in.readLong();
    this.recovery = in.readBoolean();
  }

  public void setMapcounter(MapWritable mapcounter) {
    this.mapcounter = mapcounter;
  }

  public MapWritable getMapcounter() {
    return mapcounter;
  }

  public MapWritable getMapStaffStatus() {
    return mapStaffStatus;
  }

  public void setMapStaffStatus(MapWritable mapStaffStatus) {
    this.mapStaffStatus = mapStaffStatus;
  }
}
