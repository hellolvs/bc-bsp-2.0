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

import com.chinamobile.bcbsp.fault.storage.Fault;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * StaffStatus Describes the current status of a staff. This is not intended to
 * be a comprehensive piece of data.
 */
public class StaffStatus implements Writable, Cloneable {
  /** Define LOG for outputting log information */
  static final Log LOG = LogFactory.getLog(StaffStatus.class);
  /** enumeration for reporting current phase of a task. */
  public static enum Phase {
    /** current phase of a task */
    STARTING, COMPUTE, BARRIER_SYNC, CLEANUP
  }

  /** staff state */
  public static enum State {
    /** staff state value */
    RUNNING, SUCCEEDED, FAULT, FAILED, UNASSIGNED, KILLED, COMMIT_PENDING,
    /** staff state value */
    FAILED_UNCLEAN, KILLED_UNCLEAN, STAFF_RECOVERY, WORKER_RECOVERY
  }

  /** id of the job. */
  private BSPJobID jobId;
  /** id of this staff attempt */
  private StaffAttemptID staffId;
  /** The current progress of the staff */
  private int progress;
  /** The current runState of the staff */
  private volatile State runState;
  /** The current state of the staff */
  private String stateString;
  /** The current workerManager of the staff */
  private String workerManager;
  /** The current superstepCount of the staff */
  private long superstepCount;
  /** The start time of the staff */
  private long startTime;
  /** The finish time of the staff */
  private long finishTime;
  /** The current phase of the staff */
  private volatile Phase phase = Phase.STARTING;
  /** A sign of recovery */
  private boolean recovery = false;
  /** A sign of superStep. 0--before superStep,
   * 1--superSteping, 2 --after superStep.
   */
  private int stage = 1;
  /** A flag of fault. 0--no faults, 1--have a fault */
  private int faultFlag = 0;
  /** Define fault for Fault information */
  private Fault fault;
  /** The current step super running time */
  private long currentSSTime = 0;

  /**
   * Construct
   */
  public StaffStatus() {
    jobId = new BSPJobID();
    staffId = new StaffAttemptID();
    fault = new Fault();
    this.superstepCount = 0;
  }

  /**
   * Construct
   *
   * @param jobId
   *        id of the job
   * @param staffId
   *        id of this staff attempt
   * @param progress
   *        The current progress of the staff
   * @param runState
   *        The current runState of the staff
   * @param stateString
   *        The current state of the staff
   * @param workerManager
   *        The current workerManager of the staff
   * @param phase
   *        The current phase of the staff
   */
  public StaffStatus(BSPJobID jobId, StaffAttemptID staffId, int progress,
      State runState, String stateString, String workerManager, Phase phase) {
    this.jobId = jobId;
    this.staffId = staffId;
    this.progress = progress;
    this.runState = runState;
    this.stateString = stateString;
    this.workerManager = workerManager;
    this.phase = phase;
    this.superstepCount = 0;
    setStartTime(System.currentTimeMillis());
  }

  public long getRunTime() {
    return this.currentSSTime;
  }

  public void setRunTime(long time) {
    this.currentSSTime = time;
  }

  // //////////////////////////////////////////////////
  // Accessors and Modifiers
  // //////////////////////////////////////////////////
  public void setMaxSuperStep(long max) {
    this.superstepCount = max;
  }

  public int getStage() {
    return stage;
  }

  public void setStage(int stage) {
    this.stage = stage;
  }

  public boolean isRecovery() {
    return recovery;
  }

  public void setRecovery(boolean recoveried) {
    this.recovery = recoveried;
  }

  public BSPJobID getJobId() {
    return jobId;
  }

  public StaffAttemptID getStaffId() {
    return staffId;
  }

  public int getProgress() {
    return progress;
  }

  public void setProgress(int progress) {
    this.progress = progress;
  }

  public State getRunState() {
    return runState;
  }

  public void setRunState(State state) {
    this.runState = state;
  }

  public String getStateString() {
    return stateString;
  }

  public void setStateString(String stateString) {
    this.stateString = stateString;
  }

  public String getWorkerManager() {
    return workerManager;
  }

  public void setWorkerManager(String workerManager) {
    this.workerManager = workerManager;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public synchronized void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  /**
   * Get start time of the task.
   *
   * @return 0 is start time is not set, else returns start time.
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Set startTime of the task.
   *
   * @param startTime
   *        start time
   */
  void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * Get current phase of this task.
   *
   * @return current phase of this task .
   */
  public Phase getPhase() {
    return this.phase;
  }

  /**
   * Set current phase of this task.
   *
   * @param phase
   *        phase of this task
   */
  public void setPhase(Phase phase) {
    this.phase = phase;
  }

  /**
   * Update the status of the task. This update is done by ping thread before
   * sending the status.
   *
   * @param progress The current progress of the staff
   * @param state The current state of the staff
   */
  synchronized void statusUpdate(int progress, String state) {
    setProgress(progress);
    setStateString(state);
  }

  /**
   * Update the status of the task.
   *
   * @param status
   *        updated status
   */
  synchronized void statusUpdate(StaffStatus status) {
    this.progress = status.getProgress();
    this.runState = status.getRunState();
    this.stateString = status.getStateString();
    if (status.getStartTime() != 0) {
      this.startTime = status.getStartTime();
    }
    if (status.getFinishTime() != 0) {
      this.finishTime = status.getFinishTime();
    }
    this.phase = status.getPhase();
  }

  /**
   * Update specific fields of task status This update is done in BSPMaster when
   * a cleanup attempt of task reports its status. Then update only specific
   * fields, not all.
   *
   * @param runState
   *        The current runState of the staff
   * @param progress
   *        The current progress of the staff
   * @param state
   *        The current state of the staff
   * @param phase
   *        The current phase of the staff
   * @param finishTime
   *        The finish time of the staff
   */
  synchronized void statusUpdate(State runState, int progress, String state,
      Phase phase, long finishTime) {
    setRunState(runState);
    setProgress(progress);
    setStateString(state);
    setPhase(phase);
    if (finishTime != 0) {
      this.finishTime = finishTime;
    }
  }

  /**
   * @return The number of BSP super steps executed by the task.
   */
  public long getSuperstepCount() {
    return superstepCount;
  }

  public Fault getFault() {
    return fault;
  }

  /**
   * @param fault Fault information
   */
  public void setFault(Fault fault) {
    this.fault = fault;
    this.faultFlag = 1;
  }

  /**
   * Increments the number of BSP super steps executed by the task.
   */
  public void incrementSuperstepCount() {
    superstepCount += 1;
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

  /**
   * deserialize
   *
   * @param in Reads some bytes from an input.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.jobId.readFields(in);
    this.staffId.readFields(in);
    this.progress = in.readInt();
    this.runState = WritableUtils.readEnum(in, State.class);
    this.stateString = Text.readString(in);
    this.phase = WritableUtils.readEnum(in, Phase.class);
    this.startTime = in.readLong();
    this.finishTime = in.readLong();
    this.superstepCount = in.readLong();
    this.faultFlag = in.readInt();
    if (this.faultFlag == 1) {
      this.fault.readFields(in);
    }
    this.recovery = in.readBoolean();
    this.currentSSTime = in.readLong();
    this.workerManager = Text.readString(in);
  }

  /** serialize
   * write this object to out.
   *
   * @param out Writes to the output stream.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    jobId.write(out);
    staffId.write(out);
    out.writeInt(progress);
    WritableUtils.writeEnum(out, runState);
    Text.writeString(out, stateString);
    WritableUtils.writeEnum(out, phase);
    out.writeLong(startTime);
    out.writeLong(finishTime);
    out.writeLong(superstepCount);
    if (this.faultFlag == 0) {
      out.writeInt(this.faultFlag);
    } else {
      out.writeInt(this.faultFlag);
      this.fault.write(out);
    }
    out.writeBoolean(recovery);
    out.writeLong(this.currentSSTime);
    Text.writeString(out, workerManager);
  }
}
