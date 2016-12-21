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

package com.chinamobile.bcbsp.action;

import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Directive A generic directive from the
 * {@link com.chinamobile.bcbsp.bsp.BSPController} to the
 * {@link com.chinamobile.bcbsp.bsp.WorkerManager} to take some 'action'.
 *
 *
 *
 */

public class Directive implements Writable {
  /** Define Log variable output messages */
  public static final Log LOG = LogFactory.getLog(Directive.class);
  /** State time stamp */
  private long timestamp;
  /** State Directive.Type */
  private Directive.Type type;
  /** State worker manager name */
  private String[] workerManagersName;
  /** State action list */
  private ArrayList<WorkerManagerAction> actionList;
  /** State worker manager status */
  private WorkerManagerStatus status;
  /** State fault synchronous super step */
  private int faultSSStep;
  /** State recovery */
  private boolean recovery;
  /** State change worker state */
  private boolean changeWorkerState;
  /** State fail counter */
  private int failCounter;
  /** State migrate synchronous super step */
  private int migrateSSStep = 0;

  /**
   *
   * Type The enumeration
   *
   */
  public static enum Type {
    /** request */
    /** response */
    Request(1), Response(2);
    /** a int type of variable t */
    private int t;

    /**
     * Assignment method
     * @param t
     *        int
     */
    Type(int t) {
      this.t = t;
    }

    /**
     * get the value
     * @return
     *       type
     */
    public int value() {
      return this.t;
    }
  };

  /**
   *  constructor
   */
  public Directive() {
    this.timestamp = System.currentTimeMillis();

  }

  /**
   *  constructor
   * @param workerManagersName
   *        worker manager name
   * @param actionList
   *        worker manager action list
   */
  public Directive(String[] workerManagersName,
      ArrayList<WorkerManagerAction> actionList) {
    this();
    this.type = Directive.Type.Request;
    this.workerManagersName = workerManagersName;
    this.actionList = actionList;
  }

  /**
   * constructor
   * @param status
   *        worker manager status
   */
  public Directive(WorkerManagerStatus status) {
    this();
    this.type = Directive.Type.Response;
    this.status = status;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public Directive.Type getType() {
    return this.type;
  }

  public String[] getWorkerManagersName() {
    return this.workerManagersName;
  }

  public ArrayList<WorkerManagerAction> getActionList() {
    return this.actionList;
  }

  /**
   * add action
   * @param action
   *        worker manager action
   * @return
   *        true or false
   */
  public boolean addAction(WorkerManagerAction action) {
    if (!this.actionList.contains(action)) {
      this.actionList.add(action);
      return true;
    } else {
      return false;
    }
  }

  public WorkerManagerStatus getStatus() {
    return this.status;
  }

  public int getFaultSSStep() {
    return faultSSStep;
  }

  public void setFaultSSStep(int faultSSStep) {
    this.faultSSStep = faultSSStep;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(faultSSStep);
    out.writeLong(this.timestamp);
    out.writeInt(this.type.value());
    if (getType().value() == Directive.Type.Request.value()) {
      if (this.actionList == null) {
        WritableUtils.writeVInt(out, 0);
      } else {
        WritableUtils.writeVInt(out, actionList.size());
        for (WorkerManagerAction action : this.actionList) {
          WritableUtils.writeEnum(out, action.getActionType());
          action.write(out);
        }
      }

      WritableUtils.writeCompressedStringArray(out, this.workerManagersName);
    } else if (getType().value() == Directive.Type.Response.value()) {
      this.status.write(out);
    } else {
      throw new IllegalStateException("Wrong directive type:" + getType());
    }

    /* Zhicheng Liu added */
    out.writeInt(this.migrateSSStep);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.faultSSStep = in.readInt();
    this.timestamp = in.readLong();
    int t = in.readInt();
    if (Directive.Type.Request.value() == t) {
      this.type = Directive.Type.Request;
      int length = WritableUtils.readVInt(in);
      if (length > 0) {
        this.actionList = new ArrayList<WorkerManagerAction>();
        for (int i = 0; i < length; ++i) {
          WorkerManagerAction.ActionType actionType = WritableUtils.readEnum(
              in, WorkerManagerAction.ActionType.class);
          WorkerManagerAction action = WorkerManagerAction
              .createAction(actionType);
          action.readFields(in);
          this.actionList.add(action);
        }
      } else {
        this.actionList = null;
      }

      this.workerManagersName = WritableUtils.readCompressedStringArray(in);
    } else if (Directive.Type.Response.value() == t) {
      this.type = Directive.Type.Response;
      this.status = new WorkerManagerStatus();
      this.status.readFields(in);
    } else {
      throw new IllegalStateException("Wrong directive type:" + t);
    }

    /* Zhicheng Liu added */
    this.migrateSSStep = in.readInt();

  }

  public boolean isRecovery() {
    return recovery;
  }

  public void setRecovery(boolean recovery) {
    this.recovery = recovery;
  }

  public boolean isChangeWorkerState() {
    return changeWorkerState;
  }

  public void setChangeWorkerState(boolean changeWorkerState) {
    this.changeWorkerState = changeWorkerState;
  }

  public int getFailCounter() {
    return failCounter;
  }

  public void setFailCounter(int failCounter) {
    this.failCounter = failCounter;
  }

  /* Zhicheng Liu added */
  public int getMigrateSSStep() {
    return this.migrateSSStep;
  }

  public void setMigrateSSStep(int superstep) {
    this.migrateSSStep = superstep;
  }

  /** For JUint test. */
  public void setActionList(ArrayList<WorkerManagerAction> actionList) {
	this.actionList = actionList;
  }
}
