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

package com.chinamobile.bcbsp.sync;

import java.util.List;

import com.chinamobile.bcbsp.bspcontroller.JobInProgressControlInterface;

/**
 * GeneralSSControllerInterface This is the interface class for controlling the
 * general SuperStep synchronization.
 * @author
 * @version
 */
public interface GeneralSSControllerInterface {
  /**
   * Set the handle of JobInProgress in GeneralSSController.
   * @param jip The jobprogress Controller.
   */
  public void setJobInProgressControlInterface(
      JobInProgressControlInterface jip);
  /**
   * Set the checkNumBase.
   */
  public void setCheckNumBase();
  /**
   * Prepare to SuperStep.
   */
  public void setup();
  /**
   * Cleanup after the job is finished.
   */
  public void cleanup();
  /**
   * Start the SuperStepControl.
   */
  public void start();
  /**
   * Stop the SuperStepControl.
   */
  public void stop();
  /**
   * First stage of one SuperStep: make sure that all staffs have completed the
   * local work.
   * @param checkNum the staff num.
   * @return true while finished.
   */
  public boolean generalSuperStepBarrier(int checkNum);
  /**
   * Second stage of SuperStep: get the relative information and local
   * aggregation values and generate the SuperStepCommand.
   * @param checkNum the staff num.
   * @return SuperStepCommand.
   */
  public SuperStepCommand getSuperStepCommand(int checkNum);
  /**
   * The job has finished.
   * @return true while quit.
   */
  public boolean quitBarrier();
  /**
   * @param aWMNames The list of the worker manager names.
   */
  public void recoveryBarrier(List<String> aWMNames);
  /**
   * Only for fault-tolerance. If the command has been write on the ZooKeeper,
   * return true, else return false.
   * @return true while finished.
   */
  public boolean isCommandBarrier();
  /**add by chen HA recovery need to Know the running job's SuperStep.*/
  public void setCurrentSuperStep();
}
