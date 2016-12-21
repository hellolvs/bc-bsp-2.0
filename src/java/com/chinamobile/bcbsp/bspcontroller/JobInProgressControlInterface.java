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

package com.chinamobile.bcbsp.bspcontroller;

import com.chinamobile.bcbsp.sync.SuperStepCommand;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;

/**
 * JobInProgressControlInterface. It is implemented in JobInProgress
 * @author
 * @version
 */

public interface JobInProgressControlInterface {
  /**
   * Set the SuperStepCounter in JobInProgress
   * @param superStepCounter
   *        job current superstep num
   */
  public void setSuperStepCounter(int superStepCounter);
  /**
   * Get the SuperStepCounter from JobInProgress
   * @return
   *        job superstep counter.
   */
  public int getSuperStepCounter();
  /**
   * job checkpoint that can be read when fault occurs.
   * @param ableCheckPoint
   *        checkpointnum
   */
  public void setAbleCheckPoint(int ableCheckPoint);
  /**
   * Get the number of BSPStaffs in the job
   * @return
   *        job's total staffs num.
   */
  public int getNumBspStaff();
  /**
   * Get the CheckNum for SuperStep
   * @return
   *        checkNum
   */
  public int getCheckNum();
  /**
   * Get the SuperStepCommand for the next SuperStep according to the
   * SuperStepReportContainer. The SuperStepCommand include the general
   * aggregation information.
   * @param ssrcs
   *        superstepreportcontainer
   * @return
   *        superstepcommand
   */
  public SuperStepCommand generateCommand(SuperStepReportContainer[] ssrcs);
  /**
   * The job has been completed.
   */
  public void completedJob();
  /**
   * The job is failed.
   */
  public void failedJob();
  /**
   * Output the information of log in JobInProgressControlInterface.
   * @param log
   *        jobinprogress log information.
   */
  public void reportLOG(String log);
  /**
   * Set counters
   * @param counters
   *        counter the message information.
   */
  public void setCounters(Counters counters);
  /* Zhicheng Liu added */
  public void clearStaffsForJob();
}
