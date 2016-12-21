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

import com.chinamobile.bcbsp.bspcontroller.Counters;

/**
 * WorkerSSControllerInterface WorkerSSControllerInterface for local
 * synchronization and aggregation. This class is connected to
 * WorkerAgentForJob.
 * @author
 * @version
 */
public interface WorkerSSControllerInterface {
  /**
   * Make sure that all staffs have completed the local computation and
   * message-receiving.
   * @param superStepCounter The superstep counter while synchronization.
   * @param ssrc The superstep report container while synchronization.
   * @return true while completed
   */
  public boolean firstStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc);
  /**
   * Report the local information.
   * @param superStepCounter The superstep counter while synchronization.
   * @param ssrc The superstep report container while synchronization.
   * @return boolean
   */
  public boolean secondStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc);
  /**
   * Make sure that all staffs have completed the checkpoint-write operation.
   * @param superStepCounter The superstep counter while synchronization.
   * @param ssrc The superstep report container while synchronization.
   * @return true while completed
   */
  public boolean checkPointStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc);
  /**
   * Make sure that all staffs have saved the computation result and the job
   * finished successfully.
   * @param superStepCounter The superstep counter while synchronization.
   * @param ssrc The superstep report container while synchronization.
   * @return true while completed
   */
  public boolean saveResultStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc);
  /**
   * Set the Counters.
   * @param counters The superstep counter while synchronization.
   */
  public void setCounters(Counters counters);
}
