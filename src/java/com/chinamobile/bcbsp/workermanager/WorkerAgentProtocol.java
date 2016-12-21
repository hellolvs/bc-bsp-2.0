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

import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import java.io.IOException;

/**
 * Protocol that staff child process uses to contact its parent process.
 */
public interface WorkerAgentProtocol extends WorkerAgentInterface {

  /** Called when a child staff process starts, to get its staff.
   * @param staffId the id of staff
   * @return staffId
   */
  Staff getStaff(StaffAttemptID staffId) throws IOException;

  /**
   * Periodically called by child to check if parent is still alive.
   * @param staffId the id of staff
   * @return True if the staff is known
   */
  boolean ping(StaffAttemptID staffId) throws IOException;

  /**
   * Report that the staff is successfully completed. Failure is assumed if the
   * staff process exits without calling this.
   * @param staffid
   *        staff's id
   * @param shouldBePromoted
   *        whether to promote the staff's output or not
   */
  void done(StaffAttemptID staffid, boolean shouldBePromoted)
      throws IOException;

  /**
   * Report that the staff encounted a local fileSystem error.
   * @param staffId the id of staff
   * @param message error messages
   */
  void fsError(StaffAttemptID staffId, String message) throws IOException;

  /**
   * The function is used for detect fault.
   * @param staffId the id of staff
   * @param staffStatus the status of staff
   * @param fault Fault
   * @param stage the current phase of staff
   */
  void setStaffStatus(StaffAttemptID staffId, int staffStatus,
      Fault fault, int stage);

  /**
   * Judge the recovery state of staff by staffId.
   * @param staffId the id of staff
   * @return the result of staffRecovery(true or false)
   */
  boolean getStaffRecoveryState(StaffAttemptID staffId);

  /**
   * Judge the state of staffChangeWorker.
   * @param staffId the id of staff
   * @return the result of StaffChangeWorkerState(true or false)
   */
  boolean getStaffChangeWorkerState(StaffAttemptID staffId);

  /**
   * Judge the state of staffChangeWorker.
   * @param staffId the id of staff
   * @return the counter of failed staff
   */
  int getFailCounter(StaffAttemptID staffId);

  /**
   * Add staff report counter.
   * @param jobId BSPJobID
   */
  void addStaffReportCounter(BSPJobID jobId);

  /**
   * Add counters.
   * @param jobId BSPJobID
   * @param pCounters Counters
   */
  void addCounters(BSPJobID jobId, Counters pCounters);

  /**
   * Get current migrate superStep.
   * @param staffId the id of staff
   * @return current migrate superStep
   */
  int getMigrateSuperStep(StaffAttemptID staffId);

  /**
   * Update staff's information.
   * @param staffId the id of staff
   * @return the result of updateWorkerJobState(true or false)
   */
  boolean updateWorkerJobState(StaffAttemptID staffId);

}
