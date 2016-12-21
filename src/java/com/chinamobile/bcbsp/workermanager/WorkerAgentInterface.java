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

import java.io.Closeable;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.rpc.BSPRPCProtocolVersion;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * Worker Agent Interface.
 */
public interface WorkerAgentInterface extends BSPRPCProtocolVersion, Closeable,
    Constants {

  /**
   * All staffs belongs to the same job will use this to complete the local
   * synchronization and aggregation.
   * @param jobId BSPJobID
   * @param staffId StaffAttemptID
   * @param superStepCounter the counter of superStep
   * @param ssrc SuperStepReportContainer
   * @return the result of setting up localBarrier
   */
  boolean localBarrier(BSPJobID jobId, StaffAttemptID staffId,
      int superStepCounter, SuperStepReportContainer ssrc);

  /**
   * Get the number of workers which run the job.
   * @param jobId (BSPJobID
   * @param staffId StaffAttemptID
   * @return the number of workers which run the job
   */
  int getNumberWorkers(BSPJobID jobId, StaffAttemptID staffId);

  /**
   * Set the number of workers which run the job.
   * @param jobId BSPJobID
   * @param staffId StaffAttemptID
   * @param num the number of workers
   */
  void setNumberWorkers(BSPJobID jobId, StaffAttemptID staffId, int num);

  /**
   * Get the name of workerManager.
   * @param jobId BSPJobID
   * @param staffId StaffAttemptID
   * @return The workerManagerName.
   */
  String getWorkerManagerName(BSPJobID jobId, StaffAttemptID staffId);

  /**
   * Get WorkerAgent BSPJobID
   * @return BSPJobID
   */
  BSPJobID getBSPJobID();

  /**
   * This method is used to set mapping table that shows the partition to the
   * worker.
   * @param jobId BSPJobID
   * @param partitionId id of specific partition
   * @param hostName the name of host
   */
  void setWorkerNametoPartitions(BSPJobID jobId, int partitionId,
      String hostName);

  /**
   * Get a free port.
   * @return port
   */
  int getFreePort();

  /**
   * Set the WorkerAgentStaffInterface's address for the staff with staffID.
   * @param staffID StaffAttemptID
   * @param addr IP address
   */
  void setStaffAgentAddress(StaffAttemptID staffID, String addr);

  /**
   * Clear the counter of staff report
   * @param jobId BSPJobID
   */
  void clearStaffRC(BSPJobID jobId);
}
