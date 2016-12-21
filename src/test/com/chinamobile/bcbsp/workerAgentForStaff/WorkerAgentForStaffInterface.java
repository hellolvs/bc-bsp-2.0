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

package com.chinamobile.bcbsp.workerAgentForStaff;

import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.VersionedProtocol;
/**
 * WorkerAgentForStaffInterface.java.
 */
public interface WorkerAgentForStaffInterface extends VersionedProtocol {
  public static final long versionID = 0L;

  /**
   * This method is used to worker which this worker's partition id equals
   * belongPartition.
   * @param jobId the current BSP job id
   * @param staffId the current Staff id
   * @param belongPartition the current partition
   * @return worker manager
   */
  WorkerAgentForStaffInterface getWorker(BSPJobID jobId,
      StaffAttemptID staffId, int belongPartition);

  /**
   * This method is used to put the HeadNode to WorkerAgentForJob's map.
   * @param jobId the current BSP job id
   * @param staffId the current Staff id
   * @param belongPartition
   *        the partitionID which the HeadNode belongs to
   */
  void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
      int belongPartition, BytesWritable data, String type);

  /**
   * This method is used to put the HeadNode to WorkerAgentForJob's map.
   * @param jobId the current BSP job id
   * @param staffId the current Staff id
   * @param belongPartition
   *        the partitionID which the HeadNode belongs to
   */
  void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
      int belongPartition, BytesWritable data);

  /**
   * Get the address of this WorkerAgentForStaff.
   * @return address
   */
  String address();

  /**
   * This method will be invoked before the staff be killed, to notice the
   * staff to do some cleaning operations.
   */
  void onKillStaff();

}
