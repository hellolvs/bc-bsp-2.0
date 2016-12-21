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

import com.chinamobile.bcbsp.bspcontroller.ClusterStatus;
import com.chinamobile.bcbsp.bspcontroller.JobInProgressListener;
import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import java.util.Collection;

/**
 * Manages information about the {@link WorkerManager}s in the cluster.
 * environment. This interface is not intended to be implemented by users.
 */
public interface WorkerManagerControlInterface {

  /**
   * Get the current status of the cluster.
   * @param detailed
   *        if true then report workerManager names as well
   * @return summary of the state of the cluster
   */
  ClusterStatus getClusterStatus(boolean detailed);

  /**
   * Find WorkerManagerProtocol with corresponded workerManager server status.
   * @param status
   *        WorkerManagerStatus
   * @return WorkerManagerProtocol
   */
  WorkerManagerProtocol findWorkerManager(WorkerManagerStatus status);

  /**
   * Find the collection of workerManager servers.
   * @return Collection of workerManager servers list.
   */
  Collection<WorkerManagerProtocol> findWorkerManagers();

  /**
   * Collection of WorkerManagerStatus as the key set.
   * @return Collection of WorkerManagerStatus.
   */
  Collection<WorkerManagerStatus> workerServerStatusKeySet();

  /**
   * Registers a JobInProgressListener to WorkerManagerControlInterface.
   * Therefore, adding a JobInProgress will trigger the jobAdded function.
   * @param listener
   *        JobInProgressListener listener to be added.
   */
  void addJobInProgressListener(JobInProgressListener listener);

  /**
   * Unregisters a JobInProgressListener to WorkerManagerControlInterface.
   * Therefore, the remove of a JobInProgress will trigger the jobRemoved
   * action.
   * @param listener
   *        JobInProgressListener to be removed.
   */
  void removeJobInProgressListener(JobInProgressListener listener);

  /**
   * Update the WorkerManagerStatus Cache(now it is used in SimpleStaffScheduler
   * and BSPController).
   * @param old
   *        the original WorkerManagerStatus, it will be replaced by the new
   *        WorkerManagerStatus.
   * @param newKey the new WorkerManagerStatus.
   */
  void updateWhiteWorkerManagersKey(WorkerManagerStatus old,
      WorkerManagerStatus newKey);

  /**
   * Current WorkerManager.
   * @return return WorkerManagersName.
   */
  String[] getActiveWorkerManagersName();

  /**
   * Remove worker from white.
   * @param wms WorkerManagerStatus
   * @return return WorkerManagerProtocol.
   */
  WorkerManagerProtocol removeWorkerFromWhite(WorkerManagerStatus wms);

  /**
   * Add worker to gray.
   * @param wms WorkerManagerStatus
   * @param wmp WorkerManagerProtocol
   */
  void addWorkerToGray(WorkerManagerStatus wms, WorkerManagerProtocol wmp);

  /**
   * Get the count of failed job on worker.
   * @return the count of failed job on worker
   */
  int getMaxFailedJobOnWorker();
}
