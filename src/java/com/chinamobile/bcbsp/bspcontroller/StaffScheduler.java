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
package com.chinamobile.bcbsp.bspcontroller;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.chinamobile.bcbsp.workermanager.WorkerManagerControlInterface;
import com.chinamobile.bcbsp.Constants.BspControllerRole;

/**
 * Used by a {@link BSPController} to schedule {@link Staff}s on
 * {@link WorkerManager} s.
 * @author
 * @version
 */
abstract class StaffScheduler implements Configurable {
  /**BSP system configuration*/
  protected Configuration conf;
  /**workermanager controller*/
  protected WorkerManagerControlInterface controller;
  /**queueManager*/
  protected QueueManager queueManager;
  /**BSP controller role*/
  protected BspControllerRole role = BspControllerRole.NEUTRAL;
  /**
   * get the configuration
   * @return conf
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * set BSP configuration.
   * @param conf to be set.
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * set workermanager controller
   * @param controller
   *        workermanager controller.
   */
  public synchronized void setWorkerManagerControlInterface(
      WorkerManagerControlInterface controller) {
    this.controller = controller;
  }

  /**
   * Lifecycle method to allow the scheduler to start any work in separate
   * threads.
   * @throws IOException
   */
  public void start() throws IOException {
    // do nothing
  }

  /**
   * Lifecycle method to allow the scheduler to stop any work it is doing.
   * @throws IOException
   */
  public void stop() throws IOException {
    // do nothing
  }

  /**
   * Returns a collection of jobs in an order which is specific to the
   * particular scheduler.
   * @param queue Queue name.
   * @return JobInProgress corresponded to the specified queue.
   */
  public abstract Collection<JobInProgress> getJobs(String queue);
  public QueueManager getQueueManager() {
    return queueManager;
  }

  /**
   * set queuemanager of staff schedule.
   * @param queueManager
   *        queuemanager to be set.
   */
  public void setQueueManager(QueueManager queueManager) {
    this.queueManager = queueManager;
  }

  /**
   * start the jobprocessor
   */
  public void jobProcessorStart() {
    // do nothing
  }

  /**
   * get the BSPcontroller role.
   * @return
   *        BSPcontroller role.
   */
  public BspControllerRole getRole() {
    return role;
  }

  /**
   * set the BSPcontroller role.
   * @param role
   *        controller to be set.
   */
  public void setRole(BspControllerRole role) {
    this.role = role;
  }
}
