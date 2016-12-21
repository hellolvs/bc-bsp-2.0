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

/**
 * SynchronizationServerInterface This is an interface which used for
 * Synchronization Server. It should be initialized when starting the BC-BSP
 * cluster and stopped when stopping the BC-BSP cluster.
 * @version 2.0
 */
public interface SynchronizationServerInterface {
  /**
   * Make a preparation and start the Synchronization Service.
   * @return true while started.
   */
  public boolean startServer();
  /**
   * Release relative resource and stop the Synchronization Service.
   * @return ture while stopped.
   */
  public boolean stopServer();
}
