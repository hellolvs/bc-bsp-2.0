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

package com.chinamobile.bcbsp.util;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * ClusterUtil
 */
public class ClusterUtil {
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(ClusterUtil.class);

  /**
   * Data Structure to hold WorkerManager Thread and WorkerManager instance
   */
  public static class WorkerManagerThread extends Thread {
    /** Define a workerManager */
    private final WorkerManager workerManager;

    /**
     * @param r a workerManager
     * @param index Used distingushing the object returned.
     */
    public WorkerManagerThread(final WorkerManager r, final int index) {
      super(r, "WorkerManager:" + index);
      this.workerManager = r;
    }

    /** @return the groom server */
    public WorkerManager getWorkerManager() {
      return this.workerManager;
    }

    /**
     * Block until the groom server has come online, indicating it is ready to
     * be used.
     */
    public void waitForServerOnline() {
      while (!workerManager.isRunning()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.error("[waitForServerOnline]", e);
        }
      }
    }
  }

  /**
   * Creates a {@link WorkerManagerThread}. Call 'start' on the returned thread
   * to make it run.
   *
   * @param c
   *        Configuration to use.
   * @param hrsc
   *        Class to create.
   * @param index
   *        Used distingushing the object returned.
   * @throws IOException
   * @return Groom server added.
   */
  public static ClusterUtil.WorkerManagerThread createWorkerManagerThread(
      final Configuration c, final Class<? extends WorkerManager> hrsc,
      final int index) throws IOException {
    WorkerManager server;
    try {
      server = hrsc.getConstructor(Configuration.class).newInstance(c);
    } catch (Exception e) {
      LOG.error("[createWorkerManagerThread]", e);
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
    return new ClusterUtil.WorkerManagerThread(server, index);
  }

  /**
   * Start the cluster.
   *
   * @param m BSPController
   * @param conf The current job configuration
   * @param groomservers The workerManagerThread
   * @return Address to use contacting master.
   * @throws InterruptedException
   * @throws IOException
   */
  public static String startup(final BSPController m,
      final List<ClusterUtil.WorkerManagerThread> groomservers,
      Configuration conf) throws IOException, InterruptedException {
    if (m != null) {
      BSPController.startMaster((BSPConfiguration) conf);
    }
    if (groomservers != null) {
      for (ClusterUtil.WorkerManagerThread t : groomservers) {
        t.start();
      }
    }
    return m == null ? null : BSPController.getAddress(conf).getHostName();
  }

  /**
   * Shut down BC-BSP Cluster
   * @param master BSPController
   * @param groomThreads The workerManagerThread
   * @param conf The current job configuration
   */
  public static void shutdown(BSPController master,
      List<WorkerManagerThread> groomThreads, Configuration conf) {
    LOG.debug("Shutting down BC-BSP Cluster");
  }
}
