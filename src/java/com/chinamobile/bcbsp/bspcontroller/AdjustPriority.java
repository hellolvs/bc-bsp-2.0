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

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Initailize AdjustPriority to adjust job priority.
 * @author Fengshanbo
 */
public class AdjustPriority implements AdjustPriorityInterface {
  /**Record the log information.*/
  private static final Log LOG = LogFactory.getLog(AdjustPriority.class);
  /**Manage the job queue operation.*/
  private QueueManager queueManager;
  /**Response ratio value of job.*/
  private double hrnR;
  /**Get BSP configuration.*/
  private BSPConfiguration conf = new BSPConfiguration();
  /**default threshold.*/
  private int defaultAdjustThreshold = 0;
  /**First adjust threshold value between queues.*/
  private double threshold0 = conf.getInt(
      Constants.DEFAULT_BC_BSP_JOB_ADJUST_THRESHOLD0, defaultAdjustThreshold);
  /**Second adjust threshold value between queues.*/
  private double threshold1 = conf.getInt(
      Constants.DEFAULT_BC_BSP_JOB_ADJUST_THRESHOLD1, defaultAdjustThreshold);
  /**Third adjust threshold value between queues.*/
  private double threshold2 = conf.getInt(
      Constants.DEFAULT_BC_BSP_JOB_ADJUST_THRESHOLD2, defaultAdjustThreshold);
  /**Forth adjust threshold value between queues.*/
  private double threshold3 = conf.getInt(
      Constants.DEFAULT_BC_BSP_JOB_ADJUST_THRESHOLD3, defaultAdjustThreshold);
  /**Priorityis of different job queues.*/
  private final String[] priority = {Constants.PRIORITY.LOWER,
    Constants.PRIORITY.LOW, Constants.PRIORITY.NORMAL,
    Constants.PRIORITY.HIGH,
    Constants.PRIORITY.HIGHER};
  /**Queuename of different job queue.*/
  private final String[] queueName = {"HIGHER_WAIT_QUEUE", "HIGH_WAIT_QUEUE",
    "NORMAL_WAIT_QUEUE", "LOW_WAIT_QUEUE", "LOWER_WAIT_QUEUE"};
  /**list of JobInProgress.*/
  private List<JobInProgress> list;
  /**
   * Construction method of AdjustPriority.
   * @param queueManager
   *        queueManager of the job.
   */
  public AdjustPriority(QueueManager queueManager) {
    this.queueManager = queueManager;
  }
  /**
   * Adjust job priority between different queues.
   * @param jip
   *        jip that needed to be adjusted
   * @param priority
   *        target priority of jip.
   */
  public final void adjustPriority(JobInProgress jip, String priority) {
    // TODO Auto-generated method stub
    String from;
    String to;
    from = jip.getQueueNameFromPriority();
    jip.setPriority(priority);
    to = jip.getQueueNameFromPriority();
    queueManager.moveJob(from, to, jip);
    LOG.info("now " + jip.getJobID() + " is at " +
        jip.getQueueNameFromPriority());
  }
  /**
   * resort waitQueues depends on different thresholds.
   */
  public final void resortQueue() {
    // TODO Auto-generated method stub
    final double[] highResponse = {threshold0, threshold1,
      threshold2, threshold3};
    try {
      for (int i = highResponse.length; i > 0; i--) {
        list = new ArrayList<JobInProgress>(queueManager
            .findQueue(queueName[i]).getJobs());
        if (list.isEmpty()) {
          LOG.info(queueName[i] + " is empty!");
          continue;
        }
        for (JobInProgress job : list) {
          hrnR = job.getHRN();
          LOG.info("job's hrnR is " + hrnR);
          for (int j = highResponse.length - 1; j >= highResponse.length -
              i; j--) {
            if (hrnR > highResponse[j]) {
              adjustPriority(job, priority[j + 1]);
              break;
            }
          }
        }
      }
    } catch (NullPointerException e) {
      //LOG.error("NullPointer is found in adjustQueue! ", e);
      throw new RuntimeException("NullPointer is found in adjustQueue! ", e);
    }
  }
}
