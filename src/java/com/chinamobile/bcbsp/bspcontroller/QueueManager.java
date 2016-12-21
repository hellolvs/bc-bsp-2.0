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

//import java.util.List;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.Constants.BspControllerRole;

/**
 * A BSPJob Queue Manager.
 * @author
 * @version
 */
public class QueueManager {
    /**queues to store jobInprogress information*/
  private ConcurrentMap<String, Queue<JobInProgress>> queues = new
        ConcurrentHashMap<String, Queue<JobInProgress>>();
    //add by chen
    /**BSPcontroller interim state between active and standby
     * (backup controller*/
  protected BspControllerRole role = BspControllerRole.NEUTRAL;
    /**handle log information in queuemanager class*/
  private static final Log LOG = LogFactory.getLog(QueueManager.class);
    /**handle HDFS in this class*/
  private HDFSOperator queueLogOperator;
    /**system configuration*/
  private Configuration config;

  /**
   * queuemanager construct method.
   * @param conf
   *        system configuration.
   */
  public QueueManager(Configuration conf) {
  }

  /**
   * QueueManager construct method BSP system configuration and controller role.
   * @param conf
   *        bsp system configuration.
   * @param role
   *        BSPcontroller role.
   */
  public QueueManager(Configuration conf, BspControllerRole role) {
    this.config = conf;
    this.role = role;
    // if(role.equals(BspControllerRole.ACTIVE)){
    // LOG.info("in QueueManager");
    this.createLogFile();
//        }
  }

    /**
     * Initialize a job.
     * @param job
     *            required initialzied.
     */
  public void initJob(JobInProgress job) throws IOException {
    job.initStaffs();
  }

    /**
     * Add a job to the specified queue.
     * @param name
     *            of the queue.
     * @param job
     *            to be added.
     */
  public void addJob(String name, JobInProgress job) {
    Queue<JobInProgress> queue = queues.get(name);
    if (null != queue) {
      try {
                //LOG.info("in addJob and role is "+ role);
        if (role.equals(BspControllerRole.ACTIVE)) {
                   // LOG.info("before this.queueLogOperator.writeFile()");
          this.queueLogOperator.writeFile(job.getJobID().toString() + "&" +
                   name, config.get(Constants.BC_BSP_HA_LOG_DIR) +
                   Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
        }
      } catch (IOException e) {
                // TODO Auto-generated catch block
        LOG.error("Can not record " + job.getJobID().toString() +
            "&" + name + ":" + e);
        throw new RuntimeException("Can not record " +
            job.getJobID().toString() +
            "&" + name + ":", e);
      }
      queue.addJob(job);
    }
  }

    /**
     * Sort jobs by priority and then by start time.
     * @param name
     *              of the queue
     */
  public void resortWaitQueue(String name) {
    Queue<JobInProgress> queue = queues.get(name);
    if (null != queue) {
      queue.resortQueue();
    }
  }

    /**
     * Remove a job from the head of a designated queue.
     * @param name
     *            from which a job is removed.
     * @param job
     *            to be removed from the queue.
     */
  public void removeJob(String name, JobInProgress job) {
    Queue<JobInProgress> queue = queues.get(name);
    if (null != queue) {
      queue.removeJob(job);
    }
  }

    /**
     * Move a job from a queue to another.
     * @param from
     *            a queue a job is to be removed.
     * @param to
     *            a queue a job is to be added.
     * @param job
     *            job to be removed
     */
  public void moveJob(String from, String to, JobInProgress job) {
        synchronized (queues) {
      removeJob(from, job);
      addJob(to, job);
    }
  }

    /**
     * Create a FCFS queue with the name provided.
     * @param name
     *            of the queue.
     */
  public void createFCFSQueue(String name) {
    queues.putIfAbsent(name, new FCFSQueue(name));
  }

    /**
     * Find Queue according to the name specified.
     * @param name
     *            of the queue.
     * @return queue of JobInProgress
     */
  public Queue<JobInProgress> findQueue(String name) {
    return queues.get(name);
  }

  /**
   * get BSPconteroller role.
   * @return
   *        controller role.
   */
  public BspControllerRole getRole() {
    return role;
  }

  /**
   * set the BSProle
   * @param role
   *        role state to be set.
   */
  public void setRole(BspControllerRole role) {
    this.role = role;
  }

  /**
   * create queue log file.
   */
  public void createLogFile() {
       // LOG.info("in  if(role.equals(BspControllerRole.ACTIVE))");
    try {
      queueLogOperator = new HDFSOperator();
      if (!queueLogOperator.isExist(config.get(Constants.BC_BSP_HA_LOG_DIR) +
        Constants.BC_BSP_HA_QUEUE_OPERATE_LOG)) {
        queueLogOperator.createFile(config.get(Constants.BC_BSP_HA_LOG_DIR) +
          Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
      }
    } catch (IOException e) {
            // TODO Auto-generated catch block
      throw new RuntimeException("[createLogFile()]", e);
    }
        //LOG.info("after  queueLogOperator = new HDFSOperator();");
  }

  /**
   * adjust job priority in the queues.
   */
  public void adjustQueue() {
  // TODO Auto-generated method stub
    AdjustPriority adjustPriority = new AdjustPriority(this);
    adjustPriority.resortQueue();
  }

  /**
   * create the HRN waitqueue
   * @param name
   *        queuename.
   */
  public void createHRNQueue(String name) {
    queues.putIfAbsent(name, new HRNQueue(name));
  }

    /**
     * this method is used to move specific method from one
     * waitqueue to another waitqueue
     * @param to
     *        the waitQueue need to moved to
     * @param jib
     *        jibInprogress need to removed.
     */
  public void moveJobHrn(String to, JobInProgress jib) {
    LOG.info("in moveJobHrn");
    int WaitQueuesTotal = 4;
    String[] waitQueues = {"HIGHER_WAIT_QUEUE", "HIGH_WAIT_QUEUE",
      "NORMAL_WAIT_QUEUE", "LOW_WAIT_QUEUE", "LOWER_WAIT_QUEUE"};
    List<JobInProgress> list;
    for (int i = 0; i <= WaitQueuesTotal; i++) {
      list = new ArrayList<JobInProgress>(findQueue(waitQueues[i]).getJobs());
      if (list.contains(jib)) {
        removeJob(waitQueues[i], jib);
        addJob(to, jib);
      }
    }
  }

  /**
   * get the jobs in all the waitqueues.
   * @return
   *        waitQueue jobs.
   */
  public Collection<JobInProgress> getJobs() {
    int WaitQueuesTotal = 4;
    Collection<JobInProgress> jobs = new
        LinkedBlockingQueue<JobInProgress>();
    String[] waitQueues = {"HIGHER_WAIT_QUEUE", "HIGH_WAIT_QUEUE",
      "NORMAL_WAIT_QUEUE", "LOW_WAIT_QUEUE", "LOWER_WAIT_QUEUE"};
    for (int i = 0; i <= WaitQueuesTotal; i++) {
      jobs.addAll(findQueue(waitQueues[i]).getJobs());
    }
    return jobs;
  }

  /**
   * For JUnit test.
   */
  public void setQueueLogOperator(HDFSOperator queueLogOperator) {
    this.queueLogOperator = queueLogOperator;
  }

  public HDFSOperator getQueueLogOperator() {
    return queueLogOperator;
  }
}