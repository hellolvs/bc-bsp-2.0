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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * first come first service queue for processing,finished and failed jobs.
 * @author hadoop
 */
class FCFSQueue implements Queue<JobInProgress> {
  /** log file for FCFSQueue */
  private static final Log LOG = LogFactory.getLog(FCFSQueue.class);
  /** queue name */
  private final String name;
  /**jobinprogress queue*/
  private BlockingQueue<JobInProgress> queue = new
      LinkedBlockingQueue<JobInProgress>();
  /**tmp for queue resort*/
  private List<JobInProgress> resort_tmp = new ArrayList<JobInProgress>();
  /**jobs num counter*/
  private long counter;
  /**
   * FCFSQueue construct method
   * @param name
   *        FCFSQueue name.
   */
  public FCFSQueue(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public void addJob(JobInProgress job) {
    try {
      queue.put(job);
    } catch (InterruptedException ie) {
      //LOG.error("Fail to add a job to the " + this.name + " queue.", ie);
      throw new RuntimeException("Fail to add a job to the " +
        this.name + " queue.", ie);
    }
  }

  @Override
  public void resortQueue() {
    Comparator<JobInProgress> comp = new Comparator<JobInProgress>() {
      public int compare(JobInProgress o1, JobInProgress o2) {
        int res = o1.getPriority().compareTo(o2.getPriority());
        if (res == 0) {
          if (o1.getStartTime() < o2.getStartTime()) {
            res = -1;
          } else {
            res =( o1.getStartTime() == o2.getStartTime() ? 0 : 1);
          }
        }
        return res;
      }
    };
    synchronized (queue) {
      try {
        resort_tmp.clear();
        int wait_count = queue.size();
        int i = 0;
        for (i = 0; i < wait_count; i++) {
          resort_tmp.add(queue.take());
        }
        Collections.sort(resort_tmp, comp);
        for (i = 0; i < wait_count; i++) {
          queue.put(resort_tmp.get(i));
        }
      } catch (Exception e) {
        //LOG.error("resort error: " + e.toString());
        throw new RuntimeException("resort error: ", e);
      }
    }
  }

  @Override
  public void removeJob(JobInProgress job) {
    queue.remove(job);
  }

  @Override
  public JobInProgress removeJob() {
    try {
      return queue.take();
    } catch (InterruptedException ie) {
      //LOG.error("Fail to remove a job from the " + this.name + " queue.", ie);
      throw new RuntimeException("Fail to remove a job from the " +
        this.name + " queue.", ie);
    } catch (Exception e) {
      // TODO : Here is Exception when stop the JobProcessor Thread.
      LOG.error("Fail to remove a job from the " + this.name + " queue.", e);
    }
    return null;
  }

  @Override
  public Collection<JobInProgress> getJobs() {
    return queue;
  }

  /**
   * if the FCFSQueue contains specific job
   * @param job
   *        jobs to tell
   * @return if the queue contains job.
   */
  public boolean contains(JobInProgress job) {
    // TODO Auto-generated method stub
    return queue.contains(job);
  }

  @Override
  public long getSize() {
    // TODO Auto-generated method stub
    return this.counter;
  }

  @Override
  public boolean isEmpty() {
    // TODO Auto-generated method stub
    if (this.counter == 0L) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public JobInProgress getFront() {
    // TODO Auto-generated method stub
    return queue.peek();
  }
}
