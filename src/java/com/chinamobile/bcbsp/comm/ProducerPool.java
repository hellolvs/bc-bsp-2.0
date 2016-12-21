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

package com.chinamobile.bcbsp.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ProducerTools Pool.
 * @author Bai Qiushi
 * @version 1.0 2012-3-29
 */
public class ProducerPool extends ThreadGroup {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(ProducerPool.class);
  /** next Serial Number. */
  private int nextSerialNumber = 0;
  /** maxim producer number. */
  private int maxProducerNum;
  /** job ID. */
  @SuppressWarnings("unused")
  private String jobID;
  /** HostName:Port--ProducerTool. */
  private HashMap<String, ProducerTool> producers = null;
  
  /**
   * Constructor of ProducerPool.
   * @param maxNum
   *        int
   * @param ajobID
   *        String
   */
  public ProducerPool(final int maxNum, final String ajobID) {
    super("ProducerTools Pool");
    this.maxProducerNum = maxNum;
    this.jobID = ajobID;
    this.producers = new HashMap<String, ProducerTool>();
  }
  
  /**
   * Tell all producers no more messages to finish them.
   */
  public final void finishAll() {
    Thread[] producersNow = getAllThread();
    for (int i = 0; i < producersNow.length; i++) {
      if (!(producersNow[i] instanceof ProducerTool)) {
        continue;
      }
      ProducerTool p = (ProducerTool) producersNow[i];
      p.setNoMoreMessagesFlag(true);
    }
  }
  
  /** Complete all producer. */
  public final void completeAll() {
    Thread[] producersNow = getAllThread();
    for (int i = 0; i < producersNow.length; i++) {
      if (!(producersNow[i] instanceof ProducerTool)) {
        continue;
      }
      ProducerTool p = (ProducerTool) producersNow[i];
      p.complete();
    }
  }
  
  /**
   * Get the number of ProducerTool that are alive.
   * @param superStepCount
   *        int
   * @return The number of ProducerTool that are alive.
   */
  public final int getActiveProducerCount(final int superStepCount) {
    Thread[] producersNow = getAllThread();
    int count = 0;
    for (int i = 0; i < producersNow.length; i++) {
      if ((producersNow[i] instanceof ProducerTool)
          && ((ProducerTool) producersNow[i]).getProgress() < superStepCount) {
        count++;
      }
    }
    return count;
  }
  
  /**
   * Set set active producer progress.
   * @param superStepCount
   *        int
   */
  public final void setActiveProducerProgress(final int superStepCount) {
    Thread[] producersNow = getAllThread();
    for (int i = 0; i < producersNow.length; i++) {
      try {
        if (producersNow[i] instanceof ProducerTool) {
          ((ProducerTool) producersNow[i]).setProgress(superStepCount);
        }
      } catch (Exception e) {
        LOG.warn("ProducerTool set active producer progress ", e);
      }
    }
  }
  
  /** Clear all failed producer. */
  public final void cleanFailedProducer() {
    ArrayList<String> failRecord = new ArrayList<String>();
    for (Entry<String, ProducerTool> entry : this.producers.entrySet()) {
      if (entry.getValue().isFailed()) {
        failRecord.add(entry.getKey());
      }
    }
    for (String str : failRecord) {
      this.producers.remove(str);
    }
  }
  
  /**
   * Get the number of ProducerTool. This may include killed Threads that were
   * not replaced.
   * @return The number of ProducerTool.
   */
  public final int getProducerCount() {
    Thread[] producersNow = getAllThread();
    int count = 0;
    for (int i = 0; i < producersNow.length; i++) {
      if ((producersNow[i] instanceof ProducerTool)) {
        count++;
      }
    }
    return count;
  }
  
  /**
   * Obtain a free ProducerTool to send messages. Give preference to the
   * ProducerTool for the same destination.
   * @param hostNameAndPort
   *        String
   * @param superStepCount
   *        int
   * @param sender
   *        Sender
   * @return ProducerTool
   */
  public final ProducerTool getProducer(final String hostNameAndPort,
      final int superStepCount, final Sender sender) {
    ProducerTool pt = null;
    if (producers.containsKey(hostNameAndPort)) {
      pt = producers.get(hostNameAndPort);
    } else {
      int count = this.getActiveProducerCount(superStepCount - 1);
      if (count < this.maxProducerNum) {
        pt = startNewProducer(hostNameAndPort, superStepCount, sender);
        this.producers.put(hostNameAndPort, pt);
      } else {
        while ((pt = getAnIdleProducer()) == null) {
          try {
            LOG.info("ProducerPool is sleeping to wait for an idle producer..");
            Thread.sleep(100);
          } catch (InterruptedException e) {
            LOG.error("[ProducerPool] caught: ", e);
            return null;
          }
        }
        pt.setHostNameAndPort(hostNameAndPort);
      }
    }
    return pt;
  }
  
  /**
   * Get all threads in pool.
   * @return Thread[]
   */
  private Thread[] getAllThread() {
    Thread[] threads = new Thread[activeCount()];
    this.enumerate(threads);
    return threads;
  }
  
  /**
   * Get an idle producer.
   * @return ProduerTool or null when no one is idle.
   */
  private ProducerTool getAnIdleProducer() {
    ProducerTool pt = null;
    Thread[] producersNow = getAllThread();
    for (int i = 0; i < producersNow.length; i++) {
      if (producersNow[i] instanceof ProducerTool) {
        ProducerTool p = (ProducerTool) producersNow[i];
        if (p.isIdle()) {
          pt = p;
          return pt;
        }
      }
    }
    return pt;
  }
  
  /**
   * Create a new ProducerTool.
   * @param hostNameAndPort
   *        String
   * @param superStepCount
   *        int
   * @param sender
   *        Sender
   * @return ProducerTool
   */
  private synchronized ProducerTool startNewProducer(
      final String hostNameAndPort, final int superStepCount,
      final Sender sender) {
    ProducerTool newProducer = new ProducerTool(this, this.nextSerialNumber++,
        null, hostNameAndPort, "BSP", sender);
    newProducer.setProgress(superStepCount);
    newProducer.start();
    LOG.info("[ProducerPool] has started a new ProducerTool to: "
        + hostNameAndPort);
    return newProducer;
  }

  public int getMaxProducerNum() {
    return maxProducerNum;
  }

  public void setMaxProducerNum(int maxProducerNum) {
    this.maxProducerNum = maxProducerNum;
  }

  public HashMap<String, ProducerTool> getProducers() {
    return producers;
  }
}
