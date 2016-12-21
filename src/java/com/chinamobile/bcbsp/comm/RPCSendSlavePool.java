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
 * The RPC send thread pool, manage threads.
 */
public class RPCSendSlavePool extends ThreadGroup {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(RPCSendSlavePool.class);
  /** maximum producer number of job. */
  private int maxSlaveNum;
  /** next serial number. */
  private int nextSerialNum = 0;
  /** BSP job ID. */
  @SuppressWarnings("unused")
  private String jobID;
  /** HostName:Port--SingleSendSlave. */
  private HashMap<String, RPCSingleSendSlave> slaves = null;
  
  /**
   * Constructor of the send slave thread pool.
   * @param maxSlaveNum
   * @param jobID
   */
  public RPCSendSlavePool(int maxSlaveNum, String jobID) {
    super("RPCSendSlaves pool");
    this.maxSlaveNum = maxSlaveNum;
    this.jobID = jobID;
    this.slaves = new HashMap<String, RPCSingleSendSlave>();
  }
  
  /**
   * Create a new ProducerTool.
   * @param hostNameAndPort
   * @param superStepCount
   * @return RPCSingleSendSlave
   */
  private synchronized RPCSingleSendSlave startNewSlave(String hostNameAndPort,
      int superStepCount, RPCSender sender) {
    RPCSingleSendSlave newSendSlave = new RPCSingleSendSlave(this,
        this.nextSerialNum++, null, hostNameAndPort, "BSP", sender);
    newSendSlave.setProgress(superStepCount);
    newSendSlave.start();
    LOG.info("[RPCSendSlavePool] has started a new RPCSingleSendSlave to: "
        + hostNameAndPort);
    return newSendSlave;
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
   * Get the number of RPCSingleSendSlave. This may include killed Threads that
   * were not replaced.
   * @return This may include killed Threads that were not replaced.
   */
  public int getSlaveCount() {
    Thread[] slavesNow = getAllThread();
    int count = 0;
    for (int i = 0; i < slavesNow.length; i++) {
      if ((slavesNow[i] instanceof RPCSingleSendSlave)) {
        count++;
      }
    }
    return count;
  }
  
  /**
   * @param superStepCount
   * @return The number of RPCSingleSendSlave that are alive.
   */
  public int getActiveSlaveCount(int superStepCount) {
    Thread[] slavesNow = getAllThread();
    int count = 0;
    for (int i = 0; i < slavesNow.length; i++) {
      if ((slavesNow[i] instanceof RPCSingleSendSlave)
          && ((RPCSingleSendSlave) slavesNow[i]).getProgress() < superStepCount) {
        count++;
      }
    }
    return count;
  }
  
  /**
   * Obtain a free RPCSlave to send messages. Give preference to the
   * ProducerTool for the same destination.
   * @param hostNameAndPort
   * @param superStepCount
   * @param sender
   * @return ProducerTool
   */
  public RPCSingleSendSlave getSlave(String hostNameAndPort,
      int superStepCount, RPCSender sender) {
    RPCSingleSendSlave rpcSlave = null;
    if (this.slaves.containsKey(hostNameAndPort)) {
      rpcSlave = this.slaves.get(hostNameAndPort);
    } else {
      int count = this.getActiveSlaveCount(superStepCount - 1);
      if (count < this.maxSlaveNum) {
        rpcSlave = startNewSlave(hostNameAndPort, superStepCount, sender);
        this.slaves.put(hostNameAndPort, rpcSlave);
      } else {
        while ((rpcSlave = getAnIdleSlave()) == null) {
          try {
            LOG.info("[RPCSendSlavePool] is sleeping to wait"
                + " to get an idle RPCSingSendSlave...");
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException("[RPCSendSlavePool] caught: ", e);
            // return null;
          }
        }
        rpcSlave.setHostNameAndPort(hostNameAndPort);
      }
    }
    return rpcSlave;
  }
  
  /**
   * Get an idle slave.
   * @return RPCSingleSendSlave or null when no one is idle.
   */
  private RPCSingleSendSlave getAnIdleSlave() {
    RPCSingleSendSlave rpcSlave = null;
    Thread[] slavesNow = getAllThread();
    for (int i = 0; i < slavesNow.length; i++) {
      if (slavesNow[i] instanceof RPCSingleSendSlave) {
        RPCSingleSendSlave p = (RPCSingleSendSlave) slavesNow[i];
        if (p.isIdle()) {
          rpcSlave = p;
          return rpcSlave;
        }
      }
    }
    return rpcSlave;
  }
  
  /**
   * Set the active slave progress.
   * @param superStepCount
   */
  public void setActiveSlaveProgress(int superStepCount) {
    Thread[] slavesNow = getAllThread();
    for (int i = 0; i < slavesNow.length; i++) {
      try {
        if (slavesNow[i] instanceof RPCSingleSendSlave) {
          ((RPCSingleSendSlave) slavesNow[i]).setProgress(superStepCount);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "[RPCSendSlavePool] setActiveSlaveProgress ", e);
      }
    }
  }
  
  /**
   * Remove the failed RPCSingleSendSlave thread.
   */
  public void cleanFailedSlave() {
    ArrayList<String> failRecord = new ArrayList<String>();
    for (Entry<String, RPCSingleSendSlave> entry : this.slaves.entrySet()) {
      if (entry.getValue().isFailed()) {
        failRecord.add(entry.getKey());
      }
    }
    for (String str : failRecord) {
      this.slaves.remove(str);
    }
  }
  
  /**
   * Tell all slaves no more messages to finish them.
   */
  public void finishAll() {
    Thread[] slavesNow = getAllThread();
    for (int i = 0; i < slavesNow.length; i++) {
      if (!(slavesNow[i] instanceof RPCSingleSendSlave)) {
        continue;
      }
      RPCSingleSendSlave p = (RPCSingleSendSlave) slavesNow[i];
      p.setNoMoreMessagesFlag(true);
    }
  }
  
  /**
   * Tell all the slaves that they could return if they have been idle.
   */
  public void completeAll() {
    Thread[] slavesNow = getAllThread();
    for (int i = 0; i < slavesNow.length; i++) {
      if (!(slavesNow[i] instanceof RPCSingleSendSlave)) {
        continue;
      }
      RPCSingleSendSlave p = (RPCSingleSendSlave) slavesNow[i];
      p.complete();
    }
  }

  public int getMaxSlaveNum() {
    return maxSlaveNum;
  }

  public void setMaxSlaveNum(int maxSlaveNum) {
    this.maxSlaveNum = maxSlaveNum;
  }

  public HashMap<String, RPCSingleSendSlave> getSlaves() {
    return slaves;
  }

  public void setSlaves(HashMap<String, RPCSingleSendSlave> slaves) {
    this.slaves = slaves;
  }
}
