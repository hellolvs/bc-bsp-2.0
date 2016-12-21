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

import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPIds;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPCreateModeImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPZookeeperImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPkeeperStateImpl;
import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * WorkerSSController This is an implementation for ZooKeeper.
 * WorkerSSController for local synchronization and aggregation. This class is
 * connected to WorkerAgentForJob.
 * @author
 * @version
 */
public class WorkerSSController implements WorkerSSControllerInterface, Watcher {
  /**The bsp configuration.*/
  private BSPConfiguration conf;
  /**The zookeeper address.*/
  private final String zookeeperAddr;
  /**The zookeeper root address.*/
  private final String bspZKRoot;
  /**The object of the zookeeper for synchronization.*/
  private BSPZookeeper bspzk = null;
  /**The id of the bsp job.*/
  private BSPJobID jobId;
  /**The name of the worker.*/
  private String workerName;
  /**The log of the class.*/
  public static final Log LOG = LogFactory.getLog(StaffSSController.class);
  /**The connected Latch.*/
  private CountDownLatch connectedLatch = null;
  /**The superstep counter while synchronization.*/
  private Counters counters;
  /**
   * Generate the WorkerSSController to control the local synchronization and
   * aggregation.
   * @param aJobId The id of the bspjob
   * @param aWorkerName the name of the worker
   */
  public WorkerSSController(BSPJobID aJobId, String aWorkerName) {
    this.conf = new BSPConfiguration();
    this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
        + ":"
        + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
            Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
    this.jobId = aJobId;
    this.workerName = aWorkerName;
  }
  
  public WorkerSSController(){
    this.conf = new BSPConfiguration();
    this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
        + ":"
        + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
            Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
  }
  
  /** For JUnit test. */
  public WorkerSSController(BSPJobID aJobId, String aWorkerName,BSPConfiguration bspconf) {
    this.conf = bspconf;
    this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
            + ":"
            + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
                Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
    this.jobId = aJobId;
    this.workerName = aWorkerName;
  }
  @Override
  public boolean firstStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc) {
    LOG.info(this.jobId.toString() + " on " + this.workerName
        + " enter the firstStageSuperStepBarrier of "
        + Integer.toString(superStepCounter));
    try {
      this.bspzk = getZooKeeper();
      LOG.info(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss" + "/"
          + Integer.toString(superStepCounter) + "/" + this.workerName
          + (ssrc.getDirFlag())[0]);
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
          + "/" + Integer.toString(superStepCounter) + "/" + this.workerName
          + (ssrc.getDirFlag())[0], new byte[0], BSPIds.OPEN_ACL_UNSAFE,
          new BSPCreateModeImpl().getPERSISTENT());
      closeZooKeeper();
      return true;
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[firstStageSuperStepBarrier]", e);
      return false;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[firstStageSuperStepBarrier]", e);
      return false;
    }
  }
  @Override
  public boolean secondStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc) {
    LOG.info(this.jobId.toString() + " on " + this.workerName
        + " enter the secondStageSuperStepBarrier of "
        + Integer.toString(superStepCounter));
    try {
      this.bspzk = getZooKeeper();
      LOG.info(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" + "/"
          + Integer.toString(superStepCounter) + "/" + this.workerName);
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
          + "/" + Integer.toString(superStepCounter) + "/" + this.workerName,
          ssrc.toStringForMigrate().getBytes(), BSPIds.OPEN_ACL_UNSAFE,
          new BSPCreateModeImpl().getPERSISTENT());
      LOG.info(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" + "/"
          + Integer.toString(superStepCounter) + "/" + this.workerName + ":"
          + ssrc.toStringForMigrate());
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-counters" + "/" + Integer.toString(superStepCounter) + "/"
          + this.workerName, this.counters.makeEscapedCompactString()
          .getBytes(), BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl()
          .getPERSISTENT());
      this.counters.clearCounters();
      closeZooKeeper();
      return true;
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[secondStageSuperStepBarrier]", e);
      return false;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[secondStageSuperStepBarrier]", e);
      return false;
    }
  }
  @Override
  public boolean checkPointStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc) {
    LOG.info(this.jobId.toString() + " on " + this.workerName
        + " enter the checkPointStageBarrier of "
        + Integer.toString(superStepCounter));
    try {
      this.bspzk = getZooKeeper();
      LOG.info("[checkPointStageSuperStepBarrier]" + this.bspZKRoot + "/"
          + jobId.toString().substring(17) + "-ss" + "/"
          + Integer.toString(superStepCounter) + "/" + this.workerName
          + (ssrc.getDirFlag())[0]);
      String send = "no information";
      if (ssrc.getStageFlag() ==
          Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE) {
        send = ssrc.getActiveMQWorkerNameAndPorts();
        LOG.info("Test send:" + send);
        LOG.info("Test path:" + bspZKRoot + "/"
            + jobId.toString().substring(17) + "-ss" + "/"
            + Integer.toString(superStepCounter) + "/" + this.workerName
            + (ssrc.getDirFlag())[0]);
      }
      if (bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
          + "/" + Integer.toString(superStepCounter) + "/" + this.workerName
          + (ssrc.getDirFlag())[0], false) == null) {
        bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
            + "/" + Integer.toString(superStepCounter) + "/" + this.workerName
            + (ssrc.getDirFlag())[0], send.getBytes(), BSPIds.OPEN_ACL_UNSAFE,
            new BSPCreateModeImpl().getPERSISTENT());
      }
      closeZooKeeper();
      return true;
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[checkPointStageSuperStepBarrier]", e);
      return false;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[checkPointStageSuperStepBarrier]", e);
      return false;
    }
  }
  @Override
  public boolean saveResultStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc) {
    try {
      this.bspzk = getZooKeeper();
      String[] dir = ssrc.getDirFlag();
      for (String e : dir) {
        if (bspzk.equaltoStat(bspZKRoot + "/" + jobId.toString().substring(17)
            + "-ss" + "/" + Integer.toString(superStepCounter) + "/"
            + this.workerName + e, false)) {
          bspzk.delete(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-ss" + "/"
                  + Integer.toString(superStepCounter) + "/" + this.workerName
                  + e,
              bspzk.exists(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                      + "/" + Integer.toString(superStepCounter) + "/"
                      + this.workerName + e, false).getAversion());
        }
      }
      closeZooKeeper();
      return true;
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[saveResultStageSuperStepBarrier]", e);
      return false;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[saveResultStageSuperStepBarrier]", e);
      return false;
    }
  }
  /**
   * @param event The WatchedEvent event.
   */
  public void process(WatchedEvent event) {
    if (event.getState() == new BSPkeeperStateImpl().getSyncConnected()) {
      this.connectedLatch.countDown();
    }
  }
  /**
   * @return BSPZookeeper
   */
  public BSPZookeeper getZooKeeper() {
    try {
      if (this.bspzk == null) {
        this.connectedLatch = new CountDownLatch(1);
        this.bspzk = new BSPZookeeperImpl(this.zookeeperAddr,
            Constants.SESSION_TIME_OUT, this);
        this.zkWaitConnected(bspzk);
        return bspzk;
      } else {
        return this.bspzk;
      }
    } catch (Exception e) {
      LOG.error("[getZooKeeper]", e);
      return null;
    }
  }
  /**
   * @param zk the object of the zookeeper.
   */
  public void zkWaitConnected(BSPZookeeper zk) {
    if (bspzk.equaltoState()) {
      try {
        this.connectedLatch.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }
  /**
   * To close the connected of the Zookeeper.
   */
  private void closeZooKeeper() {
    if (this.bspzk != null) {
      try {
        this.bspzk.close();
        this.bspzk = null;
      } catch (InterruptedException e) {
        LOG.error("[closeZooKeeper]", e);
      }
    }
  }
  /**
   * @return counters The superstep counter while synchronization.
   */
  public Counters getCounters() {
    return counters;
  }
  @Override
  public void setCounters(Counters pCounters) {
    if (this.counters == null) {
      this.counters = new Counters();
    }
    this.counters.incrAllCounters(pCounters);
  }
  
  /** For JUnit test. */
  public void setcounters(Counters counter){
    this.counters=counter;
}
  
}
