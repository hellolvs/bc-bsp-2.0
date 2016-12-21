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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.mortbay.log.Log;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.Constants.BspCounters;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.bspcontroller.JobInProgressControlInterface;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPIds;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPCreateModeImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPZookeeperImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPkeeperStateImpl;
import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * GeneralSSController GeneralSSController for completing the general SuperStep
 * synchronization control. This class is connected to JobInProgress.
 * @author
 * @version
 */
public class GeneralSSController implements Watcher,
    GeneralSSControllerInterface {
  /**The log of the class.*/
  private static final org.apache.commons.logging.Log LOG = LogFactory
      .getLog(GeneralSSController.class);
  /**The bsp configuration.*/
  private BSPConfiguration conf;
  /**The job inprogress controller.*/
  private JobInProgressControlInterface jip;
  /**The id of the bsp job.*/
  private BSPJobID jobId;
  /**The superstep counter.*/
  private int superStepCounter = 0;
  /**The fault superstepcounter.*/
  private int faultSuperStepCounter = 0;
  /**The base checke number.*/
  private int checkNumBase;
  /**The zookeeper of the bsp.*/
  private BSPZookeeper bspzk = null;
  /**The zookeeper address.*/
  private final String zookeeperAddr;
  /**The root of the zookeeper.*/
  private final String bspZKRoot;
  /**The volatile variable of the thread.*/
  private volatile Integer mutex = 0;
  /**The stage flag.*/
  private int stageFlag = 1;
  /**The object of he zookeeper run.*/
  private ZooKeeperRun zkRun = new ZooKeeperRun();
  /**The object of the counters.*/
  private Counters counters = new Counters();
  /**The connected Latch.*/
  private CountDownLatch connectedLatch = null;
  /**
   * The operation of the zookeeper.
   * @author
   */
  public class ZooKeeperRun extends Thread {
    /**
     * @param ssc SuperStepCommand
     * @throws Exception exception.
     */
    public void startNextSuperStep(SuperStepCommand ssc) throws Exception {
      int nextSuperStep = ssc.getNextSuperStepNum();
      jip.reportLOG(jobId.toString() + " the next superstepnum is : "
          + nextSuperStep);
      if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-ss" + "/" + nextSuperStep, false)) {
        bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
            + "/" + nextSuperStep, new byte[0], BSPIds.OPEN_ACL_UNSAFE,
            new BSPCreateModeImpl().getPERSISTENT());
      } else {
        jip.reportLOG("The node hash exists" + bspZKRoot + "/"
            + jobId.toString().substring(17) + "-ss" + "/" + nextSuperStep);
        List<String> tmpList = new ArrayList<String>();
        tmpList = bspzk.getChildren(bspZKRoot + "/"
            + jobId.toString().substring(17) + "-ss" + "/" + nextSuperStep,
            false);
        for (String e : tmpList) {
          bspzk.delete(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-ss" + "/"
                  + nextSuperStep + "/" + e,
              bspzk.exists(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                      + "/" + nextSuperStep + "/" + e, false).getAversion());
        }
      }
      if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-sc" + "/" + nextSuperStep, false)) {
        bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
            + "/" + nextSuperStep, new byte[0], BSPIds.OPEN_ACL_UNSAFE,
            new BSPCreateModeImpl().getPERSISTENT());
      } else {
        List<String> tmpList = new ArrayList<String>();
        tmpList = bspzk.getChildren(bspZKRoot + "/"
            + jobId.toString().substring(17) + "-sc" + "/" + nextSuperStep,
            false);
        for (String e : tmpList) {
          bspzk.delete(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" + "/"
                  + nextSuperStep + "/" + e,
              bspzk.exists(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                      + "/" + nextSuperStep + "/" + e, false).getAversion());
        }
      }
      if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-migrate", false)) {
        bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17)
            + "-migrate", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
            new BSPCreateModeImpl().getPERSISTENT());
      } else {
        jip.reportLOG("The node hash exists" + bspZKRoot + "/"
            + jobId.toString().substring(17) + "-migrate" + "/");
        List<String> tmpList = new ArrayList<String>();
        // Stat tmpStat = null;
        tmpList = bspzk.getChildren(bspZKRoot + "/"
            + jobId.toString().substring(17) + "-migrate", false);
        for (String e : tmpList) {
          bspzk.delete(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-migrate"
                  + "/" + e,
              bspzk.exists(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-migrate"
                      + "/" + e, false).getAversion());
        }
      }
      if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-counters" + "/" + nextSuperStep, false)) {
        bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17)
            + "-counters" + "/" + nextSuperStep, new byte[0],
            BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());
      } else {
        List<String> tmpList = new ArrayList<String>();
        tmpList = bspzk.getChildren(bspZKRoot + "/"
            + jobId.toString().substring(17) + "-counters" + "/"
            + nextSuperStep, false);
        for (String e : tmpList) {
          bspzk.delete(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-counters"
                  + "/" + nextSuperStep + "/" + e,
              bspzk.exists(
                  bspZKRoot + "/" + jobId.toString().substring(17)
                      + "-counters" + "/" + nextSuperStep + "/" + e, false)
                  .getAversion());
        }
      }
      if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-sc" + "/" + superStepCounter + "/" + Constants.COMMAND_NAME,
          false)) {
        LOG.info("no such node ,now will create it");
        bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
            + "/" + superStepCounter + "/" + Constants.COMMAND_NAME, ssc
            .toString().getBytes(), BSPIds.OPEN_ACL_UNSAFE,
            new BSPCreateModeImpl().getPERSISTENT());
      }
      jip.reportLOG(jobId.toString() + " command of next is " + ssc.toString());
      jip.reportLOG(jobId.toString() + " [Write Command Path] " + bspZKRoot
          + "/" + jobId.toString().substring(17) + "-sc" + "/"
          + superStepCounter + "/" + Constants.COMMAND_NAME);
      jip.reportLOG(jobId.toString() + " leave the barrier of "
          + superStepCounter);
    }
    /**
     * @param command SuperStepCommand
     * @throws Exception exception.
     */
    public void stopNextSuperStep(String command) throws Exception {
      if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-sc" + "/" + superStepCounter + "/" + Constants.COMMAND_NAME,
          false)) {
        bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
            + "/" + superStepCounter + "/" + Constants.COMMAND_NAME,
            command.getBytes(), BSPIds.OPEN_ACL_UNSAFE,
            new BSPCreateModeImpl().getPERSISTENT());
      }
      jip.reportLOG(jobId.toString() + " command of next is " + command);
      jip.reportLOG(jobId.toString() + " prepare to quit");
    }
    /**
     * @param ableCheckPoint while can Check Point
     */
    public void cleanReadHistory(int ableCheckPoint) {
      List<String> tmpList = new ArrayList<String>();
      try {
        tmpList = bspzk.getChildren(bspZKRoot + "/"
            + jobId.toString().substring(17) + "-ss" + "/" + ableCheckPoint,
            false);
        for (String e : tmpList) {
          bspzk.delete(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-ss" + "/"
                  + ableCheckPoint + "/" + e,
              bspzk.exists(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                      + "/" + ableCheckPoint + "/" + e, false).getAversion());
          jip.reportLOG("The node hash exists" + bspZKRoot + "/"
              + jobId.toString().substring(17) + "-ss" + "/" + ableCheckPoint
              + "/" + e);
        }
      } catch (Exception exc) {
        jip.reportLOG(jobId.toString() + " [cleanReadHistory]"
            + exc.getMessage());
      }
    }
    /**
     * This is a thread and execute the logic control.
     */
    @Override
    public void run() {
      boolean jobEndFlag = true;
      try {
        if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
            + "-ss" + "/" + superStepCounter, false)) {
          bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
              + "/" + superStepCounter, new byte[0], BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
        }
        if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
            + "-sc" + "/" + superStepCounter, false)) {
          bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
              + "/" + superStepCounter, new byte[0], BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
        }
        if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
            + "-counters" + "/" + superStepCounter, false)) {
          bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17)
              + "-counters" + "/" + superStepCounter, new byte[0],
              BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());
        }
      } catch (Exception e) {
        jip.reportLOG(jobId.toString() + " [run]" + e.getMessage());
      }
      while (jobEndFlag) {
        try {
          long start = System.currentTimeMillis();
          setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
          generalSuperStepBarrier(checkNumBase * 2);
          setStageFlag(Constants.SUPERSTEP_STAGE.SECOND_STAGE);
          SuperStepCommand ssc = getSuperStepCommand(checkNumBase);
          GeneralSSController.this.counters.findCounter(
              BspCounters.TIME_IN_SYNC_MS).increment(
              System.currentTimeMillis() - start);
          updateJobCounters(checkNumBase);
          switch (ssc.getCommandType()) {
          case Constants.COMMAND_TYPE.START:
            startNextSuperStep(ssc);
            setCheckNumBase();
            superStepCounter = ssc.getNextSuperStepNum();
            jip.setSuperStepCounter(superStepCounter);
          break;
          case Constants.COMMAND_TYPE.START_AND_CHECKPOINT:
            startNextSuperStep(ssc);
            generalSuperStepBarrier(checkNumBase * 3);
            jip.setAbleCheckPoint(superStepCounter);
            LOG.info("ableCheckPoint: " + superStepCounter);
            superStepCounter = ssc.getNextSuperStepNum();
            jip.setSuperStepCounter(superStepCounter);
          break;
          case Constants.COMMAND_TYPE.START_AND_RECOVERY:
            cleanReadHistory(ssc.getAbleCheckPoint());
            startNextSuperStep(ssc);
            setCheckNumBase();
            superStepCounter = ssc.getAbleCheckPoint();
            generalSuperStepBarrier(checkNumBase * 1);
            superStepCounter = ssc.getNextSuperStepNum();
            jip.setSuperStepCounter(superStepCounter);
          break;
          case Constants.COMMAND_TYPE.STOP:
            stopNextSuperStep(ssc.toString());
            jobEndFlag = quitBarrier();
            jip.clearStaffsForJob();
          break;
          default:
            jip.reportLOG(jobId.toString() + " Unkonwn command of "
                + ssc.getCommandType());
          }
        } catch (Exception e) {
          jip.reportLOG(jobId.toString() + "error: " + e.toString());
        }
      }
    }
  }
  /**
   * Generate the GeneralSSController to control the synchronization between
   * SuperSteps。
   * @param jobId The id of the bspjob.
   */
  public GeneralSSController(BSPJobID jobId) {
    this.jobId = jobId;
    this.conf = new BSPConfiguration();
    this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
        + ":"
        + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
            Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
    setup();
  }
  
  /** For JUnit test. */
  public GeneralSSController(BSPJobID jobId,BSPConfiguration conf) {
    this.jobId = jobId;
    this.conf = conf;
    this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
            + ":"
            + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
                Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);;
    this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
    setup();
  }
  @Override
  public boolean isCommandBarrier() {
    try {
      if (bspzk.equaltostat(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-sc" + "/" + superStepCounter + "/" + Constants.COMMAND_NAME,
          false)) {
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      jip.reportLOG("[isCommandBarrier] " + e.getMessage());
      return false;
    }
  }
  @Override
  public void setJobInProgressControlInterface(JobInProgressControlInterface
      aJip) {
    this.jip = aJip;
    this.superStepCounter = aJip.getSuperStepCounter();
  }
  @Override
  public void setCheckNumBase() {
    this.checkNumBase = jip.getCheckNum();
  }
  /**
   * @return stageflag.
   */
  public int getStageFlag() {
    return stageFlag;
  }
  /**
   * @param stageFlag The stageFlag.
   */
  public void setStageFlag(int stageFlag) {
    this.stageFlag = stageFlag;
  }
  @Override
  public void process(WatchedEvent event) {
    synchronized (mutex) {
      mutex.notify();
    }
    if (event.getState() == new BSPkeeperStateImpl().getSyncConnected()) {
      this.connectedLatch.countDown();
    }
  }
  /**
   * get zookeeper.
   * @return BSPZookeeper
   */
  private BSPZookeeper getZooKeeper() {
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
   * @param bspzk BSPzookeeper.
   */
  public void zkWaitConnected(BSPZookeeper bspzk) {
    if (bspzk.equaltoState()) {
      try {
        this.connectedLatch.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }
  @Override
  public void setup() {
    try {
      this.bspzk = this.getZooKeeper();
      if (bspzk != null) {
        if (bspzk.equaltostat(this.bspZKRoot + "/"
            + this.jobId.toString().substring(17) + "-s", false)) {
          bspzk
              .create(this.bspZKRoot + "/"
                  + this.jobId.toString().substring(17) + "-s", new byte[0],
                  BSPIds.OPEN_ACL_UNSAFE,
                  new BSPCreateModeImpl().getPERSISTENT());
        }
        if (bspzk.equaltostat(this.bspZKRoot + "/"
            + this.jobId.toString().substring(17) + "-d", false)) {
          bspzk
              .create(this.bspZKRoot + "/"
                  + this.jobId.toString().substring(17) + "-d", new byte[0],
                  BSPIds.OPEN_ACL_UNSAFE,
                  new BSPCreateModeImpl().getPERSISTENT());
        }
        if (bspzk.equaltostat(this.bspZKRoot + "/"
            + this.jobId.toString().substring(17) + "-ss", false)) {
          bspzk.create(
              this.bspZKRoot + "/" + this.jobId.toString().substring(17)
                  + "-ss", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
        }
        if (bspzk.equaltostat(this.bspZKRoot + "/"
            + this.jobId.toString().substring(17) + "-sc", false)) {
          bspzk.create(
              this.bspZKRoot + "/" + this.jobId.toString().substring(17)
                  + "-sc", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
        }
        if (bspzk.equaltostat(this.bspZKRoot + "/"
            + this.jobId.toString().substring(17) + "-counters", false)) {
          bspzk.create(
              this.bspZKRoot + "/" + this.jobId.toString().substring(17)
                  + "-counters", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
        }
        if (bspzk.equaltostat(this.bspZKRoot + "/"
            + this.jobId.toString().substring(17) + "-migrate", false)) {
          bspzk.create(
              this.bspZKRoot + "/" + this.jobId.toString().substring(17)
                  + "-migrate", new byte[0], BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
        }
      }
    } catch (Exception e) {
      jip.reportLOG(jobId.toString() + " [setup]" + e.getMessage());
    }
  }
  @Override
  public void cleanup() {
    List<String> list = new ArrayList<String>();
    List<String> tmpList = new ArrayList<String>();
    try {
      try {
        list.clear();
        list = bspzk.getChildren(this.bspZKRoot + "/"
            + jobId.toString().substring(17) + "-s", false);
        for (String e : list) {
          bspzk.delete(
              this.bspZKRoot + "/" + jobId.toString().substring(17) + "-s"
                  + "/" + e,
              bspzk.exists(
                  this.bspZKRoot + "/" + jobId.toString().substring(17) + "-s"
                      + "/" + e, false).getVersion());
        }
      } catch (Exception e) {
        LOG.info("The exception is "+e.getStackTrace());
      } finally {
        bspzk.delete(
            this.bspZKRoot + "/" + jobId.toString().substring(17) + "-s",
            bspzk.exists(
                this.bspZKRoot + "/" + jobId.toString().substring(17) + "-s",
                false).getVersion());
      }
      jip.reportLOG(jobId.toString() + "delete the -s");
      try {
        list.clear();
        list = bspzk.getChildren(this.bspZKRoot + "/"
            + jobId.toString().substring(17) + "-d", false);
        for (String e : list) {
          bspzk.delete(
              this.bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
                  + "/" + e,
              bspzk.exists(
                  this.bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
                      + "/" + e, false).getVersion());
        }
      } catch (Exception e) {
        LOG.info("The exception is "+e.getStackTrace());
      } finally {
        bspzk.delete(
            this.bspZKRoot + "/" + jobId.toString().substring(17) + "-d",
            bspzk.exists(
                this.bspZKRoot + "/" + jobId.toString().substring(17) + "-d",
                false).getVersion());
      }
      jip.reportLOG(jobId.toString() + "delete the -d");
      try {
        list.clear();
        list = bspzk.getChildren(this.bspZKRoot + "/"
            + jobId.toString().substring(17) + "-migrate", false);
        for (String e : list) {
          bspzk.delete(
              this.bspZKRoot + "/" + jobId.toString().substring(17)
                  + "-migrate" + "/" + e,
              bspzk.exists(
                  this.bspZKRoot + "/" + jobId.toString().substring(17)
                      + "-migrate" + "/" + e, false).getVersion());
        }
      } catch (Exception e) {
        LOG.info("The exception is "+e.getStackTrace());
      } finally {
        bspzk.delete(
            this.bspZKRoot + "/" + jobId.toString().substring(17) + "-migrate",
            bspzk.exists(
                this.bspZKRoot + "/" + jobId.toString().substring(17)
                    + "-migrate", false).getVersion());
      }
      jip.reportLOG(jobId.toString() + "delete the -migrate");
      list.clear();
      list = bspzk.getChildren(this.bspZKRoot + "/"
          + jobId.toString().substring(17) + "-ss", false);
      for (String e : list) {
        try {
          tmpList.clear();
          tmpList = bspzk.getChildren(this.bspZKRoot + "/"
              + jobId.toString().substring(17) + "-ss" + "/" + e, false);
          for (String ee : tmpList) {
            bspzk.delete(
                this.bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                    + "/" + e + "/" + ee,
                bspzk.exists(
                    this.bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-ss" + "/" + e + "/" + ee, false).getAversion());
          }
        } catch (Exception exc) {
          exc.printStackTrace();
        } finally {
          bspzk.delete(
              this.bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                  + "/" + e,
              bspzk.exists(
                  this.bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                      + "/" + e, false).getVersion());
        }
      }
      bspzk.delete(
          this.bspZKRoot + "/" + jobId.toString().substring(17) + "-ss",
          bspzk.exists(
              this.bspZKRoot + "/" + jobId.toString().substring(17) + "-ss",
              false).getVersion());
      jip.reportLOG(jobId.toString() + "delete the -ss");
      list.clear();
      list = bspzk.getChildren(this.bspZKRoot + "/"
          + jobId.toString().substring(17) + "-sc", false);
      for (String e : list) {
        try {
          tmpList.clear();
          tmpList = bspzk.getChildren(this.bspZKRoot + "/"
              + jobId.toString().substring(17) + "-sc" + "/" + e, false);
          for (String ee : tmpList) {
            bspzk.delete(
                this.bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                    + "/" + e + "/" + ee,
                bspzk.exists(
                    this.bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-sc" + "/" + e + "/" + ee, false).getAversion());
          }
        } catch (Exception exc) {
          LOG.info("The exception is "+ exc.getStackTrace());
        } finally {
          bspzk.delete(
              this.bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                  + "/" + e,
              bspzk.exists(
                  this.bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                      + "/" + e, false).getVersion());
        }
      }
      bspzk.delete(
          this.bspZKRoot + "/" + jobId.toString().substring(17) + "-sc",
          bspzk.exists(
              this.bspZKRoot + "/" + jobId.toString().substring(17) + "-sc",
              false).getVersion());
      jip.reportLOG(jobId.toString() + "delete the -sc");
      list.clear();
      list = bspzk.getChildren(this.bspZKRoot + "/"
          + jobId.toString().substring(17) + "-counters", false);
      for (String e : list) {
        try {
          tmpList.clear();
          tmpList = bspzk.getChildren(this.bspZKRoot + "/"
              + jobId.toString().substring(17) + "-counters" + "/" + e, false);
          for (String ee : tmpList) {
            bspzk.delete(
                this.bspZKRoot + "/" + jobId.toString().substring(17)
                    + "-counters" + "/" + e + "/" + ee,
                bspzk.exists(
                    this.bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-counters" + "/" + e + "/" + ee, false)
                    .getAversion());
          }
        } catch (Exception exc) {
          LOG.info("The exception is "+ exc.getStackTrace());
        } finally {
          bspzk.delete(
              this.bspZKRoot + "/" + jobId.toString().substring(17)
                  + "-counters" + "/" + e,
              bspzk.exists(
                  this.bspZKRoot + "/" + jobId.toString().substring(17)
                      + "-counters" + "/" + e, false).getVersion());
        }
      }
      bspzk.delete(
          this.bspZKRoot + "/" + jobId.toString().substring(17) + "-counters",
          bspzk.exists(
              this.bspZKRoot + "/" + jobId.toString().substring(17)
                  + "-counters", false).getVersion());
      jip.reportLOG(jobId.toString() + "delete the -counters");
    } catch (KeeperException e) {
      jip.reportLOG(jobId.toString() + "delet error: " + e.toString());
    } catch (InterruptedException e) {
      jip.reportLOG(jobId.toString() + "delet error: " + e.toString());
    }
  }
  @Override
  public void start() {
    this.zkRun.start();
  }
  @Override
  @SuppressWarnings("deprecation")
  public void stop() {
    this.zkRun.stop();
  }
  @Override
  public boolean generalSuperStepBarrier(int checkNum) {
    List<String> list = new ArrayList<String>();
    try {
      jip.reportLOG(jobId.toString() + " enter the barrier of "
          + superStepCounter);
      while (true) {
        synchronized (mutex) {
          list.clear();
          list = bspzk.getChildren(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-ss" + "/"
                  + superStepCounter, true);
          if (list.size() < checkNum) {
            mutex.wait();
          } else {
            break;
          }
        }
      }
      return true;
    } catch (KeeperException e) {
      jip.reportLOG(jobId.toString() + "error: " + e.toString());
      return false;
    } catch (InterruptedException e) {
      jip.reportLOG(jobId.toString() + "error: " + e.toString());
      return false;
    }
  }
  @Override
  public SuperStepCommand getSuperStepCommand(int checkNum) {
    List<String> list = new ArrayList<String>();
    try {
      while (true) {
        synchronized (mutex) {
          list.clear();
          list = bspzk.getChildren(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" + "/"
                  + superStepCounter, true);
          if (list.size() < checkNum) {
            jip.reportLOG("[getSuperStepCommand]: " + list.size()
                + " instead of " + checkNum);
            mutex.wait();
          } else {
            jip.reportLOG("[getSuperStepCommand]: " + list.size());
            break;
          }
        }
      }
      SuperStepReportContainer[] ssrcs = new
          SuperStepReportContainer[checkNumBase];
      int counter = 0;
      for (String e : list) {
        LOG.info(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" + "/"
            + superStepCounter + "/" + e + " checkNumBase=" + checkNumBase
            + " list.size=" + list.size());
        if (!e.equals(Constants.COMMAND_NAME)) {
          byte[] b = bspzk.getData(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" + "/"
                  + superStepCounter + "/" + e,
              false,
              bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                  + "-sc" + "/" + superStepCounter + "/" + e, false));
          ssrcs[counter++] = new SuperStepReportContainer(new String(b));
        }
      }
      SuperStepCommand ssc = jip.generateCommand(ssrcs);
      return ssc;
    } catch (KeeperException e) {
      e.printStackTrace();
      jip.reportLOG(jobId.toString() + "error: " + e.toString());
      return null;
    } catch (InterruptedException e) {
      e.printStackTrace();
      jip.reportLOG(jobId.toString() + "error: " + e.toString());
      return null;
    }
  }
  @Override
  public boolean quitBarrier() {
    List<String> list = new ArrayList<String>();
    try {
      while (true) {
        synchronized (mutex) {
          list.clear();
          list = bspzk.getChildren(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-ss" + "/"
                  + superStepCounter, true);
          if (list.size() > 0) {
            mutex.wait();
          } else {
            break;
          }
        }
      }
    } catch (KeeperException e) {
      e.printStackTrace();
      jip.reportLOG(jobId.toString() + "error: " + e.toString());
    } catch (InterruptedException e) {
      e.printStackTrace();
      jip.reportLOG(jobId.toString() + "error: " + e.toString());
    } finally {
      jip.completedJob();
      return false;
    }
  }
  @Override
  public void recoveryBarrier(List<String> WMNames) {
    Log.info("recoveryBarrier: this.superStepCounter " + superStepCounter); 
    faultSuperStepCounter = superStepCounter;
    int base = WMNames.size();  
    switch (this.stageFlag) {
    case Constants.SUPERSTEP_STAGE.FIRST_STAGE:
      try {
        jip.reportLOG("recoveried: " + this.jobId.toString()
            + " enter the firstStageSuperStepBarrier of "
            + Integer.toString(superStepCounter));
        for (int i = 0; i < base; i++) {
          bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
              + "/" + Integer.toString(superStepCounter) + "/" + WMNames.get(i)
              + "-recovery" + 0, new byte[0], BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
          bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
              + "/" + Integer.toString(superStepCounter) + "/" + WMNames.get(i)
              + "-recovery" + 1, new byte[0], BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
          Log.info("first--recoveryBarrier: " + "recovery" + i);
        }
        jip.reportLOG("recoveried: " + this.jobId.toString()
            + " enter the secondStageSuperStepBarrier(first) of "
            + Integer.toString(superStepCounter));
        for (int i = 0; i < base; i++) {
          bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
              + "/" + Integer.toString(superStepCounter) + "/" + WMNames.get(i)
              + "-recovery" + i, "RECOVERY".getBytes(), BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
          bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17)
              + "-counters" + "/" + Integer.toString(superStepCounter) + "/"
              + WMNames.get(i), this.counters.makeEscapedCompactString()
              .getBytes(), BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl()
              .getPERSISTENT());
          Log.info("second-(first)--recoveryBarrier: " + "recovery" + i);
        }
      } catch (KeeperException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    break;
    case Constants.SUPERSTEP_STAGE.SECOND_STAGE:
      try {
        jip.reportLOG("recoveried "
            + this.jobId.toString()
            + " enter the secondStageSuperStepBarrier(second) of"
            + " superStepCounter: "
            + Integer.toString(superStepCounter));
        for (int i = 0; i < base; i++) {
          bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
              + "/" + Integer.toString(superStepCounter) + "/" + WMNames.get(i)
              + "-recovery" + i, "RECOVERY".getBytes(), BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getPERSISTENT());
          bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17)
              + "-counters" + "/" + Integer.toString(superStepCounter) + "/"
              + WMNames.get(i), this.counters.makeEscapedCompactString()
              .getBytes(), BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl()
              .getPERSISTENT());
          Log.info("second--recoveryBarrier: " + "recovery" + i);
        }
      } catch (KeeperException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    break;
    default:
      jip.reportLOG(jobId.toString() + " Unkonwn command of ");
    }
  }
  /**
   * add by chen HA recovery need to Know the running job's SuperStep.
   */
  public void setCurrentSuperStep() {
    List<String> list = new ArrayList<String>();
    try {
      list = bspzk.getChildren(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-ss", false);
      int tempStepCounter = list.size() - 1;
      for (int i = 0; i < tempStepCounter; i++) {
        superStepCounter = i;
        this.updateJobCounters(checkNumBase);
      }
      superStepCounter = list.size() - 1;
      jip.setSuperStepCounter(superStepCounter);
      LOG.info("HA the superStepCounter=" + superStepCounter);
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  /**
   * Get counters from ZooKeeper and update jip Counters.
   */
  public void updateJobCounters(int checkNum) {
    List<String> list = new ArrayList<String>();
    try {
      while (true) {
        synchronized (mutex) {
          list.clear();
          list = bspzk.getChildren(bspZKRoot + "/"
              + jobId.toString().substring(17) + "-counters" + "/"
              + superStepCounter, true);
          if (list.size() < checkNum) {
            jip.reportLOG("[getCounters]: " + list.size() + " instead of "
                + checkNum);
            mutex.wait();
          } else {
            jip.reportLOG("[getCounters]: " + list.size());
            break;
          }
        }
      }
      for (String e : list) {
        LOG.info(bspZKRoot + "/" + jobId.toString().substring(17) + "-counters"
            + "/" + superStepCounter + "/" + e);
        byte[] b = bspzk.getData(
            bspZKRoot + "/" + jobId.toString().substring(17) + "-counters"
                + "/" + superStepCounter + "/" + e,
            false,
            bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                + "-counters" + "/" + superStepCounter + "/" + e, false));
        this.counters.incrAllCounters(Counters
            .fromEscapedCompactString(new String(b)));
      }
      jip.setCounters(this.counters);
    } catch (ParseException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
      jip.reportLOG(jobId.toString() + "error: " + e.toString());
    } catch (InterruptedException e) {
      e.printStackTrace();
      jip.reportLOG(jobId.toString() + "error: " + e.toString());
    }
  }
}
