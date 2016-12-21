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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPIds;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPCreateModeImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPZookeeperImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPkeeperStateImpl;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;

/**
 * StaffSSController StaffSSController for completing the staff SuperStep
 * synchronization control. This class is connected to BSPStaff.
 * @author
 * @version
 */
public class StaffSSController implements StaffSSControllerInterface, Watcher {
  /**The bsp configuration.*/
  private BSPConfiguration conf;
  /**The zookeeper address.*/
  private final String zookeeperAddr;
  /**The root of the bspzkeeper.*/
  private final String bspZKRoot;
  /**The bspzookeeper.*/
  private BSPZookeeper bspzk = null;
  /**The volatile variable of the thread.*/
  private volatile Integer mutex = 0;
  /**The id of the bsp job.*/
  private BSPJobID jobId;
  /**The id of the staff job.*/
  private StaffAttemptID staffId;
  /**The worker agent.*/
  private WorkerAgentProtocol workerAgent;
  /**The log of the class.*/
  public static final Log LOG = LogFactory.getLog(StaffSSController.class);
  /**The rebuildtime of the sync.*/
  public static long rebuildTime = 0;
  /**The object of the counters.*/
  private Counters counters = null;
  /**The connected Latch.*/
  private CountDownLatch connectedLatch = null;
  /**
   * @param jobId the id of the job.
   * @param staffId the id of the staff.
   * @param workerAgent the worker agent.
   */
  public StaffSSController(BSPJobID jobId, StaffAttemptID staffId,
      WorkerAgentProtocol workerAgent) {
    this.conf = new BSPConfiguration();
    this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
        + ":"
        + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
            Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
    this.jobId = jobId;
    this.staffId = staffId;
    this.workerAgent = workerAgent;
  }
  
  /** For JUnit test. */
  public StaffSSController(BSPJobID jobId, StaffAttemptID staffId,
      WorkerAgentProtocol workerAgent,BSPConfiguration bspconf){
  this.zookeeperAddr = bspconf.get(Constants.ZOOKEEPER_QUORUM)
            + ":"
            + bspconf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
                Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
    this.jobId = jobId;
    this.staffId = staffId;
    this.workerAgent = workerAgent;
  
}
  @Override
  public HashMap<Integer, String> scheduleBarrier(SuperStepReportContainer ssrc) {
    LOG.info(staffId + " enter the scheduleBarrier");
    String send = ssrc.getPartitionId() + Constants.SPLIT_FLAG
        + this.workerAgent.getWorkerManagerName(jobId, staffId)
        + Constants.SPLIT_FLAG + ssrc.getPort1() + "-" + ssrc.getPort2();
    try {
      this.bspzk = getZooKeeper();
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-s"
          + "/" + staffId.toString(), send.getBytes(), BSPIds.OPEN_ACL_UNSAFE,
          new BSPCreateModeImpl().getPERSISTENT());
      List<String> list = new ArrayList<String>();
      List<String> workerManagerNames = new ArrayList<String>();
      HashMap<Integer, String> partitionToWorkerManagerName
      = new HashMap<Integer, String>();
      int checkNum = ssrc.getCheckNum();
      while (true) {
        synchronized (mutex) {
          list.clear();
          list = bspzk.getChildren(bspZKRoot + "/"
              + jobId.toString().substring(17) + "-s", true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " ,waiting......");
            mutex.wait();
          } else {
            for (String e : list) {
              byte[] b = bspzk.getData(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-s" + "/"
                      + e,
                  false,
                  bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                      + "-s" + "/" + e, false));
              String[] receive = new String(b).split(Constants.SPLIT_FLAG);
              String hostWithPort = receive[1] + ":" + receive[2];
              LOG.info(hostWithPort);
              partitionToWorkerManagerName.put(Integer.valueOf(receive[0]),
                  hostWithPort);
              workerAgent.setWorkerNametoPartitions(jobId,
                  Integer.valueOf(receive[0]), receive[1]);
              if (!workerManagerNames.contains(receive[1])) {
                workerManagerNames.add(receive[1]);
              }
            }
            this.workerAgent.setNumberWorkers(jobId, staffId,
                workerManagerNames.size());
            list.clear();
            workerManagerNames.clear();
            break;
          }
        }
      }
      closeZooKeeper();
      LOG.info(staffId + " leave the scheduleBarrier");
      return partitionToWorkerManagerName;
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[schedulerBarrier]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[schedulerBarrier]", e);
      return null;
    }
  }
  @Override
  public HashMap<Integer, List<Integer>> loadDataBarrier(
      SuperStepReportContainer ssrc) {
    LOG.info(staffId + " enter the loadDataBarrier");
    String send = ssrc.getPartitionId() + Constants.SPLIT_FLAG
        + ssrc.getMinRange() + Constants.SPLIT_FLAG + ssrc.getMaxRange();
    int checkNum = ssrc.getCheckNum();
    try {
      this.bspzk = getZooKeeper();
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
          + "/" + staffId.toString(), send.getBytes(), BSPIds.OPEN_ACL_UNSAFE,
          new BSPCreateModeImpl().getPERSISTENT());
      List<String> list = new ArrayList<String>();
      HashMap<Integer, List<Integer>> partitionToRange
      = new HashMap<Integer, List<Integer>>();
      while (true) {
        synchronized (mutex) {
          list = bspzk.getChildren(bspZKRoot + "/"
              + jobId.toString().substring(17) + "-d", true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " ,waiting......");
            mutex.wait();
          } else {
            for (String e : list) {
              byte[] b = bspzk.getData(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-d" + "/"
                      + e,
                  false,
                  bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                      + "-d" + "/" + e, false));
              String[] receive = new String(b).split(Constants.SPLIT_FLAG);
              List<Integer> tmplist = new ArrayList<Integer>();
              tmplist.add(Integer.parseInt(receive[1]));
              tmplist.add(Integer.parseInt(receive[2]));
              partitionToRange.put(Integer.parseInt(receive[0]), tmplist);
            }
            closeZooKeeper();
            LOG.info(staffId + " leave the loadDataBarrier");
            return partitionToRange;
          }
        }
      }
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[loadDataBarrier]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[loadDataBarrier]", e);
      return null;
    }
  }
  @Override
  public boolean loadDataBarrier(SuperStepReportContainer ssrc,
      String partitionType) {
    LOG.info(staffId + " enter the loadDataBarrier");
    int checkNum = ssrc.getCheckNum();
    try {
      this.bspzk = getZooKeeper();
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
          + "/" + staffId.toString() + "-" + ssrc.getDirFlag()[0], new byte[0],
          BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());
      List<String> list = new ArrayList<String>();;
      while (true) {
        synchronized (mutex) {
          list = bspzk.getChildren(bspZKRoot + "/"
              + jobId.toString().substring(17) + "-d", true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " ,waiting......");
            mutex.wait();
          } else {
            closeZooKeeper();
            LOG.info(staffId + " leave the loadDataBarrier");
            return true;
          }
        }
      }
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[loadDataBarrier]", e);
      return false;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[loadDataBarrier]", e);
      return false;
    }
  }
  @Override
  public HashMap<Integer, Integer> loadDataInBalancerBarrier(
      SuperStepReportContainer ssrc, String partitionType) {
    LOG.info(staffId + " enter the loadDataInBalancerBarrier");
    String counterInfo = "";
    for (Integer e : ssrc.getCounter().keySet()) {
      counterInfo += e + Constants.SPLIT_FLAG + ssrc.getCounter().get(e)
          + Constants.SPLIT_FLAG;
    }
    counterInfo += staffId.toString().substring(26, 32);
    try {
      this.bspzk = getZooKeeper();
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
          + "/" + staffId.toString() + "-" + ssrc.getDirFlag()[0],
          counterInfo.getBytes(), BSPIds.OPEN_ACL_UNSAFE,
          new BSPCreateModeImpl().getPERSISTENT());
      List<String> list = new ArrayList<String>();
      HashMap<Integer, Integer> counter = new HashMap<Integer, Integer>();
      // Stat s = null;
      int checkNum = ssrc.getCheckNum();
      int copyNum = ssrc.getNumCopy();
      int[][] bucketHasHeadNodeInStaff = new int[checkNum * copyNum][checkNum];
      while (true) {
        synchronized (mutex) {
          list = bspzk.getChildren(bspZKRoot + "/"
              + jobId.toString().substring(17) + "-d", true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " waiting......");
            mutex.wait();
          } else {
            for (String e : list) {
              byte[] b = bspzk.getData(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-d" + "/"
                      + e,
                  false,
                  bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                      + "-d" + "/" + e, false));
              String receive[] = new String(b).split(Constants.SPLIT_FLAG);
              int sid = Integer.parseInt(receive[receive.length - 1]);
              for (int i = 0; i < receive.length - 1; i += 2) {
                int index = Integer.parseInt(receive[i]);
                int value = Integer.parseInt(receive[i + 1]);
                bucketHasHeadNodeInStaff[index][sid] += value;
                if (counter.containsKey(index)) {
                  counter.put(index, (counter.get(index) + value));
                } else {
                  counter.put(index, value);
                }
              }
            }
            int numHeadNode = 0;
            int[] index = new int[counter.keySet().size() + 1];
            int[] value = new int[counter.keySet().size() + 1];
            int k = 1;
            for (Integer ix : counter.keySet()) {
              index[k] = ix;
              value[k] = counter.get(ix);
              numHeadNode += value[k];
              k++;
            }
            HashMap<Integer, Integer> hashBucketToPartition
            = new HashMap<Integer, Integer>();
            HashMap<Integer, Integer> bucketIDToPartitionID
            = new HashMap<Integer, Integer>();
            int[] pid = new int[checkNum];
            boolean[] flag = new boolean[index.length];
            int avg = numHeadNode / checkNum;
            int[][][] partitionHasHeadNodeInStaff
            = new int[checkNum][checkNum][1];
            for (int n = 0; n < checkNum; n++) {
              boolean stop = false;
              while (!stop) {
                int id = find(value, flag, avg - pid[n]);
                if (id != 0) {
                  pid[n] += value[id];
                  flag[id] = true;
                  for (int i = 0; i < checkNum; i++) {
                    partitionHasHeadNodeInStaff[n][i][0]
                        += bucketHasHeadNodeInStaff[index[id]][i];
                  }
                  hashBucketToPartition.put(index[id], n);
                } else {
                  stop = true;
                }
              }
            }
            int id = pid.length - 1;
            for (int i = 1; i < flag.length; i++) {
              if (!flag[i]) {
                for (int j = 0; j < checkNum; j++) {
                  partitionHasHeadNodeInStaff[id % pid.length][j][0]
                      += bucketHasHeadNodeInStaff[index[i]][j];
                }
                hashBucketToPartition.put(index[i], id-- % pid.length);
                if (id == -1) {
                  id = pid.length - 1;
                  }
              }
            }
            boolean[] hasBeenSelected = new boolean[checkNum];
            for (int i = 0; i < checkNum; i++) {
              int partitionID = 0;
              int max = Integer.MIN_VALUE;
              for (int j = 0; j < checkNum; j++) {
                if (partitionHasHeadNodeInStaff[i][j][0] > max
                    && !hasBeenSelected[j]) {
                  max = partitionHasHeadNodeInStaff[i][j][0];
                  partitionID = j;
                }
              }
              hasBeenSelected[partitionID] = true;
              for (int e : hashBucketToPartition.keySet()) {
                if (hashBucketToPartition.get(e) == i) {
                  bucketIDToPartitionID.put(e, partitionID);
                }
              }
            }
            closeZooKeeper();
            LOG.info(staffId + " leave the loadDataInBalancerBarrier");
            return bucketIDToPartitionID;
          }
        }
      }
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[loadDataInBalancerBarrier]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.info("[loadDataInBalancerBarrier]", e);
      return null;
    }
  }
  /**
   * @param value value array
   * @param flag flag array
   * @param lack lack
   * @return partitionindex
   */
  private static int find(int value[], boolean flag[], int lack) {
    int id = 0;
    int min = Integer.MAX_VALUE;
    for (int i = value.length - 1; i > 0; i--) {
      if (!flag[i]) {
        if ((lack - value[i] >= 0) && (min > lack - value[i])) {
          min = lack - value[i];
          id = i;
        }
      }
    }
    return id;
  }
  @Override
  public boolean firstStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc) {
    LOG.info(staffId.toString() + " enter the firstStageSuperStepBarrier of "
        + Integer.toString(superStepCounter));
    List<String> list = new ArrayList<String>();
    int checkNum = ssrc.getCheckNum();
    try {
      this.bspzk = getZooKeeper();
      workerAgent.localBarrier(jobId, staffId, superStepCounter, ssrc);
      while (true) {
        synchronized (mutex) {
          list.clear();
          list = bspzk.getChildren(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-ss" + "/"
                  + Integer.toString(superStepCounter), true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " ,waiting......");
            LOG.info(list.toString());
            mutex.wait();
          } else {
            workerAgent.clearStaffRC(jobId);
            closeZooKeeper();
            LOG.info("[firstStageSuperStepBarrier]--[List]" + list.toString());
            LOG.info(staffId.toString()
                + " leave the firstStageSuperStepBarrier of "
                + Integer.toString(superStepCounter));
            return true;
          }
        }
      }
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
  public SuperStepCommand secondStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc) {
    LOG.info(staffId.toString() + " enter the secondStageSuperStepBarrier of "
        + Integer.toString(superStepCounter));
    List<String> list = new ArrayList<String>();
    int checkNum = ssrc.getCheckNum();
    try {
      this.bspzk = getZooKeeper();
      workerAgent.addCounters(jobId, this.counters);
      this.counters.clearCounters();
      workerAgent.localBarrier(jobId, staffId, superStepCounter, ssrc);
      while (true) {
        synchronized (mutex) {
          list.clear();
          list = bspzk.getChildren(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" + "/"
                  + Integer.toString(superStepCounter), true);
          LOG.info("[secondStageSuperStepBarrier] ZooKeeper get children....");
          if (list.size() < checkNum
              && bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                  + "-sc" + "/" + superStepCounter + "/"
                  + Constants.COMMAND_NAME, false) == null) {
            mutex.wait();
            LOG.info(list.size() + " of " + checkNum + " ,waiting......");
            LOG.info(list.toString());
          } else {
            workerAgent.clearStaffRC(jobId);
            LOG.info("[secondStageSuperStepBarrier]--[List]" + list.toString());
            byte[] b = bspzk.getData(
                bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" + "/"
                    + superStepCounter + "/" + Constants.COMMAND_NAME,
                false,
                bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                    + "-sc" + "/" + superStepCounter + "/"
                    + Constants.COMMAND_NAME, false));
            LOG.info("[Get Command Path]" + bspZKRoot + "/"
                + jobId.toString().substring(17) + "-sc" + "/"
                + superStepCounter + "/" + Constants.COMMAND_NAME);
            LOG.info("[Init Command String]" + new String(b));
            SuperStepCommand ssc = new SuperStepCommand(new String(b));
            closeZooKeeper();
            LOG.info(staffId.toString()
                + " leave the secondStageSuperStepBarrier of "
                + Integer.toString(superStepCounter));
            return ssc;
          }
        }
      }
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[secondStageSuperStepBarrier]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[secondStageSuperStepBarrier]", e);
      return null;
    } catch (Exception e) {
      closeZooKeeper();
      LOG.error("[secondStageSuperStepBarrier]", e);
      return null;
    }
  }
  @Override
  public SuperStepCommand secondStageSuperStepBarrierForRecovery(
      int superStepCounter) {
    LOG.info("secondStageSuperStepBarrierForRecovery(int superStepCounter) "
        + superStepCounter);
    SuperStepCommand ssc = null;
    try {
      this.bspzk = getZooKeeper();
      while (true) {
        if (bspzk.equaltoStat(bspZKRoot + "/" + jobId.toString().substring(17)
            + "-sc" + "/" + superStepCounter + "/" + Constants.COMMAND_NAME,
            false)) {
          break;
        } else {
          Thread.sleep(500);
        }
      }
      byte[] b = bspzk.getData(
          bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" + "/"
              + superStepCounter + "/" + Constants.COMMAND_NAME,
          false,
          bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
              + "/" + superStepCounter + "/" + Constants.COMMAND_NAME, false));
      LOG.info("byte[] b = zk.getData size: " + b.length);
      ssc = new SuperStepCommand(new String(b));
      closeZooKeeper();
      LOG.info(staffId.toString() + " recovery: superStepCounter: "
          + Integer.toString(superStepCounter));
      return ssc;
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[secondStageSuperStepBarrierForRecovery]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[secondStageSuperStepBarrierForRecovery]", e);
      return null;
    }
  }
  @Override
  public HashMap<Integer, String> checkPointStageSuperStepBarrier(
      int superStepCounter, SuperStepReportContainer ssrc) {
    LOG.info(staffId.toString()
        + " enter the checkPointStageSuperStepBarrier of "
        + Integer.toString(superStepCounter) + " : " + ssrc.getDirFlag()[0]);

    HashMap<Integer, String> workerNameAndPorts
    = new HashMap<Integer, String>();

    List<String> list = new ArrayList<String>();
    int checkNum = ssrc.getCheckNum();
    try {
      this.bspzk = getZooKeeper();
      workerAgent.localBarrier(jobId, staffId, superStepCounter, ssrc);
      while (true) {
        synchronized (mutex) {
          list.clear();
          list = bspzk.getChildren(
              bspZKRoot + "/" + jobId.toString().substring(17) + "-ss" + "/"
                  + Integer.toString(superStepCounter), true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " ,waiting......");
            mutex.wait();
          } else {
            workerAgent.clearStaffRC(jobId);
            if (ssrc.getStageFlag()
                != Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE) {
              return null;
            }
            for (String e : list) {
              LOG.info("Test ReadCK zookeeper:" + e);
              if (!e.endsWith(ssrc.getDirFlag()[0])) {
                continue;
              }
              byte[] b = bspzk.getData(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                      + "/" + Integer.toString(superStepCounter) + "/" + e,
                  false,
                  bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                      + "-ss" + "/" + Integer.toString(superStepCounter) + "/"
                      + e, false));
              LOG.info("Test path:" + bspZKRoot + "/"
                  + jobId.toString().substring(17) + "-ss" + "/"
                  + Integer.toString(superStepCounter) + "/" + e);
              LOG.info("Test initBytes:" + new String(b));
              String[] receive = new String(b).split(Constants.KV_SPLIT_FLAG);
              for (String str : receive) {
                LOG.info("Test firstSplit:" + str);
                String[] firstSplit = str.split(":");
                workerNameAndPorts.put(Integer.valueOf(firstSplit[0]),
                    firstSplit[1] + ":" + firstSplit[2]);
              }
            }
            closeZooKeeper();
            LOG.info(staffId.toString()
                + " leave the checkPointStageSuperStepBarrier of "
                + Integer.toString(superStepCounter) + " : "
                + ssrc.getDirFlag()[0]);
            return workerNameAndPorts;
          }
        }
      }
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[checkPointStageSuperStepBarrier]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[checkPointStageSuperStepBarrier]", e);
      return null;
    }
  }
  @Override
  public boolean saveResultStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc) {
    LOG.info(staffId.toString()
        + " enter the saveResultStageSuperStepBarrier of "
        + Integer.toString(superStepCounter));
    workerAgent.localBarrier(jobId, staffId, superStepCounter, ssrc);
    LOG.info(staffId.toString()
        + " leave the saveResultStageSuperStepBarrier of "
        + Integer.toString(superStepCounter));
    return true;
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
   * @return BSPzookeeper.
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
   * close zookeeper.
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
  public void zkWaitConnected(BSPZookeeper zk) {
    if (bspzk.equaltoState()) {
      try {
        this.connectedLatch.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }
  @Override
  public HashMap<Integer, String> scheduleBarrierForMigrate(
      SuperStepReportContainer ssrc) {
    LOG.info(staffId + " enter the scheduleBarrierForMigrate");
    String send = ssrc.getPartitionId() + Constants.SPLIT_FLAG
        + this.workerAgent.getWorkerManagerName(jobId, staffId)
        + Constants.SPLIT_FLAG + ssrc.getPort1() + "-" + ssrc.getPort2();
    LOG.info("Test in MigrateBarrier! "+send);
    try {
      this.bspzk = getZooKeeper();
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17)
          + "-migrate" + "/" + staffId.toString(), send.getBytes(),
          BSPIds.OPEN_ACL_UNSAFE, new BSPCreateModeImpl().getPERSISTENT());
      List<String> list = new ArrayList<String>();
      List<String> workerManagerNames = new ArrayList<String>();
      HashMap<Integer, String> partitionToWorkerManagerName
      = new HashMap<Integer, String>();
      int checkNum = ssrc.getCheckNum();
      while (true) {
        synchronized (mutex) {
          list.clear();
          list = bspzk.getChildren(bspZKRoot + "/"
              + jobId.toString().substring(17) + "-migrate", true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " ,waiting......");
            mutex.wait();
          } else {
            for (String e : list) {
              byte[] b = bspzk.getData(
                  bspZKRoot + "/" + jobId.toString().substring(17) + "-migrate"
                      + "/" + e,
                  false,
                  bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                      + "-migrate" + "/" + e, false));
              String[] receive = new String(b).split(Constants.SPLIT_FLAG);
              String hostWithPort = receive[1] + ":" + receive[2];
              LOG.info(receive[0]+hostWithPort);
              partitionToWorkerManagerName.put(Integer.valueOf(receive[0]),
                  hostWithPort);
              workerAgent.setWorkerNametoPartitions(jobId,
                  Integer.valueOf(receive[0]), receive[1]);
              if (!workerManagerNames.contains(receive[1])) {
                workerManagerNames.add(receive[1]);
              }
            }
            this.workerAgent.setNumberWorkers(jobId, staffId,
                workerManagerNames.size());
            list.clear();
            workerManagerNames.clear();
            break;
          }
        }
      }
      closeZooKeeper();
      LOG.info(staffId + " leave the scheduleBarrierForMigrate");
      return partitionToWorkerManagerName;
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[schedulerBarrierForMigrate]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.error("[schedulerBarrierForMigrate]", e);
      return null;
    }
  }
  @Override
  public HashMap<Integer, Integer> rangerouter(SuperStepReportContainer ssrc) {
    LOG.info(staffId + " enter the loadDataInRangeBalancerBarrier");
    String counterInfo = "";
    for (Integer e : ssrc.getCounter().keySet()) {
      LOG.info("the e is " + e + " the value is " + ssrc.getCounter().get(e));
      counterInfo += e + Constants.SPLIT_FLAG + ssrc.getCounter().get(e)
          + Constants.SPLIT_FLAG;
    }
    counterInfo += staffId.toString().substring(26, 32);
    try {
      this.bspzk = getZooKeeper();
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
          + "/" + staffId.toString() + "-" + ssrc.getDirFlag()[0],
          counterInfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      List<String> list = new ArrayList<String>();
      HashMap<Integer, Integer> counter = new HashMap<Integer, Integer>();
      Stat s = null;
      int checkNum = ssrc.getCheckNum();
      while (true) {
        synchronized (mutex) {
          list = bspzk.getChildren(bspZKRoot + "/"
              + jobId.toString().substring(17) + "-d", true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " waiting......");
            mutex.wait();
          } else {
            for (String e : list) {
              s = bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                  + "-d" + "/" + e, false);
              byte[] b = bspzk.getData(bspZKRoot + "/"
                  + jobId.toString().substring(17) + "-d" + "/" + e, false, s);
              String[] receive = new String(b).split(Constants.SPLIT_FLAG);
              for (int i = 0; i < receive.length - 1; i += 2) {
                int index = Integer.parseInt(receive[i]);
                int value = Integer.parseInt(receive[i + 1]);
                counter.put(index, value);
                LOG.info("the index is " + index + "the value is " + value);
              }
            }
            return counter;
          }
        }
      }
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[loadDataInBalancerBarrier]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.info("[loadDataInBalancerBarrier]", e);
      return null;
    }
  }
  @Override
  public HashMap<Integer, Integer> loadDataInBalancerBarrieraForEdgeBalance(
      SuperStepReportContainer ssrc, String partitionType) {
    LOG.info(staffId + " enter the loadDataInBalancerBarrieraForEdgeBalance");
    int checkNum = ssrc.getCheckNum();
    int copyNum = ssrc.getNumCopy();
    int bucketNum = checkNum * copyNum;
    int[][] bucketToPartitionMaxEdge = new int[bucketNum][2];
    String edgeCounterInfo = "";
    String bucketToPerPartitionEdgeCounterInfo = "";
    for (Integer e : ssrc.getCounter().keySet()) {
      edgeCounterInfo += e + Constants.SPLIT_FLAG + ssrc.getCounter().get(e)
          + Constants.SPLIT_FLAG;
    }
    edgeCounterInfo += staffId.toString().substring(26, 32);
    edgeCounterInfo += "/t";
    for (int i = 0; i < ssrc.getBucketToPartitionEdgeCounter().length; i++) {
      bucketToPerPartitionEdgeCounterInfo += i + Constants.SPLIT_FLAG;
      for (int j = 0; j < ssrc.getBucketToPartitionEdgeCounter()[i].length;
          j++) {
        bucketToPerPartitionEdgeCounterInfo += ssrc
            .getBucketToPartitionEdgeCounter()[i][j] + Constants.SPLIT_FLAG;
      }
    }
    String counterInfo = "";
    counterInfo = edgeCounterInfo + bucketToPerPartitionEdgeCounterInfo;
    try {
      this.bspzk = getZooKeeper();
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
          + "/" + staffId.toString() + "-" + ssrc.getDirFlag()[0],
          counterInfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      List<String> list = new ArrayList<String>();
      HashMap<Integer, Integer> edgeCounter = new HashMap<Integer, Integer>();
      int[][] bucketToPartitionEdgeCounter = new int[bucketNum][checkNum];
      Stat s = null;
      int[][] bucketHasEdgeInStaff = new int[bucketNum][checkNum];
      while (true) {
        synchronized (mutex) {
          list = bspzk.getChildren(bspZKRoot + "/"
              + jobId.toString().substring(17) + "-d", true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " waiting......");
            mutex.wait();
          } else {
            for (String e : list) {
              s = bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                  + "-d" + "/" + e, false);
              byte[] b = bspzk.getData(bspZKRoot + "/"
                  + jobId.toString().substring(17) + "-d" + "/" + e, false, s);
              String[] receive = new String(b).split("/t");
              String edgeString = receive[0];
              String edgePerPartitonString = receive[1];
              String[] edge = edgeString.split(Constants.SPLIT_FLAG);
              int sid = Integer.parseInt(edge[edge.length - 1]);
              for (int i = 0; i < edge.length - 1; i += 2) {
                int index = Integer.parseInt(edge[i]);
                int value = Integer.parseInt(edge[i + 1]);
                bucketHasEdgeInStaff[index][sid] += value;
                if (edgeCounter.containsKey(index)) {
                  edgeCounter.put(index, (edgeCounter.get(index) + value));
                } else {
                  edgeCounter.put(index, value);
                }
              }
              String[] edgePerPartiton = edgePerPartitonString
                  .split(Constants.SPLIT_FLAG);
              for (int i = 0; i < edgePerPartiton.length; i += (checkNum + 1)) {
                int bucketID = Integer.parseInt(edgePerPartiton[i]);
                for (int j = i + 1; j < i + 1 + checkNum; j++) {
                  bucketToPartitionEdgeCounter[bucketID][j - i - 1] += Integer
                      .parseInt(edgePerPartiton[j]);
                }
              }
            }
            // again
            int edgeNum = 0;
            int[] index = new int[bucketNum + 1];
            int[] value = new int[bucketNum + 1];
            int k = 1;
            for (Integer ix : edgeCounter.keySet()) {
              index[k] = ix;
              value[k] = edgeCounter.get(ix);
              edgeNum += value[k];
              k++;
            }
            for (int i = 0; i < bucketToPartitionEdgeCounter.length; i++) {
              int max = bucketToPartitionEdgeCounter[i][0];
              int maxPartitionID = 0;
              for (int j = 1; j < bucketToPartitionEdgeCounter[i].length; j++) {
                if (bucketToPartitionEdgeCounter[i][j] > max) {
                  max = bucketToPartitionEdgeCounter[i][j];
                  maxPartitionID = j;
                }
              }
              bucketToPartitionMaxEdge[i][0] = maxPartitionID;
              bucketToPartitionMaxEdge[i][1] = max;
            }
            //
            HashMap<Integer, Integer> hashBucketToPartition
            = new HashMap<Integer, Integer>();
            HashMap<Integer, Integer> bucketIDToPartitionID
            = new HashMap<Integer, Integer>();
            int[] pid = new int[checkNum];
            boolean[] flag = new boolean[index.length];
            LOG.info("There are " + edgeNum + " edges in the datasets");
            int avg = (int) ((1 + 0.02) * edgeNum / checkNum);
            LOG.info("The average edges in per partitions are " + avg);
            int[][][] partitionHasEdgeInStaff = new int[checkNum][checkNum][1];
            for (Integer bucketid : edgeCounter.keySet()) {
              // LOG.info("this bucketid is "+bucketid);
              if (edgeCounter.get(bucketid) / checkNum
                  < bucketToPartitionMaxEdge[bucketid][1]) {
                if (!(flag[bucketid])
                    && (avg - pid[bucketToPartitionMaxEdge[bucketid][0]] > 0)) {
                  flag[bucketid] = true;
                  pid[bucketToPartitionMaxEdge[bucketid][0]]
                      += bucketToPartitionMaxEdge[bucketid][1];
                  for (int i = 0; i < checkNum; i++) {
                    partitionHasEdgeInStaff[bucketToPartitionMaxEdge
                        [bucketid][0]][i][0]
                            += bucketHasEdgeInStaff[index[bucketid]][i];
                  }
                  hashBucketToPartition.put(bucketid,
                      bucketToPartitionMaxEdge[bucketid][0]);
                }
              }
            }
            for (int n = 0; n < checkNum; n++) {
              boolean stop = false;
              while (!stop) {
                int id = find(value, flag, avg - pid[n]);
                if (id != 0) {
                  pid[n] += value[id];
                  flag[id] = true;
                  for (int i = 0; i < checkNum; i++) {
                    partitionHasEdgeInStaff[n][i][0]
                        += bucketHasEdgeInStaff[index[id]][i];
                  }
                  hashBucketToPartition.put(index[id], n);
                } else {
                  stop = true;
                }
              }
            }
            int id = pid.length - 1;
            for (int i = 1; i < flag.length; i++) {
              if (!flag[i]) {
                for (int j = 0; j < checkNum; j++) {
                  partitionHasEdgeInStaff[id % pid.length][j][0]
                      += bucketHasEdgeInStaff[index[i]][j];
                }
                hashBucketToPartition.put(index[i], id-- % pid.length);
                if (id == -1) {
                  id = pid.length - 1;
                  }
              }
            }
            boolean[] hasBeenSelected = new boolean[checkNum];
            for (int i = 0; i < checkNum; i++) {
              int partitionID = 0;
              int max = Integer.MIN_VALUE;
              for (int j = 0; j < checkNum; j++) {
                if (partitionHasEdgeInStaff[i][j][0] > max
                    && !hasBeenSelected[j]) {
                  max = partitionHasEdgeInStaff[i][j][0];
                  partitionID = j;
                }
              }
              hasBeenSelected[partitionID] = true;
              for (int e : hashBucketToPartition.keySet()) {
                if (hashBucketToPartition.get(e) == i) {
                  bucketIDToPartitionID.put(e, partitionID);
                }
              }
            }
            HashMap<Integer, Integer> partitionEdge
            = new HashMap<Integer, Integer>();
            for (Integer e : bucketIDToPartitionID.keySet()) {
              if (partitionEdge.containsKey(bucketIDToPartitionID.get(e))) {
                partitionEdge.put(
                    bucketIDToPartitionID.get(e),
                    edgeCounter.get(e)
                        + partitionEdge.get(bucketIDToPartitionID.get(e)));
              } else {
                partitionEdge.put(bucketIDToPartitionID.get(e),
                    edgeCounter.get(e));
              }
            }
            for (Integer e : partitionEdge.keySet()) {
              LOG.info("In partition " + e + " there are "
                  + partitionEdge.get(e) + " edges!");
            }
            closeZooKeeper();
            LOG.info(staffId
                + " leave the loadDataInBalancerBarrieraForEdgeBalance");
            return bucketIDToPartitionID;
          }
        }
      }
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[loadDataInBalancerBarrieraForEdgeBalance]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.info("[loadDataInBalancerBarrieraForEdgeBalance]", e);
      return null;
    }
  }

  @Override
  public HashMap<Integer, Integer> loadDataInBalancerBarrieraForIBHP(
      SuperStepReportContainer ssrc, String partitionType) {
    LOG.info(staffId + " enter the loadDataInBalancerBarrieraForIBHP");
    int checkNum = ssrc.getCheckNum();
    int copyNum = ssrc.getNumCopy();
    int bucketNum = checkNum * copyNum;
    ArrayList<Integer>[] aPlist = new ArrayList[checkNum];
    String edgeCounterInfo = "";
    String bucketToPerBucketEdgeCounterInfo = "";
    for (Integer e : ssrc.getCounter().keySet()) {
      edgeCounterInfo += e + Constants.SPLIT_FLAG + ssrc.getCounter().get(e)
          + Constants.SPLIT_FLAG;
    }
    edgeCounterInfo += staffId.toString().substring(26, 32);
    edgeCounterInfo += "/t";
    for (int i = 0; i < bucketNum; i++) {
      bucketToPerBucketEdgeCounterInfo += i + Constants.SPLIT_FLAG;
      for (int j = 0; j < bucketNum; j++) {
        bucketToPerBucketEdgeCounterInfo
        += ssrc.getBucketToBucketEdgeCounter()[i][j]
            + Constants.SPLIT_FLAG;
      }
    }
    String counterInfo = "";
    counterInfo = edgeCounterInfo + bucketToPerBucketEdgeCounterInfo;
    try {
      this.bspzk = getZooKeeper();
      bspzk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
          + "/" + staffId.toString() + "-" + ssrc.getDirFlag()[0],
          counterInfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      List<String> list = new ArrayList<String>();
      HashMap<Integer, Integer> edgeCounter = new HashMap<Integer, Integer>();
      int[][] bucketToBucketEdgeCounter = new int[bucketNum][bucketNum];
      Stat s = null;
      int[][] bucketHasEdgeInStaff = new int[bucketNum][checkNum];
      while (true) {
        synchronized (mutex) {
          list = bspzk.getChildren(bspZKRoot + "/"
              + jobId.toString().substring(17) + "-d", true);
          if (list.size() < checkNum) {
            LOG.info(list.size() + " of " + checkNum + " waiting......");
            mutex.wait();
          } else {
            for (String e : list) {
              s = bspzk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                  + "-d" + "/" + e, false);
              byte[] b = bspzk.getData(bspZKRoot + "/"
                  + jobId.toString().substring(17) + "-d" + "/" + e, false, s);
              String[] receive = new String(b).split("/t");
              String edgeString = receive[0];
              String edgePerBucketString = receive[1];
              String[] edge = edgeString.split(Constants.SPLIT_FLAG);
              int sid = Integer.parseInt(edge[edge.length - 1]);
              for (int i = 0; i < edge.length - 1; i += 2) {
                int index = Integer.parseInt(edge[i]);
                int value = Integer.parseInt(edge[i + 1]);
                bucketHasEdgeInStaff[index][sid] += value;
                if (edgeCounter.containsKey(index)) {
                  edgeCounter.put(index, (edgeCounter.get(index) + value));
                } else {
                  edgeCounter.put(index, value);
                }
              }
              String[] edgePerBucket = edgePerBucketString
                  .split(Constants.SPLIT_FLAG);
              for (int i = 0; i < edgePerBucket.length; i += (bucketNum + 1)) {
                int bucketID = Integer.parseInt(edgePerBucket[i]);
                for (int j = i + 1; j < i + 1 + bucketNum; j++) {
                  bucketToBucketEdgeCounter[bucketID][j - i - 1] += Integer
                      .parseInt(edgePerBucket[j]);
                }
              }
            }
            int edgeNum = 0;
            int[] index = new int[bucketNum + 1];
            int[] value = new int[bucketNum + 1];
            int k = 1;
            for (Integer ix : edgeCounter.keySet()) {
              index[k] = ix;
              value[k] = edgeCounter.get(ix);
              edgeNum += value[k];
              k++;
            }
            HashMap<Integer, Integer> hashBucketToPartition
            = new HashMap<Integer, Integer>();
            HashMap<Integer, Integer> bucketIDToPartitionID
            = new HashMap<Integer, Integer>();
            int[] pid = new int[checkNum];
            boolean[] flag = new boolean[index.length];
            LOG.info("There are " + edgeNum + " edges in the datasets");
            int avg = (int) ((1 + 0.02) * edgeNum / checkNum);
            LOG.info("The average edges in per partitions are " + avg);
            int[][][] partitionHasEdgeInStaff = new int[checkNum][checkNum][1];
            for (int len = 0; len < checkNum; len++) {
              aPlist[len] = new ArrayList<Integer>();
              }
            for (k = 0; k < checkNum; k++) {
              hashBucketToPartition.put(k, k);
              pid[k] += edgeCounter.get(k);
              aPlist[k].add(k);
            }
            for (int i = checkNum; i < bucketNum; i++) {
              int aPid = 0;
              float max = Integer.MIN_VALUE;
              for (int j = 0; j < checkNum; j++) {
                int tempParCap = 0;
                float temp = 0;
                for (int k1 = 0; k1 < aPlist[j].size(); k1++) {
                  tempParCap += bucketToBucketEdgeCounter[i][aPlist[j].get(k1)];
                  temp = tempParCap * (1 - (pid[j] / avg));
                  if (temp > max) {
                    max = temp;
                    aPid = j;
                  }
                }
              }
              aPlist[aPid].add(i);
              if (edgeCounter.containsKey(i)) {
                pid[aPid] += edgeCounter.get(i);
                }
              for (int h = 0; h < checkNum; h++) {
                partitionHasEdgeInStaff[aPid][h][0]
                    += bucketHasEdgeInStaff[i][h];
              }
              hashBucketToPartition.put(i, aPid);
            }
            boolean[] hasBeenSelected = new boolean[checkNum];
            for (int i = 0; i < checkNum; i++) {
              int partitionID = 0;
              int max = Integer.MIN_VALUE;
              for (int j = 0; j < checkNum; j++) {
                if (partitionHasEdgeInStaff[i][j][0] > max
                    && !hasBeenSelected[j]) {
                  max = partitionHasEdgeInStaff[i][j][0];
                  partitionID = j;
                }
              }
              hasBeenSelected[partitionID] = true;
              for (int e : hashBucketToPartition.keySet()) {
                if (hashBucketToPartition.get(e) == i) {
                  bucketIDToPartitionID.put(e, partitionID);
                }
              }
            }
            closeZooKeeper();
            LOG.info(staffId
                + " leave the loadDataInBalancerBarrieraForEdgeBalance");
            return bucketIDToPartitionID;
          }
        }
      }
    } catch (KeeperException e) {
      closeZooKeeper();
      LOG.error("[loadDataInBalancerBarrieraForEdgeBalance]", e);
      return null;
    } catch (InterruptedException e) {
      closeZooKeeper();
      LOG.info("[loadDataInBalancerBarrieraForEdgeBalance]", e);
      return null;
    }
  }
}
