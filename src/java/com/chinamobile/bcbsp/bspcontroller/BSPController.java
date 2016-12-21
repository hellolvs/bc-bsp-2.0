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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.Constants.BspControllerRole;
import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.action.KillStaffAction;
import com.chinamobile.bcbsp.action.WorkerManagerAction;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.StaffInProgress;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.fault.storage.MonitorFaultLog;
import com.chinamobile.bcbsp.http.HttpServer;
import com.chinamobile.bcbsp.rpc.ControllerProtocol;
import com.chinamobile.bcbsp.rpc.JobSubmissionProtocol;
import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFSDataInputStream;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileSystem;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPPath;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFSDataInputStreamImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFileSystemImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFspermissionImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPLocalFileSystemImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPPathImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPConnectionLossException;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPIds;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPCreateModeImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPZookeeperImpl;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.JobProfile;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffStatus;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerManagerControlInterface;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

/**
 * BSPController is responsible to control all the WorkerManagers and to manage
 * bsp jobs.
 * @author
 * @version
 */
public class BSPController implements JobSubmissionProtocol,
    ControllerProtocol, WorkerManagerControlInterface, Watcher {
  /** Initalize log information. */
  private static final Log LOG = LogFactory.getLog(BSPController.class);
  /** Time interval before nexttime access system file. */
  private static final int FS_ACCESS_RETRY_PERIOD = 10000;
  /** Worker manager that are available. */
  private static ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol>
  whiteWorkerManagers =
      new ConcurrentHashMap<WorkerManagerStatus, WorkerManagerProtocol>();
  /** Worker manager that are warned. */
  private static ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol>
  grayWorkerManagers =
      new ConcurrentHashMap<WorkerManagerStatus, WorkerManagerProtocol>();
  /** Heart beat interval and . */
  private static long HEART_BEAT_TIMEOUT;
  /**Heart beat pause interval*/
  private static long PAUSE_TIMEOUT;
  /** Increment update time interval for HA change Master. */
  private static long HEARTBAETSTOPINTERVAL = 30000;
  /** fault process thread waits for the exceptions to be reported. */
  private static long FAULTPROCESSINTERVAL = 1000;
  /** Monitor the fault log. */
  private MonitorFaultLog mfl;
  /** Initalize BSP configuration. */
  private BSPConfiguration conf;
  /** Different states of BSPController. */
  public static enum State {
    /**BSP controller state is initializing*/
    INITIALIZING,
    /**BSP controller state is running*/
    RUNNING
  }
  /** BSPcontroller rightnow state. */
  State state = State.INITIALIZING;
  /** BspController's Role for HA. */
  private BspControllerRole role = BspControllerRole.NEUTRAL;
  /** Initialize BSPZookeeper for control. */
  private BSPZookeeper bspzk = null;
  /** Operator for hdfs log. */
  private HDFSOperator haLogOperator;
  /** Identifier of controller. */
  private String controllerIdentifier;
  /** private Server interServer. */
  private Server controllerServer;
  /** HTTP Server. */
  private HttpServer infoServer;
  /** Port num. */
  private int infoPort;
  /** Get the staffstaus and put staffStatus into HashMap. */
  private StaffStatus[] stf;
  /** Hashmap of staffs. */
  private HashMap<StaffAttemptID, StaffStatus> staffs =
      new HashMap<StaffAttemptID, StaffStatus>();
  
  /** BSP system file. */
  private BSPFileSystem bspfs = null;
  /** BSP system Dir path. */
  private BSPPath bspsystemDir = null;
  /** Next job id. */
  private Integer nextJobId = Integer.valueOf(1);
  /** How many jobs has been submitted by clients. */
  private int totalSubmissions = 0;
  /** Currnetly running tasks. */
  private int runningClusterStaffs = 0;
  /** Max tasks that the Cluster can run. */
  private int maxClusterStaffs = 0;
  /** Map for JobID and Jip. */
  private Map<BSPJobID, JobInProgress> jobs =
      new ConcurrentHashMap<BSPJobID, JobInProgress>();
  /** Scheduler to scheduler staffs. */
  private StaffScheduler staffScheduler;
  /** Jips listeners list. */
  private final List<JobInProgressListener> jobInProgressListeners =
      new CopyOnWriteArrayList<JobInProgressListener>();
  /** check cluster timeout. */
  private CheckTimeOut cto;
  /** Map for predict worker staff loads in future. */
  private ConcurrentMap<String, ArrayList<StaffStatus>> workerToStaffs = new
      ConcurrentHashMap<String, ArrayList<StaffStatus>>();
  /** Process fault that have been reported to BSPcontroller. */
  private ProcessFault pf;
  /** Map for jops and its staffstatus and workermanagerstatus. */
  private Map<BSPJobID, Map<StaffStatus, WorkerManagerStatus>> faultMap =
      new ConcurrentHashMap<BSPJobID, Map<StaffStatus, WorkerManagerStatus>>();
  /** WorkerManagerStatus that has fault in. */
  private List<WorkerManagerStatus> faultWMS =
      new ArrayList<WorkerManagerStatus>();
  /**
   * This thread will run until stopping the cluster. It will check weather the
   * heart beat interval is time-out or not.
   * @author WangZhigang
   * Review comments: (1)The level of log is not clear. I
   *         think the HEART_BEAT_TIMEOUT should be the level of "warn" or
   *         "error", instead of "info". Review time: 2011-11-30; Reviewer:
   *         Hongxu Zhang. Fix log: (1)Now, if a workermanager has been
   *         HEART_BEAT_TIMEOUT, it will be viewed as fault worker. So the level
   *         of log should be "error". Fix time: 2011-12-04; Programmer: Zhigang
   *         Wang.
   */
  public class CheckTimeOut extends Thread {
    /**
     * This thread will run until stopping the cluster. It will check weather
     * the heart beat interval is time-out or not.
     */
    public void run() {
      // add by chen
      try {
        Thread.sleep(HEARTBAETSTOPINTERVAL);
      } catch (Exception e) {
        //LOG.error("change failed!!!", e);
        throw new RuntimeException("change failed!!! ", e);
      }
      while (true) {
        long nowTime = System.currentTimeMillis();
        for (WorkerManagerStatus wms : whiteWorkerManagers.keySet()) {
          long timeout = nowTime - wms.getLastSeen();
          if (timeout > HEART_BEAT_TIMEOUT) {
            LOG.error("[Fault Detective]" +
                "The worker's time out is catched in WhiteList: " +
                wms.getWorkerManagerName());
            if (wms.getStaffReports().size() != 0) {
              // workerFaultPreProcess(wms);
              wms.setFault();
              faultWMS.add(wms);
              List<StaffStatus> staffStatusList = wms.getStaffReports();
              for (StaffStatus ss : staffStatusList) {
                BSPJobID bspJobID = ss.getJobId();
                if (faultMap.containsKey(bspJobID)) {
                  faultMap.get(bspJobID).put(ss, wms);
                } else {
                  Map<StaffStatus, WorkerManagerStatus> ssToWMS =
                      new ConcurrentHashMap<StaffStatus, WorkerManagerStatus>();
                  ssToWMS.put(ss, wms);
                  faultMap.put(bspJobID, ssToWMS);
                }
              }
            }
            removeWorker(wms);
          }
        }
        for (WorkerManagerStatus wms : grayWorkerManagers.keySet()) {
          long timeout = nowTime - wms.getLastSeen();
          if (timeout > HEART_BEAT_TIMEOUT) {
            LOG.error("[Fault Detective]" +
               "The worker's time out is catched in GrayList: " +
              wms.getWorkerManagerName());
            if (wms.getStaffReports().size() != 0) {
              // workerFaultPreProcess(wms);
              wms.setFault();
              faultWMS.add(wms);
              List<StaffStatus> staffStatusList = wms.getStaffReports();
              for (StaffStatus ss : staffStatusList) {
                BSPJobID bspJobID = ss.getJobId();
                if (faultMap.containsKey(bspJobID)) {
                  faultMap.get(bspJobID).put(ss, wms);
                } else {
                  Map<StaffStatus, WorkerManagerStatus> ssToWMS =
                      new ConcurrentHashMap<StaffStatus, WorkerManagerStatus>();
                  ssToWMS.put(ss, wms);
                  faultMap.put(bspJobID, ssToWMS);
                }
              }
            }
            removeWorker(wms);
          }
          timeout = nowTime - wms.getPauseTime();
          if (timeout > PAUSE_TIMEOUT) {
            if (grayWorkerManagers.containsKey(wms)) {
              LOG.warn(wms.getWorkerManagerName() +
                  " will be transferred from [GrayList] to [WhiteList]");
              WorkerManagerProtocol wmp = grayWorkerManagers.remove(wms);
              wmp.clearFailedJobList();
              wms.setPauseTime(0);
              whiteWorkerManagers.put(wms, wmp);
            }
          }
        }
        try {
          Thread.sleep(HEART_BEAT_TIMEOUT);
        } catch (Exception e) {
          //LOG.error("CheckTimeOut", e);
          throw new RuntimeException("CheckTimeOut", e);
        }
      }
    }
  }

  /**
   * Fault process thread to handle worker exception and staff exception.
   * @author Baoxing Yang
   */
  public class ProcessFault extends Thread {
    /**
     * This thread run until the cluster stop to hanld exceptions have been
     * reported.
     */
    public void run() {
      while (true) {
        if (!faultWMS.isEmpty()) {
          Iterator<WorkerManagerStatus> faultWMSIterator = faultWMS.iterator();
          while (faultWMSIterator.hasNext()) {
            WorkerManagerStatus wms = faultWMSIterator.next();
            workerFaultPreProcess(wms);
            faultWMS.remove(wms);
          }
        }
        Iterator<BSPJobID> bspJobIdIterator = faultMap.keySet().iterator();
        while (bspJobIdIterator.hasNext()) {
          BSPJobID jobID1 = bspJobIdIterator.next();
          Map<StaffStatus, WorkerManagerStatus> ssMap = faultMap.get(jobID1);
          if (ssMap.size() == 1) {
            try {
              Thread.sleep(FAULTPROCESSINTERVAL);
              ssMap = faultMap.get(jobID1);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              throw new RuntimeException("[ProcessFault]" +
                  "InterruptedException", e);
            }
          }
          Iterator<StaffStatus> ssIterator = ssMap.keySet().iterator();
          while (ssIterator.hasNext()) {
            StaffStatus ss = ssIterator.next();
            WorkerManagerStatus wms = ssMap.get(ss);
            if (wms.isFault()) {
              continue;
            }
            JobInProgress jip = whichJob(ss.getJobId());
            LOG.info("Begin process fault staff " + ss.getStaffId());
            if (ss.getStage() == 0 || ss.getStage() == 2) {
              // reported by workers before local compute or reported by workers
              // after local compute
              boolean isRecovery = recovery(ss);
              if (!isRecovery) {
                killJob(jip);
              }
            } else if (ss.getStage() == 1) {
              // reported by workers local compute
              LOG.info(ss.getStaffId() +
                  "enter staffFaultPreProcess !########");
              boolean isRecovery = staffFaultPreProcess(ss, wms);
              LOG.info("checkfault [isRecovery]: " + isRecovery);
              if (!isRecovery) {
                killJob(jip);
              }
            }
            LOG.info("End process fault staff " + ss.getStaffId());
            faultMap.get(jobID1).remove(ss);
          }
          if (ssMap.isEmpty()) {
            faultMap.remove(jobID1);
            LOG.info("faultMap is empty:" + faultMap.isEmpty());
          }
        }
        try {
          Thread.sleep(FAULTPROCESSINTERVAL);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          throw new RuntimeException("[ProcessFault]" +
              "InterruptedException!", e);
        }
      }
    }
  }

  /**
   * BSPcontroller construction method.
   */
  public BSPController() {
  }

  /**
   * Start the BSPController process, listen on the indicated hostname/port.
   * @param conf
   *        BSPsystem configuration
   * @throws IOException
   *         System IO exception
   * @throws InterruptedException
   *         Thread interrupted exception.
   */
  public BSPController(BSPConfiguration conf) throws IOException,
      InterruptedException {
    this(conf, generateNewIdentifier());
  }

  /**
   * BSPcontroller construction method with BSP conf and controller identifier.
   * @param conf
   *        BSP system configuration
   * @param identifier
   *        BSpcontroller identifier.
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("static-access")
  BSPController(BSPConfiguration conf, String identifier) throws IOException,
      InterruptedException {
    this.conf = conf;
    this.controllerIdentifier = identifier;
    String zkAddress = conf.get(Constants.ZOOKEEPER_QUORUM) +
        ":" +
        conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
            Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    // this.zk = new ZooKeeper(zkAddress, Constants.SESSION_TIME_OUT, this);
    bspzk = new BSPZookeeperImpl(zkAddress, Constants.SESSION_TIME_OUT, this);
    // determine the role of the BspController
    this.determineBspControllerRole();
    LOG.info("the BspController's role is :" + this.role);
    this.haLogOperator = new HDFSOperator();
    if (this.role.equals(BspControllerRole.ACTIVE)) {
      LOG.info(" before this.mfl=new MonitorFaultLog();");
      this.mfl = new MonitorFaultLog();
      this.haLogOperator.deleteFile(conf.get(Constants.BC_BSP_HA_LOG_DIR));
      this.haLogOperator.createFile(conf.get(Constants.BC_BSP_HA_LOG_DIR) +
         Constants.BC_BSP_HA_SUBMIT_LOG);
    }
    // Create the scheduler and init scheduler services
    Class<? extends StaffScheduler> schedulerClass = conf.getClass(
        "bsp.master.taskscheduler", SimpleStaffScheduler.class,
        StaffScheduler.class);
    this.staffScheduler = (StaffScheduler) ReflectionUtils.newInstance(
        schedulerClass, conf);
    this.staffScheduler.setRole(role);
    String host = getAddress(conf).getHostName();
    int port = getAddress(conf).getPort();
    LOG.info("RPC BSPController: host " + host + " port " + port);
    this.controllerServer = RPC.getServer(this, host, port, conf);
    infoPort = conf.getInt("bsp.http.infoserver.port", 40026);
    // infoPort = 40026;
    infoServer = new HttpServer("bcbsp", host, infoPort, true, conf);
    infoServer.setAttribute("bcbspController", this);
    // starting webserver
    infoServer.start();
    this.HEART_BEAT_TIMEOUT = conf.getLong(Constants.HEART_BEAT_TIMEOUT, 5000);
    this.PAUSE_TIMEOUT = conf.getInt(Constants.SLEEP_TIMEOUT, 10000);
    this.cto = new CheckTimeOut();
    // Baoxing Yang added
    this.pf = new ProcessFault();
    while (!Thread.currentThread().isInterrupted()) {
      try {
        if (bspfs == null) {
          // fs = FileSystem.get(conf);
          bspfs = new BSPFileSystemImpl(conf);
        }
        // Clean up the system dir, which will only work if hdfs is out
        // of safe mode.
        if (bspsystemDir == null) {
          // systemDir = new Path(getSystemDir());
          bspsystemDir = new BSPPathImpl(getSystemDir());
        }
        if (role.equals(BspControllerRole.ACTIVE)) {
          LOG.info("Cleaning up the system directory");
          LOG.info(bspsystemDir);
          // fs.delete(systemDir, true);
          bspfs.delete(bspsystemDir.getSystemDir(), true);
          // if (FileSystem.mkdirs(fs, systemDir, new FsPermission(
          // SYSTEM_DIR_PERMISSION)))
          if (BSPFileSystemImpl.mkdirs(bspfs.getFs(),
              bspsystemDir.getSystemDir(),
              new BSPFspermissionImpl(1).getFp())) {
            break;
          }
          LOG.error("Mkdirs failed to create " + bspsystemDir);
        } else {
          break;
        }
      } catch (AccessControlException ace) {
        LOG.warn("Failed to operate on bsp.system.dir (" + bspsystemDir +
           ") because of permissions.");
        LOG.warn("Manually delete the bsp.system.dir (" + bspsystemDir +
           ") and then start the BSPController.");
        LOG.warn("Bailing out ... ");
        throw ace;
      } catch (IOException ie) {
        //LOG.error("problem cleaning system directory: " + bspsystemDir, ie);
        throw new RuntimeException("problem cleaning system directory: " +
          bspsystemDir, ie);
      }
      Thread.sleep(FS_ACCESS_RETRY_PERIOD);
    }
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
    deleteLocalFiles(Constants.BC_BSP_LOCAL_SUBDIR_CONTROLLER);
  }

  @Override
  public WorkerManagerProtocol removeWorkerFromWhite(WorkerManagerStatus wms) {
    if (whiteWorkerManagers.containsKey(wms)) {
      return whiteWorkerManagers.remove(wms);
    } else {
      return null;
    }
  }

  @Override
  public void addWorkerToGray(WorkerManagerStatus wms,
      WorkerManagerProtocol wmp) {
    if (!grayWorkerManagers.containsKey(wms)) {
      grayWorkerManagers.put(wms, wmp);
    }
  }

  @Override
  public int getMaxFailedJobOnWorker() {
    return conf.getInt(Constants.BC_BSP_FAILED_JOB_PER_WORKER, 0);
  }

  /**
   * A WorkerManager registers with its status to BSPController when startup,
   * which will update WorkerManagers cache.
   * @param status
   *        to be updated in cache.
   * @return true if registering successfully; false if fail.
   */
  @Override
  public boolean register(WorkerManagerStatus status) throws IOException {
    if (null == status) {
      LOG.error("No worker server status.");
      throw new NullPointerException("No worker server status.");
    }
    Throwable e = null;
    try {
      WorkerManagerProtocol wc = (WorkerManagerProtocol) RPC.waitForProxy(
          WorkerManagerProtocol.class, WorkerManagerProtocol.versionID,
          resolveWorkerAddress(status.getRpcServer()), this.conf);
      if (null == wc) {
        LOG.warn("Fail to create Worker client at host " +
           status.getWorkerManagerName());
        return false;
      }
      if (grayWorkerManagers.containsKey(status)) {
        grayWorkerManagers.remove(status);
        whiteWorkerManagers.put(status, wc);
      } else if (!whiteWorkerManagers.containsKey(status)) {
        LOG.info(status.getWorkerManagerName() +
           " is registered to the cluster " +
           "and the maxClusterStaffs changes from " + this.maxClusterStaffs +
           " to " + (this.maxClusterStaffs + status.getMaxStaffsCount()));
        maxClusterStaffs += status.getMaxStaffsCount();
        whiteWorkerManagers.put(status, wc);
      }
    } catch (UnsupportedOperationException u) {
      e = u;
    } catch (ClassCastException c) {
      e = c;
    } catch (NullPointerException n) {
      e = n;
    } catch (IllegalArgumentException i) {
      e = i;
    } catch (Exception ex) {
      e = ex;
    }
    if (e != null) {
      LOG.error(
          "Fail to register WorkerManager " + status.getWorkerManagerName(), e);
      return false;
    }
    return true;
  }

  /**
   * Resolve worker address.
   * @param data
   *        worker address information.
   * @return
   *        Internet socket address.
   */
  private static InetSocketAddress resolveWorkerAddress(String data) {
    return new InetSocketAddress(data.split(":")[0], Integer.parseInt(data
        .split(":")[1]));
  }

  /**
   * Update white worker status.
   * @param old
   *        Old workermanager status
   * @param newKey
   *        New workermanager status.
   */
  public void updateWhiteWorkerManagersKey(WorkerManagerStatus old,
      WorkerManagerStatus newKey) {
    synchronized (whiteWorkerManagers) {
      long time = System.currentTimeMillis();
      newKey.setLastSeen(time);
      WorkerManagerProtocol worker = whiteWorkerManagers.remove(old);
      whiteWorkerManagers.put(newKey, worker);
    }
  }
/**
 * Update gray workerManager status.
 * @param old
 *        Old gray workerManager status
 * @param newKey
 *        New gray workerManager status.
 */
  public void updateGrayWorkerManagersKey(WorkerManagerStatus old,
      WorkerManagerStatus newKey) {
    synchronized (grayWorkerManagers) {
      long time = System.currentTimeMillis();
      newKey.setLastSeen(time);
      newKey.setPauseTime(old.getPauseTime());
      WorkerManagerProtocol worker = grayWorkerManagers.remove(old);
      grayWorkerManagers.put(newKey, worker);
    }
  }
/**
 * Remove job from jooListener list.
 * @param jobId
 *        job needed to be removed.
 * @return
 *        if remove the job successfully.
 */
  public synchronized boolean removeFromJobListener(BSPJobID jobId) {
    JobInProgress jip = whichJob(jobId);
    try {
      for (JobInProgressListener listener : jobInProgressListeners) {
        ArrayList<BSPJobID> removeJob = listener.jobRemoved(jip);
        for (BSPJobID removeID : removeJob) {
          if (this.jobs.containsKey(removeID)) {
            this.jobs.remove(removeID);
          }
        }
      }
      return true;
    } catch (Exception e) {
      LOG.error("Fail to alter scheduler a job is moved.", e);
      return false;
    }
  }
/**
 * record fault staff status to be recovery.
 * @param ts
 *        staff status.
 * @param isRecovery
 *        if the staff needs to be changed.
 */
  private void recordStaffFault(StaffStatus ts, boolean isRecovery) {
    Fault fault = ts.getFault();
    fault.setFaultStatus(isRecovery);
    mfl.faultLog(fault);
  }
/**
 * record woeker fault.
 * @param wms
 *        the workerManager status need to be changed.
 * @param isRecovery
 *        if the worker need to be recovery.
 */
  private void recordWorkerFault(WorkerManagerStatus wms, boolean isRecovery) {
    LOG.info("enter [Fault process]");
    if (wms.getWorkerFaultList().size() == 0) {
      Fault fault = new Fault(Fault.Type.NETWORK, Fault.Level.MAJOR,
          wms.getWorkerManagerName(), null, null, null);
      fault.setFaultStatus(isRecovery);
      mfl.faultLog(fault);
    } else {
      for (Fault fault : wms.getWorkerFaultList()) {
        fault.setFaultStatus(isRecovery);
        mfl.faultLog(fault);
      }
    }
  }

  @Override
  public void recordFault(Fault f) {
    mfl.faultLog(f);
  }
/**
 * get the staff last launch workerManager name.
 * @param map
 *        map of workerManager
 * @return
 *        the last launch workerManager name.
 */
  private String getTheLastWMName(Map<String, Integer> map) {
    String lastLWName = null;
    int lastLaunchWorker = 0;
    int i = 0;
    // add for test
    if (map == null) {
      LOG.info("map is null");
    }
    Set<String> keySet = map.keySet();
    Iterator<String> it = keySet.iterator();
    while (it.hasNext()) {
      lastLaunchWorker++;
      it.next();
    }
    Iterator<String> iter = keySet.iterator();
    while (iter.hasNext()) {
      i++;
      String key = iter.next();
      if (i == lastLaunchWorker) {
        lastLWName = key;
      }
    }
    LOG.info("getTheLastWMName(Map<String, Integer> map:)" + " " + lastLWName);
    return lastLWName;
  }
/**
 * If the fault can not be process, then return false, otherwise, return true;
 * @param ss
 *        staff status.
 * @param wms
 *        workerManager status.
 * @return
 *        whether the fault can be process.
 */
  public boolean staffFaultPreProcess(StaffStatus ss, WorkerManagerStatus wms) {
    String workerManagerName = wms.getWorkerManagerName();
    StaffInProgress sip = null;
    JobInProgress jip = whichJob(ss.getJobId());
    sip = jip.findStaffInProgress(ss.getStaffId().getStaffID());
    if (jip.getStatus().getRunState() != JobStatus.RECOVERY) {
      jip.getStatus().setRunState(JobStatus.RECOVERY);
    }
    LOG.info("before remove from listener!#########");
    if (faultMap.get(jip.getJobID()).size() == 1  &&
      !jip.isRemovedFromListener()) {
      removeFromJobListener(ss.getJobId());
      jip.setRemovedFromListener();
    }
    LOG.info("after remove from listener!#########");
    jip.setAttemptRecoveryCounter();
    LOG.info("The recovery number is " + jip.getNumAttemptRecovery());
    sip.getStaffs().remove(sip.getStaffID());
    ss.setRecovery(true);
    sip.getStaffStatus(ss.getStaffId()).setRunState(
        StaffStatus.State.STAFF_RECOVERY);
    if (jip.getNumAttemptRecovery() > jip.getMaxAttemptRecoveryCounter()) {
//      try {
//      } catch (Exception e) {
//        LOG.error("[recordStaffFault]", e);
//      }
      LOG.error("The recovery number is " + jip.getNumAttemptRecovery() +
         " and it's up to the Max Recovery Threshold " +
         jip.getMaxAttemptRecoveryCounter());
      return false;
    } else {
      if (!jip.isCleanedWMNs()) {
        jip.cleanWMNames();
        jip.setCleanedWMNs();
      }
      if (!jip.getWMNames().contains(workerManagerName)) {
        jip.addWMNames(workerManagerName);
        LOG.info(jip.getWMNames());
      }
      Map<String, Integer> workerManagerToTimes = jip.getStaffToWMTimes().get(
          ss.getStaffId());
      // add for test
      LOG.info("workerManagerToTimes =" + workerManagerToTimes +
         "ss.getStaffId()=" +
          ss.getStaffId());
      String lastWMName = getTheLastWMName(workerManagerToTimes);
      if (workerManagerToTimes.get(lastWMName) > jip
          .getMaxStaffAttemptRecoveryCounter()) {
        boolean success = jip.removeStaffFromWorker(workerManagerName,
            ss.getStaffId());
        if (!success) {
          LOG.error("[staffFaultPreProcess]: Fail to remove staff " +
             ss.getStaffId() + " from worker " + workerManagerName);
        }
      }
      boolean gray = jip.addFailedWorker(wms);
      if (gray && whiteWorkerManagers.containsKey(wms)) {
        WorkerManagerProtocol wmp = whiteWorkerManagers.get(wms);
        wmp.addFailedJob(jip.getJobID());
        if (wmp.getFailedJobCounter() > getMaxFailedJobOnWorker()) {
          removeWorkerFromWhite(wms);
          wms.setPauseTime(System.currentTimeMillis());
          addWorkerToGray(wms, wmp);
          LOG.info(wms.getWorkerManagerName() +
             " will be transferred from [WhiteList] to [GrayList]");
        }
      }
      if (faultMap.get(jip.getJobID()).size() == 1) {
        jip.setPriority(Constants.PRIORITY.HIGHER);
        for (JobInProgressListener listener : jobInProgressListeners) {
          try {
            listener.jobAdded(jip);
            jip.setUnCleanedWMNs();
            jip.setUnRemovedFromListener();
            LOG.warn("listener.jobAdded(jip);");
          } catch (IOException ioe) {
            //LOG.error("Fail to alter Scheduler a job is added.", ioe);
            throw new RuntimeException("Fail to alter Scheduler" +
               "a job is added.", ioe);
          }
        }
      }
      boolean isSuccessProcess = true;
      recordStaffFault(ss, isSuccessProcess);
      return true;
    }
  }
/**
 * remove workerManager status from white or gray list.
 * @param wms
 *        the workerManager need to be removed.
 */
  private void removeWorker(WorkerManagerStatus wms) {
    if (whiteWorkerManagers.containsKey(wms)) {
      whiteWorkerManagers.remove(wms);
      LOG.error(wms.getWorkerManagerName() +
         " is removed from [WhiteList] because HeartBeatTimeOut " +
         "and the maxClusterStaffs changes from " + maxClusterStaffs +
         " to " +
         (maxClusterStaffs - wms.getMaxStaffsCount()));
      maxClusterStaffs = maxClusterStaffs - wms.getMaxStaffsCount();
    } else if (grayWorkerManagers.containsKey(wms)) {
      grayWorkerManagers.remove(wms);
      LOG.error(wms.getWorkerManagerName() +
         " is removed from [GrayList] because HeartBeatTimeOut " +
         "and the maxClusterStaffs changes from " + maxClusterStaffs +
         " to " +
         (maxClusterStaffs - wms.getMaxStaffsCount()));
      maxClusterStaffs = maxClusterStaffs - wms.getMaxStaffsCount();
    }
    boolean isSuccessProcess = true;
    recordWorkerFault(wms, isSuccessProcess);
  }

  /**
   * fault preprocess for worker exception.
   * @param wms
   *        workerManager status need to be changed.
   */
  public void workerFaultPreProcess(WorkerManagerStatus wms) {
    List<StaffStatus> staffStatus;
    Set<BSPJobID> bspJobIDs = new HashSet<BSPJobID>();
    staffStatus = wms.getStaffReports();
    LOG.info("staffStatus = wms.getStaffReports() size: ;" +
      staffStatus.size());
    // update the failed worker's staffs' status
    for (StaffStatus ss : staffStatus) {
      LOG.info("enter whichJob########");
      JobInProgress jip = null;
      int count = 0;
      while (jip == null && count < 3) {
        jip = whichJob(ss.getJobId());
        count++;
      }
      if (jip == null) {
        LOG.info("jip is null######");
      }
      LOG.info("workerFaultPreProcess--jip =" +
         "whichJob(ss.getJobId()--jip.getJobID()) " + jip.getJobID());
      StaffInProgress sip = jip.findStaffInProgress(ss.getStaffId()
          .getStaffID());
      ss.setRecovery(true);
      sip.getStaffStatus(ss.getStaffId()).setRunState(
          StaffStatus.State.WORKER_RECOVERY);
      if (!bspJobIDs.contains(ss.getJobId())) {
        bspJobIDs.add(ss.getJobId());
      }
      LOG.info("bspJobIDs.add(ss.getJobId()); size: ;" + bspJobIDs.size());
    }
    for (BSPJobID bspJobId : bspJobIDs) {
      JobInProgress jip = whichJob(bspJobId);
      // AttemptRecoveryCounter++
      if (jip.getStatus().getRunState() != JobStatus.RECOVERY) {
        jip.getStatus().setRunState(JobStatus.RECOVERY);
        jip.setAttemptRecoveryCounter();
      }
      if (faultMap.get(jip.getJobID()).size() == 1 &&
         !jip.isRemovedFromListener()) {
        removeFromJobListener(jip.getJobID());
        jip.setRemovedFromListener();
      }
      StaffInProgress[] staffs = jip.getStaffInProgress();
      for (StaffInProgress sip : staffs) {
        if (sip.getStaffStatus(sip.getStaffID()).getRunState() ==
            StaffStatus.State.WORKER_RECOVERY &&
           sip.getWorkerManagerStatus().equals(wms)) {
          // remove from active staffs
          sip.getStaffs().remove(sip.getStaffID());
          boolean success = jip.removeStaffFromWorker(
              wms.getWorkerManagerName(), sip.getStaffID());
          if (!success) {
            LOG.error("[staffFaultPreProcess]: Fail to remove staff " +
               sip.getStaffID() + " from worker " +
               wms.getWorkerManagerName());
          }
        }
      }
      for (StaffStatus ss : staffStatus) {
        faultMap.get(bspJobId).remove(ss);
      }
      if (jip.getNumAttemptRecovery() > jip.getMaxAttemptRecoveryCounter()) {
        boolean isSuccessProcess = false;
        try {
          recordWorkerFault(wms, isSuccessProcess);
        } catch (Exception e) {
          //LOG.error("[recordWorkerFault]", e);
          throw new RuntimeException("[recordWorkerFault]", e);
        }
        LOG.error("The recovery number is " + jip.getNumAttemptRecovery() +
           " and it's up to the Max Recovery Threshold " +
           jip.getMaxAttemptRecoveryCounter());
        jip.killJob();
      } else {
        if (!jip.isCleanedWMNs()) {
          jip.cleanWMNames();
          jip.setCleanedWMNs();
        }
        // jip.addWMNames(wms.getWorkerManagerName());
        if (!jip.getWMNames().contains(wms.getWorkerManagerName())) {
          jip.addWMNames(wms.getWorkerManagerName());
          LOG.info(jip.getWMNames());
        }
        if (faultMap.get(bspJobId).isEmpty()) {
          faultMap.remove(bspJobId);
          jip.setPriority(Constants.PRIORITY.HIGHER);
          for (JobInProgressListener listener : jobInProgressListeners) {
            try {
              listener.jobAdded(jip);
              jip.setUnCleanedWMNs();
              jip.setUnRemovedFromListener();
              LOG.warn("the recoveried job is added to the wait queue ---" +
                 "[listener.jobAdded(jip)]");
            } catch (IOException ioe) {
              //LOG.error("Fail to alter Scheduler a job is added.", ioe);
              throw new RuntimeException("Fail to alter Scheduler" +
                 "a job is added.", ioe);
            }
          }
        }
        LOG.info("enter [Fault process]");
      }
    }
  }

  /**
   * recovery for the controller's fault before local compute
   * @param jobId
   *        BSPjob's id.
   * @return
   *        if the job is recovery successfully.
   */
  @Override
  public boolean recovery(BSPJobID jobId) {
    LOG.error("so bad ! The job is failed before dispatch to the workers !" +
       "please submit it again or quit computing!");
    if (-1 == jobId.getId()) {
      LOG.info(jobId.getJtIdentifier());
    } else {
      try {
        this.killJob(jobId);
      } catch (IOException e) {
        LOG.error("recovery", e);
        return false;
      }
    }
    return true;
  }

  /**
   * exceptions reported by workers before local compute or after local compute.
   * @param ss
   *        staff status need to be recovery
   * @return
   *        if the staff has been recovery.
   */
  public boolean recovery(StaffStatus ss) {
    if (ss.getStage() == 2) {
      LOG.error("so bad ! Though trying several times to recovery the staff: " +
         ss.getStaffId() + " , the job is failed after local computing !");
      boolean isSuccessProcess = false;
      recordStaffFault(ss, isSuccessProcess);
      try {
        this.killJob(ss.getJobId());
      } catch (IOException e) {
        LOG.error("recovery", e);
      }
      return true;
    } else if (ss.getStage() == 0) {
      LOG.error("so bad ! The job is failed before local computing !" +
         "please submit it again !");
      boolean isSuccessProcess = false;
      recordStaffFault(ss, isSuccessProcess);
      try {
        this.killJob(ss.getJobId());
      } catch (IOException e) {
        LOG.error("recovery", e);
      }
      return true;
    } else {
      LOG.error("error: no such stage !");
      return false;
    }
  }

  /**
   * Check there is a fault or not.
   * @param wms
   *        workerManager need to be checked.
   */
  public void checkFault(WorkerManagerStatus wms) {
    try {
      // Note: Now the list of workerFault reported by WorkerManager is
      // always 0.
      if (wms.getWorkerFaultList().size() != 0) {
        // some add operation of WorkerFaultList should exist in
        // WorkerManager's IO catch statement
        workerFaultPreProcess(wms);
      } else {
        List<StaffStatus> slist = wms.getStaffReports();
        for (StaffStatus ss : slist) {
          JobInProgress jip = whichJob(ss.getJobId());
          if (jip == null) {
            continue;
          }
          switch (ss.getRunState()) {
          case RUNNING:
            // add by cuiliping update staffStatus
            jip.updateStaffs(ss);
            break;
          case FAULT:
            LOG.error("[Fault Detective] The fault of " + ss.getStaffId() +
               " has been catched");
            LOG.info("ProcessFault is " + this.pf.isAlive());
            if (!this.pf.isAlive()) {
              this.pf.start();
            }
            if (ss.getRunState() == StaffStatus.State.STAFF_RECOVERY) {
              break;
            }
            synchronized (faultMap) {
            if (!faultMap.containsKey(ss.getJobId())) {
              Map<StaffStatus, WorkerManagerStatus> ssToWMS =
                  new ConcurrentHashMap<StaffStatus, WorkerManagerStatus>();
              ssToWMS.put(ss, wms);
              faultMap.put(ss.getJobId(), ssToWMS);
            } else {
              faultMap.get(ss.getJobId()).put(ss, wms);
            }
          }
            break;
          default:
            break;
          }
        }
      }
    } catch (Exception e) {
      //LOG.error("[checkFault]", e);
      throw new RuntimeException("[checkFault]", e);
    }
  }

  @Override
  public boolean report(Directive directive) throws IOException {
    // check the type of directive is Response or not
    if (directive.getType().value() != Directive.Type.Response.value()) {
      throw new IllegalStateException("WorkerManager should report()" +
         " with Response. But the Current report type is:" +
         directive.getType());
    }
    // process the heart-beat information.
    WorkerManagerStatus status = directive.getStatus();
    // add by cui
    List<StaffStatus> stafflistList = status.getStaffReports();
    for (StaffStatus e : stafflistList) {
      StaffAttemptID attemptId = e.getStaffId();
      staffs.put(attemptId, e);
    }
    if (whiteWorkerManagers.containsKey(status) ||
       grayWorkerManagers.containsKey(status)) {
      WorkerManagerStatus ustus = null;
      for (WorkerManagerStatus old : whiteWorkerManagers.keySet()) {
        if (old.equals(status)) {
          ustus = status;
          updateWhiteWorkerManagersKey(old, ustus);
          // LOG.info("after  updateWhiteWorkerManagersKey(old, ustus);");
          break;
        }
      }
      for (WorkerManagerStatus old : grayWorkerManagers.keySet()) {
        if (old.equals(status)) {
          ustus = status;
          updateGrayWorkerManagersKey(old, ustus);
          // LOG.info("after  updateGrayWorkerManagersKey(old, ustus);");
          break;
        }
      }
      if (role.equals(BspControllerRole.ACTIVE)) {
        checkFault(ustus);
      }
    } else {
      throw new RuntimeException("WorkerManager not found." +
         status.getWorkerManagerName());
    }
    return true;
  }

  /* Baoxing Yang added */
  /**
   * Find job in processingQueue or waitQueue.
   * @param id
   *        jobID need to be found
   * @return
   *        if find the job.
   */
  private JobInProgress whichJob(BSPJobID id) {
    for (JobInProgress job : staffScheduler
        .getJobs(SimpleStaffScheduler.PROCESSING_QUEUE)) {
      if (job.getJobID().equals(id)) {
        return job;
      }
    }
    for (JobInProgress job : staffScheduler
        .getJobs(SimpleStaffScheduler.HIGHER_WAIT_QUEUE)) {
      if (job.getJobID().equals(id)) {
        return job;
      }
    }
    return null;
  }

  // /////////////////////////////////////////////////////////////
  // BSPController methods
  // /////////////////////////////////////////////////////////////
  // Get the job directory in system directory
  /**
   * Get the job directory in system directory.
   * @param id
   *        the jobId that need to be found.
   * @return
   *        path of the job.
   */
  public Path getSystemDirectoryForJob(BSPJobID id) {
    return new Path(getSystemDir(), id.toString());
  }
/**
 * get BCP local file Dir.
 * @return
 *        BSP tmp Dir.
 * @throws IOException
 *         exceptions during find the dir.
 */
  String[] getLocalDirs() throws IOException {
    return conf.getStrings("Constants.BC_BSP_TMP_DIRECTORY");
  }
/**
 * delete local files.
 * @throws IOException
 *         exceptions during delete local files.
 */
  void deleteLocalFiles() throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      // FileSystem.getLocal(conf).delete(new Path(localDirs[i]), true);
      new BSPLocalFileSystemImpl(conf).delete(
          new BSPHdfsImpl().newPath(localDirs[i]), true);
    }
  }
/**
 * delete local files and its subdir
 * @param subdir
 *        subdirectories of local file.
 * @throws IOException
 *         exceptions during delete local file.
 */
  void deleteLocalFiles(String subdir) throws IOException {
    try {
      String[] localDirs = getLocalDirs();
      for (int i = 0; i < localDirs.length; i++) {
        // FileSystem.getLocal(conf).delete(
        // new Path(localDirs[i], subdir), true);
        new BSPLocalFileSystemImpl(conf).delete(
            new BSPHdfsImpl().newpath(localDirs[i], subdir), true);
      }
    } catch (NullPointerException e) {
      //throw new RuntimeException("[deleteLocalFiles] ", e);
      LOG.info(e);
    }
  }

  /**
   * Constructs a local file name. Files are distributed among configured local
   * directories.
   * @param pathString
   *        local path of local dir
   * @return
   *        get the default local path if the set pathstring is null.
   * @throws IOException
   *         exception during get the local path.
   */
  public Path getLocalPath(String pathString) throws IOException {
    return conf.getLocalPath(Constants.BC_BSP_LOCAL_DIRECTORY, pathString);
  }
/**
 * start master process with BSP configuration.
 * @param conf
 *        BSP configuration that has been set.
 * @return
 *        startmaster result.
 * @throws IOException
 *         exceptions during start the master.
 * @throws InterruptedException
 *         interruptedExceptions during start thread.
 */
  public static BSPController startMaster(BSPConfiguration conf)
      throws IOException, InterruptedException {
    return startMaster(conf, generateNewIdentifier());
  }
/**
 * startMaster thread
 * @param conf
 *        BSP conf
 * @param identifier
 *        identifier of BSP controller.
 * @return
 *        result of start master.
 * @throws IOException
 *         exceptions during start the master.
 * @throws InterruptedException
 *         interruptedExceptions during start thread.
 */
  public static BSPController startMaster(BSPConfiguration conf,
      String identifier) throws IOException, InterruptedException {
    BSPController result = new BSPController(conf, identifier);
    result.staffScheduler.setWorkerManagerControlInterface(result);
    // LOG.info("before result.staffScheduler.start(); and the role is " +
    // result.role);
    if (result.role.equals(BspControllerRole.ACTIVE)) {
      result.staffScheduler.start();
    }
    return result;
  }
/**
 * get BSPcontroller socket address.
 * @param conf
 *        BSP conf to get the address.
 * @return
 *        socket address of controller.
 */
  public static InetSocketAddress getAddress(Configuration conf) {
    String bspControllerStr = conf.get(Constants.BC_BSP_CONTROLLER_ADDRESS);
    return NetUtils.createSocketAddr(bspControllerStr);
  }

  /**
   * BSPController identifier
   * @return String BSPController identification number
   */
  private static String generateNewIdentifier() {
    return new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
  }
/**
 * get the controller active to offer service.
 * @throws InterruptedException
 *         interruptedExceptions during start service thread.
 * @throws IOException
 *         exceptions during start service.
 */
  public void offerService() throws InterruptedException, IOException {
    if (this.role.equals(BspControllerRole.ACTIVE)) {
      this.cto.start();
      // Baoxing Yang added
      this.pf.start();
    }
    this.controllerServer.start();
    synchronized (this) {
      state = State.RUNNING;
    }
    LOG.info("Starting RUNNING");
    this.controllerServer.join();
    LOG.info("Stopped RPC Master server.");
  }

  // //////////////////////////////////////////////////
  // InterServerProtocol
  // //////////////////////////////////////////////////
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(ControllerProtocol.class.getName())) {
      return ControllerProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())) {
      return JobSubmissionProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to BSPController: " + protocol);
    }
  }

  // //////////////////////////////////////////////////
  // JobSubmissionProtocol
  // //////////////////////////////////////////////////
  /**
   * This method returns new job id. The returned job id increases sequentially.
   * @return
   * new job id.
   */
  @Override
  public BSPJobID getNewJobId() throws IOException {
    int id;
    synchronized (nextJobId) {
      id = nextJobId;
      nextJobId = Integer.valueOf(id + 1);
    }
    return new BSPJobID(this.controllerIdentifier, id);
  }

  @Override
  public JobStatus submitJob(BSPJobID jobID, String jobFile) {
    if (jobs.containsKey(jobID)) {
      // job already running, don't start twice
      LOG.warn("The job (" + jobID + ") was already submitted");
      return jobs.get(jobID).getStatus();
    }
    JobInProgress jip = null;
    try {
      // submit log format like jobid$jobSubmitDir
      if (this.role.equals(BspControllerRole.ACTIVE)) {
        this.haLogOperator.writeFile(jobID.toString() + "&" + jobFile,
            conf.get(Constants.BC_BSP_HA_LOG_DIR) +
               Constants.BC_BSP_HA_SUBMIT_LOG);
      }
      jip = new JobInProgress(jobID, new Path(jobFile), this, this.conf);
      jip.getGssc().setJobInProgressControlInterface(jip);
      return addJob(jobID, jip);
    } catch (IOException e) {
      LOG.error("Exception has been catched in BSPController--submitJob !", e);
      Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, jobID.toString(),
          e.toString());
      recordFault(f);
      recovery(jobID);
      killJob(jip);
      return null;
    }
  }

  // //////////////////////////////////////////////////
  // WorkerManagerControlInterface functions
  // //////////////////////////////////////////////////
  @SuppressWarnings("static-access")
  @Override
  public ClusterStatus getClusterStatus(boolean detailed) {
    int workerManagerCount = this.whiteWorkerManagers.size();
    String[] workerManagersName = new String[workerManagerCount];
    this.runningClusterStaffs = 0;
    int count = 0;
    for (Map.Entry<WorkerManagerStatus, WorkerManagerProtocol> entry :
      this.whiteWorkerManagers
        .entrySet()) {
      WorkerManagerStatus wms = entry.getKey();
      workerManagersName[count++] = wms.getWorkerManagerName();
      this.runningClusterStaffs += wms.getRunningStaffsCount();
    }
    if (detailed) {
      return new ClusterStatus(workerManagersName, this.maxClusterStaffs,
          this.runningClusterStaffs, this.state);
    } else {
      return new ClusterStatus(workerManagerCount, this.maxClusterStaffs,
          this.runningClusterStaffs, this.state);
    }
  }

  @Override
  public StaffAttemptID[] getStaffStatus(BSPJobID jobId) throws IOException {
    JobInProgress job = jobs.get(jobId);
    StaffAttemptID[] attemptID = job.getAttemptIDList();
    return attemptID;
  }

  @Override
  public StaffStatus[] getStaffDetail(BSPJobID jobId) {
    JobInProgress jip = jobs.get(jobId);
    StaffAttemptID[] attemptID = jip.getAttemptIDList();
    StaffInProgress[] sip = jip.getStaffInProgress();
    List<StaffStatus> staffStatusList = new ArrayList<StaffStatus>();
    for (int i = 0; i < sip.length; i++) {
      staffStatusList.add(sip[i].getStaffStatus(attemptID[i]));
    }
    LOG.info("sizesize:" + staffStatusList.size());
    return staffStatusList.toArray(new StaffStatus[staffStatusList.size()]);
  }

  @Override
  public void setCheckFrequency(BSPJobID jobID, int cf) {
    JobInProgress jip = jobs.get(jobID);
    jip.setCheckPointFrequency(cf);
  }

  @Override
  public void setCheckFrequencyNext(BSPJobID jobId) {
    JobInProgress jip = jobs.get(jobId);
    jip.setCheckPointNext();
  }

  @Override
  public WorkerManagerProtocol findWorkerManager(WorkerManagerStatus status) {
    return whiteWorkerManagers.get(status);
  }

  @Override
  public Collection<WorkerManagerProtocol> findWorkerManagers() {
    return whiteWorkerManagers.values();
  }

  @Override
  public Collection<WorkerManagerStatus> workerServerStatusKeySet() {
    return whiteWorkerManagers.keySet();
  }

  @Override
  public void addJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.add(listener);
  }

  @Override
  public void removeJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.remove(listener);
  }

  @SuppressWarnings("static-access")
  @Override
  public String[] getActiveWorkerManagersName() {
    int workerManagerCount = this.whiteWorkerManagers.size();
    workerManagerCount += this.grayWorkerManagers.size();
    String[] workerManagersName = new String[workerManagerCount];
    int count = 0;
    for (Map.Entry<WorkerManagerStatus, WorkerManagerProtocol> entry :
      this.whiteWorkerManagers
        .entrySet()) {
      WorkerManagerStatus wms = entry.getKey();
      workerManagersName[count++] = wms.getWorkerManagerName();
    }
    for (Map.Entry<WorkerManagerStatus, WorkerManagerProtocol> entry :
      this.grayWorkerManagers
        .entrySet()) {
      WorkerManagerStatus wms = entry.getKey();
      workerManagersName[count++] = wms.getWorkerManagerName();
    }
    return workerManagersName;
  }

  /**
   * Adds a job to the bsp master. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * @param jobId
   *        The id for the job submitted which needs to be added
   * @param jip
   *        jobInprogress that need to be added
   * @return job status that has been added.
   */
  private synchronized JobStatus addJob(BSPJobID jobId, JobInProgress jip) {
    totalSubmissions++;
    synchronized (jobs) {
      jobs.put(jip.getProfile().getJobID(), jip);
      for (JobInProgressListener listener : jobInProgressListeners) {
        try {
          listener.jobAdded(jip);
        } catch (IOException ioe) {
          LOG.error("Fail to alter Scheduler a job is added.", ioe);
          Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, jobId.toString(),
              ioe.toString());
          recordFault(f);
          recovery(jobId);
          this.killJob(jip);
        }
      }
    }
    return jip.getStatus();
  }

  @Override
  public JobStatus[] jobsToComplete() throws IOException {
    return getJobStatus(jobs.values(), true);
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException {
    return getJobStatus(jobs.values(), false);
  }
/**
 * get the jobstatus form jip collection.
 * @param jips
 *        jips status that need to be get.
 * @param toComplete
 *        if the job is to be completed
 * @return
 *        jobstatus list get from jips.
 */
  private synchronized JobStatus[] getJobStatus(Collection<JobInProgress> jips,
      boolean toComplete) {
    if (jips == null) {
      return new JobStatus[] {};
    }
    List<JobStatus> jobStatusList = new ArrayList<JobStatus>();
    for (JobInProgress jip : jips) {
      JobStatus status = jip.getStatus();
      status.setStartTime(jip.getStartTime());
      // Sets the user name
      status.setUsername(jip.getProfile().getUser());
      if (toComplete) {
        if (status.getRunState() == JobStatus.RUNNING ||
           status.getRunState() == JobStatus.PREP) {
          jobStatusList.add(status);
        }
      } else {
        jobStatusList.add(status);
      }
    }
    return jobStatusList.toArray(new JobStatus[jobStatusList.size()]);
  }

  @Override
  public synchronized String getFilesystemName() throws IOException {
    if (bspfs == null) {
      throw new IllegalStateException("FileSystem object not available yet");
    }
    return bspfs.getUri().toString();
  }

  /**
   * Return system directory to which BSP store control files.
   * @return
   *        BSP system file path.
   */
  @Override
  public String getSystemDir() {
    // Path sysDir = new Path(conf.get(Constants.BC_BSP_SHARE_DIRECTORY));
    // return fs.makeQualified(sysDir).toString();
    return bspfs.makeQualified(
        new BSPHdfsImpl().newPath(conf.get(Constants.BC_BSP_SHARE_DIRECTORY)))
        .toString();
  }

  @Override
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
    synchronized (this) {
      JobInProgress jip = jobs.get(jobid);
      if (jip != null) {
        return jip.getProfile();
      }
    }
    return null;
  }

  @Override
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
    if (this.role.equals(BspControllerRole.ACTIVE)) {
      synchronized (this) {
        JobInProgress jip = jobs.get(jobid);
        if (jip != null) {
          return jip.getStatus();
        }
      }
    }
    return null;
  }

  // add by chen
  @Override
  public Counters getCounters(BSPJobID jobid) {
    synchronized (this) {
      JobInProgress jip = jobs.get(jobid);
      if (jip != null) {
        return jip.getCounters();
      }
    }
    return null;
  }

  @Override
  public void killJob(BSPJobID jobid) throws IOException {
    JobInProgress job = jobs.get(jobid);
    if (null == job) {
      LOG.warn("killJob(): JobId " + jobid.toString() + " is not a valid job");
      return;
    }
    killJob(job);
  }
/**
 * kill job staff and the job.
 * @param jip
 *        jobInprogress that need to be killed.
 */
  private synchronized void killJob(JobInProgress jip) {
    LOG.info("Killing job " + jip.getJobID());
    StaffInProgress[] sips = jip.getStaffInProgress();
    for (int index = 0; index < sips.length; index++) {
      WorkerManagerStatus wms = sips[index].getWorkerManagerStatus();
      WorkerManagerProtocol wmp = findWorkerManager(wms);
      ArrayList<WorkerManagerAction> actionList =
          new ArrayList<WorkerManagerAction>();
      actionList.add(new KillStaffAction(sips[index].getStaffID()));
      Directive directive = new Directive(getActiveWorkerManagersName(),
          actionList);
      try {
        wmp.dispatch(jip.getJobID(), directive, false, false, 0);
      } catch (Exception e) {
        throw new RuntimeException("Fail to kill staff " +
          sips[index].getStaffID() +
           " on the WorkerManager " + wms.getWorkerManagerName(), e);
      }
    }
    jip.killJob();
    LOG.warn(jip.getJobID() + " has been killed completely");
  }

  @Override
  public boolean killStaff(StaffAttemptID taskId, boolean shouldFail)
      throws IOException {
    return false;
  }
/**
 * BSPcontroller construction method initalize BSP master.
 * @param controllerClass
 *        extend from BSPcontroller.
 * @param conf
 *        BSP system configuration to initalize the controller.
 * @return
 *        BSP controller that has been constructed.
 */
  public static BSPController constructController(
      Class<? extends BSPController> controllerClass,
      final Configuration conf) {
    try {
      Constructor<? extends BSPController> c = controllerClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "Master: " +
         controllerClass.toString() +
         ((e.getCause() != null) ? e.getCause().getMessage() : ""), e);
    }
  }

  /**
   * BSPcontroller shutdown method.
   */
  @SuppressWarnings("deprecation")
  public void shutdown() {
    try {
      LOG.info("Prepare to shutdown the BSPController");
      for (JobInProgress jip : jobs.values()) {
        if (jip.getStatus().getRunState() == JobStatus.RUNNING) {
          jip.killJobRapid();
          LOG.warn(jip.getJobID() + " has been killed by system");
        }
      }
      this.staffScheduler.stop();
      LOG.info("Succeed to stop the Scheduler Server");
      this.controllerServer.stop();
      LOG.info("Succeed to stop RPC Server on BSPController");
      this.cto.stop();
      // Baoxing Yang added
      this.pf.stop();
      cleanupLocalSystem();
      LOG.info("Succeed to cleanup temporary files on the local disk");
      // this.zk.close();
      this.bspzk.close();
    } catch (Exception e) {
      throw new RuntimeException("Fail to shutdown the BSPController", e);
    }
  }
  /**
   * clean BSP system file.
   */
  public void cleanupLocalSystem() {
    BSPConfiguration conf = new BSPConfiguration();
    File f = new File(conf.get(Constants.BC_BSP_LOCAL_DIRECTORY));
    try {
      deleteLocalDir(f);
    } catch (Exception e) {
      //LOG.error("Failed to delete the local system : " + f, e);
      throw new RuntimeException("Failed to delete the local system : " + f, e);
    }
  }

/**
 * delete BSP local file Dir and sub dir.
 * @param dir
 *        BSP system dir that need to be deleted.
 */
  public void deleteLocalDir(File dir) {
    if (dir == null || !dir.exists() || !dir.isDirectory()) {
      return;
    }
    for (File file : dir.listFiles()) {
      if (file.isFile()) {
        file.delete();
      } else if (file.isDirectory()) {
        deleteLocalDir(file); // recursive delete the subdir
      }
    }
    dir.delete();
  }

  /**
   * Return controller current state.
   * @return
   *        controller current state.
   */
  public BSPController.State currentState() {
    return this.state;
  }

/**
 * BSPcontroller state become ACTIVE.
 */
  public void becomeActive() {
    this.role = BspControllerRole.ACTIVE;
  }

  /**
   * BSPcontroller state become standby.
   */
  public void becomeStandby() {
    this.role = BspControllerRole.STANDBY;
  }

  /**
   * BSPcontroller become neutral.
   */
  public void becomeNeutral() {
    this.role = BspControllerRole.NEUTRAL;
  }

  /**
   * get BSpcontroller role.
   */
  public BspControllerRole getRole() {
    return role;
  }

  /**
   * set the BSPcontroller role.
   * @param role
   *        return the BSPcontroller has been set.
   */
  public void setRole(BspControllerRole role) {
    this.role = role;
  }

  /**
   * determine the role of bpsController.
   */
  public void determineBspControllerRole() {
    try {
      // alter by gtt
      if (bspzk.getZk() != null) {
        // Stat s = null;
        // create the directory for election
        // s = zk.exists(Constants.BSPCONTROLLER_LEADER, true);
        String bspControllerAddr = conf
            .get(Constants.BC_BSP_CONTROLLER_ADDRESS);
        if (bspzk.equaltostat(Constants.BSPCONTROLLER_LEADER, true)) {
          bspzk.create(Constants.BSPCONTROLLER_LEADER,
              bspControllerAddr.getBytes(), BSPIds.OPEN_ACL_UNSAFE,
              new BSPCreateModeImpl().getEPHEMERAL());
          this.becomeActive();
          LOG.info("acitve address is " + bspControllerAddr);
        } else {
          LOG.info("standby address is " + bspControllerAddr);
          // Stat s1 = null;
          // s1 = zk.exists(Constants.BSPCONTROLLER_STANDBY_LEADER, false);
          if (bspzk.equaltostat(Constants.BSPCONTROLLER_STANDBY_LEADER,
              false)) {
            bspzk.create(Constants.BSPCONTROLLER_STANDBY_LEADER,
                bspControllerAddr.getBytes(), BSPIds.OPEN_ACL_UNSAFE,
                new BSPCreateModeImpl().getPERSISTENT());
          } else {
            bspzk.setData(Constants.BSPCONTROLLER_STANDBY_LEADER,
                bspControllerAddr.getBytes(),
                bspzk.exists(Constants.BSPCONTROLLER_STANDBY_LEADER, false)
                    .getVersion());
            // zk.delete(Constants.BSPCONTROLLER_STANDBY_LEADER,
            // s1.getAversion());
            // zk.create(Constants.BSPCONTROLLER_STANDBY_LEADER,
            // bspControllerAddr.getBytes(), Ids.OPEN_ACL_UNSAFE,
            // CreateMode.PERSISTENT);
          }
          // zk.create(Constants.BSPCONTROLLER_STANDBY_LEADER,
          // bspControllerAddr.getBytes(), Ids.OPEN_ACL_UNSAFE,
          // CreateMode.EPHEMERAL);
          this.becomeStandby();
        }
      } else {
        this.becomeNeutral();
      }
    } catch (BSPConnectionLossException e) {
      // if the Connection loss from ZooKeeper Server,should shutdown the
      // Controller to avoid have two active
      this.shutdown();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      throw new RuntimeException("Failed to determine" +
         "the BspController's role:" +
         e);
    }
  }

  /**
   * zookeeper process method,determine BSpcontroller role.
   */
  @Override
  public void process(WatchedEvent event) {
    // TODO Auto-generated method stub
    // long old = System.currentTimeMillis();
    if (event.getType().toString().equals("NodeDeleted")) {
      // LOG.info("in process before determineBspControllerRole()");
      determineBspControllerRole();
      if (this.role.equals(BspControllerRole.ACTIVE)) {
        this.becomeNeutral();
        LOG.info("the BspController's role is :" + this.role);
        this.mfl = new MonitorFaultLog();
        try {
          this.staffScheduler.setRole(role);
          this.staffScheduler.setWorkerManagerControlInterface(this);
          this.staffScheduler.start();
          // LOG.info("in process and after this.staffScheduler.start();");
          this.recoveryForHa();
          // this.becomeActive();
          // LOG.info("the BspController's role is :"+this.role);
          // this.staffScheduler.setRole(role);
          // this.staffScheduler.getQueueManager().setRole(role);
          // this.staffScheduler.jobProcessorStart();
          // this.cto.start();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          //LOG.error("in ZooKeeper process Method And " + e.getMessage());
          throw new RuntimeException("in ZooKeeper process Method And ", e);
        }
      }
    }
  }

  /**
   * Submit all of the jobs need to recovery and recovery id.
   * @param jobsForRecovery
   *        jobs ID for recovery.
   * @throws IOException
   *         exceptions happeded during submit jobs.
   */
  public void submitAllJob(Map<BSPJobID, String> jobsForRecovery)
      throws IOException {
    int count = 1;
    // FSDataInputStream in = this.haLogOperator.readFile(conf
    // .get(Constants.BC_BSP_HA_LOG_DIR)
    // + Constants.BC_BSP_HA_SUBMIT_LOG);
    BSPFSDataInputStream bspin = new BSPFSDataInputStreamImpl(haLogOperator,
        conf.get(Constants.BC_BSP_HA_LOG_DIR) + Constants.BC_BSP_HA_SUBMIT_LOG);
    while (bspin != null) {
      try {
        // LOG.info("in first while");
        String line = bspin.readUTF();
        // count++;
        String[] temp = line.split("&");
        count = Integer.parseInt(temp[0].substring(17));
        LOG.info("jobid=" + temp[0] + " uri=" + temp[1]);
        if (this.haLogOperator.isExist(temp[1])) {
          jobsForRecovery.put(new BSPJobID().forName(temp[0]), temp[1]);
          LOG.info(" this.submitJob() jobid=" + temp[0]);
          this.submitJob(new BSPJobID().forName(temp[0]), temp[1]);
        }
      } catch (EOFException e) {
        // in.close();
        bspin = null;
      }
    }
    // haLogOperator.closeFs();
    this.nextJobId = count + 1;
    LOG.info("the next jobid is" + this.nextJobId);
  }

  /**
   * get all the jobInprogress need to recovery
   * @param jobsForRecovery
   *        jobs that need to recovery
   * @throws IOException
   *         exceptions during handle the hdfs logs.
   */
  public void getAllJobInprogressForRecovery(
      Map<BSPJobID, String> jobsForRecovery) throws IOException {
    this.haLogOperator.createFile(conf.get(Constants.BC_BSP_HA_LOG_DIR) +
       Constants.BC_BSP_HA_SUBMIT_LOG);
    for (BSPJobID jobid : jobsForRecovery.keySet()) {
      // LOG.info("refresh the submitLog jobid=" + jobid );
      String jobFile = jobsForRecovery.get(jobid);
      this.haLogOperator.writeFile(jobid.toString() + "&" + jobFile,
          conf.get(Constants.BC_BSP_HA_LOG_DIR) +
             Constants.BC_BSP_HA_SUBMIT_LOG);
    }
    // get all the jobInprogress need to recovery
    // LOG.info("before read operateLOG");
    // FSDataInputStream in = this.haLogOperator.readFile(conf
    // .get(Constants.BC_BSP_HA_LOG_DIR)
    // + Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
    BSPFSDataInputStream bspin = new BSPFSDataInputStreamImpl(haLogOperator,
        conf.get(Constants.BC_BSP_HA_LOG_DIR) +
           Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
    while (bspin != null) {
      try {
        // LOG.info("in second while");
        String line = bspin.readUTF();
        String[] temp = line.split("&");
        BSPJobID jobid = new BSPJobID().forName(temp[0]);
        if (jobsForRecovery.containsKey(jobid)) {
          LOG.info("jobid is" + jobid.toString() + " and moved to queueName=" +
             temp[1]);
          jobsForRecovery.remove(jobid);
          jobsForRecovery.put(jobid, temp[1]);
        }
      } catch (EOFException e) {
        // in.close();
        bspin = null;
      }
    }
  }

  /**
   * recovery jobs queue.
   * @param queueManager
   *        recovery jobs queue's manager
   * @param jobsForRecovery
   *        jobs that need recovery
   * @throws IOException
   *         exceptions during HDFS creat file.
   */
  // recovery the queue
  public void recoveryQueue(QueueManager queueManager,
      Map<BSPJobID, String> jobsForRecovery) throws IOException {
    String queueName = "";
    String WAIT_QUEUE = "waitQueue";
    // LOG.info("before create operateLOG");
    this.haLogOperator.createFile(conf.get(Constants.BC_BSP_HA_LOG_DIR) +
       Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
    for (BSPJobID jobid : jobsForRecovery.keySet()) {
      queueName = jobsForRecovery.get(jobid);
      if (!"".equals(queueName)) {
        // LOG.info("before write operateLOG");
        this.haLogOperator.writeFile(jobid.toString() + "&" + queueName,
            conf.get(Constants.BC_BSP_HA_LOG_DIR) +
               Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
        LOG.info("queueManager " + queueManager);
        queueManager.moveJobHrn(queueName, jobs.get(jobid));
        // if (!WAIT_QUEUE.equals(queueName)) {
        // //
        // LOG.info("in   if(!WAIT_QUEUE.equals(queueName))
        //{ and queueName="+queueName);
        // queueManager
        // .moveJob(WAIT_QUEUE, queueName, jobs.get(jobid));
        // }
      }
    }
    this.staffScheduler.setQueueManager(queueManager);
    // update the
    // staffScheduler's
    // queueManager
  }

  /**
   * kill staffs of the job in schedule process whose staffs haven't
   * schedule over
   * @param queueManager
   *        queueManager that handle the jobs in the waitQueues
   * @throws IOException
   *         exceptions haappened during handle hdfs file.
   */
  public void killStaffInScheduleing(QueueManager queueManager)
      throws IOException {
    // String WAIT_QUEUE = "waitQueue";
    // Queue<JobInProgress> waitQueue = queueManager.findQueue(WAIT_QUEUE);
    Collection<JobInProgress> jobsInWaitQueue = queueManager.getJobs();
    if (this.haLogOperator.isExist(conf.get(Constants.BC_BSP_HA_LOG_DIR) +
       Constants.BC_BSP_HA_SCHEDULE_LOG)) {
      // FSDataInputStream in = this.haLogOperator.readFile(conf
      // .get(Constants.BC_BSP_HA_LOG_DIR)
      // + Constants.BC_BSP_HA_SCHEDULE_LOG);
      BSPFSDataInputStream bspin = new BSPFSDataInputStreamImpl(haLogOperator,
          conf.get(Constants.BC_BSP_HA_LOG_DIR) +
             Constants.BC_BSP_HA_SCHEDULE_LOG);
      if (bspin != null) {
        String jobid = bspin.readUTF();
        bspin = null;
        for (JobInProgress jip : jobsInWaitQueue) {
          if (jip.getJobID().equals(new BSPJobID().forName(jobid))) {
            ArrayList<WorkerManagerStatus> wmsl = new
                ArrayList<WorkerManagerStatus>();
            // in = this.haLogOperator.readFile(conf
            // .get(Constants.BC_BSP_HA_LOG_DIR)
            // + jip.getJobID().toString());
            BSPFSDataInputStream bspIn = new BSPFSDataInputStreamImpl(
                haLogOperator, conf.get(Constants.BC_BSP_HA_LOG_DIR) +
                   jip.getJobID().toString());
            Text loaFactor = new Text();
            loaFactor.readFields(bspIn.getIn());
            while (bspIn != null) {
              try {
                WorkerManagerStatus wmStatus = new WorkerManagerStatus();
                wmStatus.readFields(bspIn.getIn());
                wmsl.add(wmStatus);
              } catch (EOFException e) {
                bspIn = null;
              }
            }
            // recovery the jobInprogress state
            StaffInProgress[] staffs = jip.getStaffInProgress();
            for (int i = 0; i < staffs.length; i++) {
              if (!staffs[i].isRunning() && !staffs[i].isComplete()) {
                Staff t = jip.obtainNewStaff(wmsl, i,
                    Double.parseDouble(loaFactor.toString()));
                WorkerManagerStatus wmss = staffs[i].getWorkerManagerStatus();
                jip.updateStaffStatus(
                    staffs[i],
                    new StaffStatus(jip.getJobID(), staffs[i].getStaffID(), 0,
                        StaffStatus.State.UNASSIGNED, "running", wmss
                            .getWorkerManagerName(),
                            StaffStatus.Phase.STARTING));
                // update the WorkerManagerStatus Cache
                wmss.setRunningStaffsCount(wmss.getRunningStaffsCount() + 1);
                LOG.info("debug: kill staffs of the job in schedule process" +
                   "whose staffs haven't schedule over");
                this.updateWhiteWorkerManagersKey(wmss, wmss);
                LOG.info(t.getStaffAttemptId() + " is divided to the " +
                   wmss.getWorkerManagerName());
              }
            }
            this.killJob(jip);
          }
        }
      }
    }
  }

  /**
   * start all of the job that have already in running queue
   * @param queueManager
   *        queueManager that handle the processing queue.
   * @throws IOException
   *         exceptions during handle hdfs log.
   */
  public void startAllRunningJob(QueueManager queueManager) throws IOException {
    String PROCESSING_QUEUE = "processingQueue";
    Queue<JobInProgress> processingQueue = queueManager
        .findQueue(PROCESSING_QUEUE);
    Collection<JobInProgress> jobs = processingQueue.getJobs();
    for (JobInProgress jip : jobs) {
      Collection<WorkerManagerStatus> wmlist = null;
      ArrayList<WorkerManagerStatus> wmsl = new
          ArrayList<WorkerManagerStatus>();
      // FSDataInputStream in = this.haLogOperator.readFile(conf
      // .get(Constants.BC_BSP_HA_LOG_DIR)
      // + jip.getJobID().toString());
      BSPFSDataInputStream bspin = new BSPFSDataInputStreamImpl(haLogOperator,
          conf.get(Constants.BC_BSP_HA_LOG_DIR) + jip.getJobID().toString());
      Text loaFactor = new Text();
      loaFactor.readFields(bspin.getIn());
      while (bspin != null) {
        try {
          WorkerManagerStatus wmStatus = new WorkerManagerStatus();
          wmStatus.readFields(bspin.getIn());
          wmsl.add(wmStatus);
        } catch (EOFException e) {
          bspin = null;
        }
      }
      wmlist = wmsl;
      // LOG.info("wmlist size=" + wmsl.size());
      // recovery the jobInprogress state
      StaffInProgress[] staffs = jip.getStaffInProgress();
      for (int i = 0; i < staffs.length; i++) {
        if (!staffs[i].isRunning() && !staffs[i].isComplete()) {
          Staff t = jip.obtainNewStaff(wmlist, i,
              Double.parseDouble(loaFactor.toString()));
          WorkerManagerStatus wmss = staffs[i].getWorkerManagerStatus();
          jip.updateStaffStatus(staffs[i], new StaffStatus(jip.getJobID(),
              staffs[i].getStaffID(), 0, StaffStatus.State.UNASSIGNED,
              "running", wmss.getWorkerManagerName(),
              StaffStatus.Phase.STARTING));
          // update the WorkerManagerStatus Cache
          wmss.setRunningStaffsCount(wmss.getRunningStaffsCount() + 1);
          // LOG.info("debug: start all the running job");
          this.updateWhiteWorkerManagersKey(wmss, wmss);
          LOG.info(t.getStaffAttemptId() + " is divided to the " +
             wmss.getWorkerManagerName());
        }
      }
      jip.getGssc().setCurrentSuperStep();
      // LOG.info("before jip.getGssc().start(); ");
      jip.getGssc().setCheckNumBase();
      jip.getGssc().start();
    }
  }

  // For recovery jobs when have token over control
  /**
   * Review Contents: revoveryForHa method is so long , you can split into
   * several short method; Review Time: 2013-10-9 Reviewer : Bairen Chen Fix log
   * : the recoveryForHa method have been split into six submethod; Fix time :
   * 2013-10-10 Programer : Changning Chen
   */
  public void recoveryForHa() throws IOException {
    LOG.info("in recoveryForHa()");
    long old = System.currentTimeMillis();
    Map<BSPJobID, String> jobsForRecovery = new
        ConcurrentHashMap<BSPJobID, String>();
    this.submitAllJob(jobsForRecovery);
    this.getAllJobInprogressForRecovery(jobsForRecovery);
    QueueManager queueManager = this.staffScheduler.getQueueManager();
    this.recoveryQueue(queueManager, jobsForRecovery);
    this.killStaffInScheduleing(queueManager);
    this.becomeActive();
    LOG.info("Now BspController's role is :" + this.role);
    this.staffScheduler.setRole(role);
    this.staffScheduler.getQueueManager().setRole(role);
    this.staffScheduler.jobProcessorStart();
    this.cto.start();
    // Baoxing Yang added
    this.pf.start();
    LOG.info("The spent time is :" + (System.currentTimeMillis() - old) / 1000 +
       "s");
    LOG.info("have successfully changed the BspController's role :" +
       this.role);
    this.startAllRunningJob(queueManager);
  }

  /* Zhicheng Liu added */
  /**
   * get jobinprogress depends on jobID
   * @param id
   *        jobID
   * @return
   *        jobInprogress object.
   */
  public JobInProgress getJob(BSPJobID id) {
    return jobs.get(id);
  }

  /**
   * Register current staffs' loads on workers
   * @param workerManagerName
   *        workerManagerName where staffs on.
   * @param status
   *        staff status that need to be updated.
   */
  public void updateStaff(String workerManagerName, StaffStatus status) {
    if (!this.workerToStaffs.containsKey(workerManagerName)) {
      ArrayList<StaffStatus> list = new ArrayList<StaffStatus>();
      list.add(status);
      this.workerToStaffs.put(workerManagerName, list);
    } else if (!this.workerToStaffs.get(workerManagerName).contains(status)) {
      this.workerToStaffs.get(workerManagerName).add(status);
    } else {
      this.workerToStaffs.get(workerManagerName).remove(status);
      this.workerToStaffs.get(workerManagerName).add(status);
    }
  }

  /**
   * Check the staffs' loads on worker
   * @param workerManagerName
   *        workerManagerName that need to be checked.
   * @return
   *        if the worker contains the workername return staffstatus list.
   */
  public ArrayList<StaffStatus> checkStaff(String workerManagerName) {
    if (this.workerToStaffs.containsKey(workerManagerName)) {
      return this.workerToStaffs.get(workerManagerName);
    } else {
      return null;
    }
  }

  /**
   * Update infomation when staff migrated
   * @param workerManagerName
   *        workerManagerName that need to be updated.
   * @param status
   *        staff status that need to be removed.
   */
  public void deleteOldStaffs(String workerManagerName, StaffStatus status) {
    if (!this.workerToStaffs.containsKey(workerManagerName)) {
      return;
    } else if (!this.workerToStaffs.get(workerManagerName).contains(status)) {
      return;
    } else {
      this.workerToStaffs.get(workerManagerName).remove(status);
    }
  }

  /**
   * get the fault map.
   * @return
   *        map for the jobs that need recovery.
   */
  /* Baoxing Yang added */
  public Map getFaultMap() {
    return this.faultMap;
  }

  /**
   * get the staffstatus.
   * @return
   *        staffstatus array.
   */
  public StaffStatus[] getStf() {
    return stf;
  }

  /**
   * set staff in staf array
   * @param stf
   *        staff array that has been set.
   */
  public void setStf(StaffStatus[] stf) {
    this.stf = stf;
  }

  /**
   * set staffs hashmap.
   * @param staffs
   *        staff hashmap has been set.
   */
  public void setStaffs(HashMap<StaffAttemptID, StaffStatus> staffs) {
    this.staffs = staffs;
  }

  /**
   * get staff hashmap
   * @return
   *        staff hashmap.
   */
  public HashMap<StaffAttemptID, StaffStatus> getStaffs() {
    return staffs;
  }
  
}
