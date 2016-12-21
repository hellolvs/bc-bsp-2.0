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

package com.chinamobile.bcbsp.workermanager;

import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.action.KillStaffAction;
import com.chinamobile.bcbsp.action.LaunchStaffAction;
import com.chinamobile.bcbsp.action.WorkerManagerAction;
import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.bspstaff.BSPStaffRunner;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.fault.storage.Fault.Level;
import com.chinamobile.bcbsp.http.HttpServer;
import com.chinamobile.bcbsp.rpc.ControllerProtocol;
import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileSystem;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFileSystemImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPLocalFileSystemImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPZookeeperImpl;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.ClassLoaderUtil;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffStatus;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * A WorkerManager is a process that manages staffs assigned by the
 * BSPController. Each WorkerManager contacts the BSPController, and it takes
 * assigned staffs and reports its status by means of periodical heart beats
 * with BSPController. Each WorkerManager is designed to run with HDFS or other
 * distributed storages. Basically, a WorkerManager and a data node should be
 * run on one physical node.
 */
public class WorkerManager implements Runnable, WorkerManagerProtocol,
    WorkerAgentProtocol, Watcher {
  /** Define Log variable for outputting messages */
  private static final Log LOG = LogFactory.getLog(WorkerManager.class);
  /** State HEART_BEAT_INTERVAL */
  private static volatile int HEART_BEAT_INTERVAL;
  /** Define the length of cacheQueue */
  private static int CACHE_QUEUE_LENGTH = 20;
  /** State BSP configuration */
  private Configuration conf;
  /** State constants */
  static enum State {
    /** The state of WorkerManager */
    NORMAL, COMPUTE, SYNC, BARRIER, STALE, INTERRUPTED, DENIED
  };
  /** State BSPZookeeper */
  private BSPZookeeper bspzk = null;
  /** WorkerManager has not been initialized */
  private volatile boolean initialized = false;
  /** WorkerManager is running */
  private volatile boolean running = true;
  /** WorkerManager is shuttingDown */
  private volatile boolean shuttingDown = false;
  /** WorkerManager is initialized */
  private boolean justInited = true;
  /** The name of workerManager */
  private String workerManagerName;
  /** IP address of bsp controller */
  private InetSocketAddress bspControllerAddr;
  /** IP address of bsp controller */
  private InetSocketAddress standbyControllerAddr;
  /** State BSPFileSystem */
  private BSPFileSystem bspsystemFS = null;

  /** State the number of failed jobs*/
  private int failures;
  /** State the max count of staff*/
  private int maxStaffsCount = 0;
  /** State the count of running staff*/
  private Integer currentStaffsCount = 0;
  /** State the count of finished staff*/
  private int finishedStaffsCount = 0;

  /** State fault list*/
  private List<Fault> workerFaultList = null;
  /** The list of StaffStatus*/
  private List<StaffStatus> reportStaffStatusList = null;
  /** Map for storing running staffs' StaffInProgress*/
  private Map<StaffAttemptID, StaffInProgress> runningStaffs = null;
  /** Map for storing finished staffs' StaffInProgress*/
  private Map<StaffAttemptID, StaffInProgress> finishedStaffs = null;
  /** Map for storing running jobs*/
  private Map<BSPJobID, RunningJob> runningJobs = null;
  /** Map for storing finished jobs*/
  private Map<BSPJobID, RunningJob> finishedJobs = null;
  /** Map for storing running JobToWorkerAgent*/
  private Map<BSPJobID, WorkerAgentForJob> runningJobtoWorkerAgent = null;
  /** Define LaunchStaffManager object */
  private LaunchStaffManager lsManager = new LaunchStaffManager();

  /** Define rpc server */
  private String rpcServer;
  /** Define worker server */
  private Server workerServer;
  /** State ControllerProtocol */
  private ControllerProtocol controllerClient;
  /** State ControllerProtocol */
  private ControllerProtocol standbyControllerClient;
  /** IP address of staff */
  private InetSocketAddress staffReportAddress;
  /** A server of Staff report server*/
  private Server staffReportServer = null;
  /** The list of failed jobs */
  private ArrayList<BSPJobID> failedJobList = new ArrayList<BSPJobID>();

  /** Http server */
  private HttpServer winfoServer;
  /** Http port */
  private int winfoPort;
  /** Define variable workerMangerStatus */
  private WorkerManagerStatus workerMangerStatus;
  /** Map for storing staffId and its StaffInProgress */
  private Map<StaffAttemptID, StaffInProgress> reprotStaffsMap = null;
  /** The finish time of staff */
  private long finishTime;
  /**For current free port counter. It will travel around 60001~65535 */
  private int currentFreePort = 60000;

  /**
   * Constructor.
   * @param conf Configuration
   */
  public WorkerManager(Configuration conf) throws IOException {
    LOG.info("worker start");
    this.conf = conf;
    String mode = conf.get(Constants.BC_BSP_CONTROLLER_ADDRESS);
    if (!mode.equals("local")) {
      choseActiveControllerAddress();
    }
  }

  /**
   * Initialize workerManager.
   */
  @SuppressWarnings("static-access")
  public synchronized void initialize() throws IOException {
    if (this.conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST) != null) {
      this.workerManagerName = conf
          .get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST);
    }
    if (this.workerManagerName == null) {
      this.workerManagerName = DNS.getDefaultHost(
          conf.get("bsp.dns.interface", "default"),
          conf.get("bsp.dns.nameserver", "default"));
    }
    // check local disk
    checkLocalDirs(conf.getStrings(Constants.BC_BSP_LOCAL_DIRECTORY));
    deleteLocalFiles("workerManager");
    this.workerFaultList = new ArrayList<Fault>();
    this.reportStaffStatusList = new ArrayList<StaffStatus>();
    this.runningStaffs = new ConcurrentHashMap<StaffAttemptID,
        StaffInProgress>();
    this.finishedStaffs = new ConcurrentHashMap<StaffAttemptID,
        StaffInProgress>();
    this.runningJobs = new ConcurrentHashMap<BSPJobID, RunningJob>();
    this.finishedJobs = new ConcurrentHashMap<BSPJobID, RunningJob>();
    this.runningJobtoWorkerAgent = new ConcurrentHashMap<BSPJobID,
        WorkerAgentForJob>();
    this.reprotStaffsMap = new ConcurrentHashMap<StaffAttemptID,
        StaffInProgress>();
    this.conf.set(Constants.BC_BSP_WORKERAGENT_HOST, this.workerManagerName);
    this.conf.set(Constants.BC_BSP_WORKERMANAGER_RPC_HOST,
        this.workerManagerName);
    this.maxStaffsCount = conf.getInt(Constants.BC_BSP_WORKERMANAGER_MAXSTAFFS,
        1);
    this.HEART_BEAT_INTERVAL = conf.getInt(Constants.HEART_BEAT_INTERVAL, 1000);
    LOG.info("The max number of staffs is : " + this.maxStaffsCount);
    int rpcPort = -1;
    String rpcAddr = null;
    if (!this.initialized) {
      rpcAddr = conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST,
          Constants.DEFAULT_BC_BSP_WORKERMANAGER_RPC_HOST);
      rpcPort = conf.getInt(Constants.BC_BSP_WORKERMANAGER_RPC_PORT, 5000);
      if (-1 == rpcPort || null == rpcAddr) {
        throw new IllegalArgumentException("Error rpc address " + rpcAddr +
            " port" + rpcPort);
      }
      this.workerServer = RPC.getServer(this, rpcAddr, rpcPort, conf);
      this.workerServer.start();
      this.rpcServer = rpcAddr + ":" + rpcPort;
      LOG.info("Worker rpc server --> " + rpcServer);
    }
    String address = conf.get(Constants.BC_BSP_WORKERMANAGER_REPORT_ADDRESS);
    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
    String bindAddress = socAddr.getHostName();
    int tmpPort = socAddr.getPort();
    // RPC initialization
    this.staffReportServer = RPC.getServer(this, bindAddress, tmpPort, 10,
        false, this.conf);
    this.staffReportServer.start();
    // http server
    InetAddress addr = InetAddress.getLocalHost();
    String ipSlave = addr.getHostAddress().toString();
    winfoPort = conf.getInt("bcbsp.http.winfoserver.port", 40027);
    winfoServer = new HttpServer("bcbsp", ipSlave, winfoPort, true, conf);
    winfoServer.setAttribute("WorkerManager", this);
    LOG.info("prot：40027");
    winfoServer.start();
    LOG.info("server has started");
    // get the assigned address
    this.staffReportAddress = staffReportServer.getListenerAddress();
    LOG.info("WorkerManager up at: " + this.staffReportAddress);
    DistributedCache.purgeCache(this.conf);
    // establish the communication link to bsp master
    this.controllerClient = (ControllerProtocol) RPC.waitForProxy(
        ControllerProtocol.class, ControllerProtocol.versionID,
        bspControllerAddr, conf);
    // establish the communication link to standby bsp master
    if ("ha".equals(conf.get(Constants.BC_BSP_HA_FLAG, ""))) {
      this.standbyControllerClient = (ControllerProtocol) RPC.waitForProxy(
          ControllerProtocol.class, ControllerProtocol.versionID,
          this.standbyControllerAddr, conf);
    }
    LOG.info("bspControllerAddr = " + bspControllerAddr +
        " standbyControllerAddr = " + standbyControllerAddr);
    // enroll in bsp controller
    if (-1 == rpcPort || null == rpcAddr) {
      throw new IllegalArgumentException("Error rpc address " + rpcAddr +
          " port" + rpcPort);
    }
    this.lsManager.start();
    workerMangerStatus = new WorkerManagerStatus(workerManagerName,
            cloneAndResetRunningStaffStatuses(), maxStaffsCount,
            currentStaffsCount, finishedStaffsCount, failures, this.rpcServer,
            workerFaultList);
    this.workerMangerStatus.setHost(bindAddress);
    this.workerMangerStatus.setHttpPort(this.staffReportAddress.toString());
    this.workerMangerStatus.setLocalIp(ipSlave);
    if (!this.controllerClient.register(workerMangerStatus)) {
      LOG.error("There is a problem in establishing communication" +
        " link with BSPController");
      throw new IOException("There is a problem in establishing" +
        " communication link with BSPController.");
    } else {
      LOG.info("have registed to bsp master");
    }
    if ("ha".equals(conf.get(Constants.BC_BSP_HA_FLAG, ""))) {
      if (!this.standbyControllerClient.register(workerMangerStatus)) {
        LOG.error("There is a problem in establishing communication" +
          " link with BSPController");
        throw new IOException("There is a problem in establishing" +
          " communication link with BSPController.");
      } else {
        LOG.info("have registed to standby bsp master");
      }
    }
    this.running = true;
    this.initialized = true;
  }

  /**
   * Get the IP address of staff report.
   * @return the port at which the staff tracker bound to
   */
  public synchronized InetSocketAddress getStaffTrackerReportAddress() {
    return staffReportAddress;
  }

  /**
   * Launch staff manager.
   */
  private class LaunchStaffManager extends Thread {
    /** block queue */
    private final BlockingQueue<Directive> buffer = new
        LinkedBlockingQueue<Directive>();

    /**
     * Put directive into queue.
     * @param directive {@link Directive}
     */
    public void put(Directive directive) {
      try {
        buffer.put(directive);
      } catch (InterruptedException ie) {
        LOG.error("Unable to put directive into queue.", ie);
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void run() {
      while (true) {
        try {
          Directive directive = buffer.take();
          // update tasks status
          ArrayList<WorkerManagerAction> actionList = directive.getActionList();
          LOG.info("Got Response from BSPController with " +
            ((actionList != null) ? actionList.size() : 0) + " actions");
          // perform actions
          boolean recovery = directive.isRecovery();
          boolean changeWorkerState = directive.isChangeWorkerState();
          int failCounter = directive.getFailCounter();
          if (actionList != null) {
            for (WorkerManagerAction action : actionList) {
              try {
                if (action instanceof LaunchStaffAction) {
                  if (recovery) {
                    String localPath = conf
                        .get(Constants.BC_BSP_LOCAL_DIRECTORY) +
                          "/workerManager";
                    LOG.info("if(recovery == true)" + " " + localPath);
                    if (new BSPLocalFileSystemImpl(conf)
                        .exists(new BSPHdfsImpl().newpath(localPath,
                            ((LaunchStaffAction) action).getStaff()
                                .getStaffAttemptId().toString()))) {
                      new BSPLocalFileSystemImpl(conf)
                          .delete(new BSPHdfsImpl().newpath(localPath,
                              ((LaunchStaffAction) action).getStaff()
                                  .getStaffAttemptId().toString()), true);
                    }
                  }
                  LaunchStaffAction temp = (LaunchStaffAction) action;
                  LOG.info("Now will start staff " +
                    temp.getStaff().getStaffID());
                  LOG.info("debug:" + temp.getStaff().getJobFile());
                  startNewStaff((LaunchStaffAction) action, directive,
                      recovery, changeWorkerState, failCounter);
                  // return true;
                } else {
                  KillStaffAction killAction = (KillStaffAction) action;
                  if (runningStaffs.containsKey(killAction.getStaffID())) {
                    StaffInProgress sip = runningStaffs.get(killAction
                        .getStaffID());
                    sip.staffStatus.setRunState(StaffStatus.State.KILLED);
                    sip.killAndCleanup(true);
                  } else {
                    LOG.warn(killAction.getStaffID() +
                        " is not in the runningStaffs " +
                        "and the kill action is invalid.");
                  }
                  // return false;
                }
              } catch (IOException e) {
                LOG.error(
                    "Exception has been catched in WorkerManager--dispatch !",
                    e);
                StaffInProgress sip = null;
                sip = runningStaffs.get(((LaunchStaffAction) action).getStaff()
                    .getStaffAttemptId());
                sip.getStatus().setStage(0); // convenient for
                // the call in
                // controller
                sip.setStaffStatus(Constants.SATAFF_STATUS.FAULT, new Fault(
                    Fault.Type.DISK, Level.WARNING, sip.getStatus()
                        .getWorkerManager(), "IOException happened", sip
                        .getStatus().getJobId().toString(), sip.getStatus()
                          .getStaffId().toString()));
              }
            }
          }
        } catch (Exception e) {
          //LOG.error(" [WorkerManager run]", e);
          throw new RuntimeException("WorkerManager run() exception", e);
        }
      }
    }
  }

  @Override
  public boolean dispatch(BSPJobID jobId, Directive directive,
      boolean recovery, boolean changeWorkerState, int failCounter) {
    directive.setRecovery(recovery);
    directive.setChangeWorkerState(changeWorkerState);
    directive.setFailCounter(failCounter);
    this.lsManager.put(directive);
    return true;
  }

  /**
   * Check local disk.
   * @param localDirs the string array of local disk
   */
  private static void checkLocalDirs(String[] localDirs)
      throws DiskErrorException {
    boolean writable = false;
    if (localDirs != null) {
      for (int i = 0; i < localDirs.length; i++) {
        try {
          DiskChecker.checkDir(new File(localDirs[i]));
          LOG.info("Local System is Normal : " + localDirs[i]);
          writable = true;
        } catch (DiskErrorException e) {
          //LOG.error("BSP Processor local", e);
          throw new RuntimeException("WorkerManager checkLocalDirs" +
            " exception", e);
        }
      }
    }

    if (!writable) {
      throw new DiskErrorException("all local directories are not writable");
    }
  }

  public String[] getLocalDirs() {
    return conf.getStrings(Constants.BC_BSP_LOCAL_DIRECTORY);
  }

  /**
   * Delete local file.
   */
  public void deleteLocalFiles() throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      File f = new File(localDirs[i]);
      deleteLocalDir(f);
    }
  }

  /**
   * Delete local directory.
   * @param dir File
   */
  public void deleteLocalDir(File dir) {
    if (dir == null || !dir.exists() || !dir.isDirectory()) {
      return;
    }
    for (File file : dir.listFiles()) {
      if (file.isFile()) {
        // delete the file
        file.delete();
      } else if (file.isDirectory()) {
        // recursive delete the subdir
        deleteLocalDir(file);
      }
    }
    // delete the root directory
    dir.delete();
  }

  /**
   * Delete local file.
   * @param subdir local file
   */
  public void deleteLocalFiles(String subdir) throws IOException {
    try {
      String[] localDirs = getLocalDirs();
      for (int i = 0; i < localDirs.length; i++) {
        new BSPLocalFileSystemImpl(this.conf).delete(
            new BSPHdfsImpl().newpath(localDirs[i], subdir), true);
      }
    } catch (NullPointerException e) {
      //LOG.error("[deleteLocalFiles]", e);
      throw new RuntimeException("WorkerManager deleteLocalFiles" +
          " exception", e);
    }
  }

  /**
   * Delete local file.
   */
  public void cleanupStorage() throws IOException {
    deleteLocalFiles();
  }

  /**
   * Clean up threads.
   */
  private void startCleanupThreads() throws IOException {

  }

  /**
   * Update staff statistics.
   * @param jobId BSPJobID
   */
  public void updateStaffStatistics(BSPJobID jobId) throws Exception {
    synchronized (currentStaffsCount) {
      currentStaffsCount--;
    }
    finishedStaffsCount++;
    if (finishedStaffs.size() > CACHE_QUEUE_LENGTH) {
      finishedStaffs.clear();
    }
   synchronized (runningJobs) {
      int counter = runningJobs.get(jobId).getStaffCounter();
      if (counter > 0) {
        runningJobs.get(jobId).setStaffCounter(counter - 1);
      }
      if (runningJobs.get(jobId).getStaffCounter() == 0) {
        if (finishedJobs.size() > CACHE_QUEUE_LENGTH) {
          finishedJobs.clear();
        }
        finishedJobs.put(jobId, runningJobs.remove(jobId));
        runningJobtoWorkerAgent.get(jobId).close();
        runningJobtoWorkerAgent.remove(jobId);
      }
    }
  }

  /**
   * Set up WorkerManagerStatus,report to controller client.
   * @return the state of WorkerManager
   */
  public State offerService() throws Exception {
    while (running && !shuttingDown) {
      try {
        this.reportStaffStatusList.clear();
        Iterator<Entry<StaffAttemptID, StaffInProgress>> runningStaffsIt =
            runningStaffs.entrySet().iterator();
        Entry<StaffAttemptID, StaffInProgress> entry;
        while (runningStaffsIt.hasNext()) {
          entry = runningStaffsIt.next();
          switch (entry.getValue().getStatus().getRunState()) {
          case COMMIT_PENDING:
          case UNASSIGNED:
            break;
          case RUNNING:
            this.reportStaffStatusList.add(entry.getValue().getStatus());
            if (this.reprotStaffsMap.containsKey(entry.getKey())) {
              this.reprotStaffsMap.remove(entry.getKey());
            }
            this.reprotStaffsMap.put(entry.getKey(), entry.getValue());
            break;
          case SUCCEEDED:
            updateStaffStatistics(entry.getValue().getStatus().getJobId());
            runningStaffsIt.remove();
            finishedStaffs.put(entry.getKey(), entry.getValue());
            if (this.reprotStaffsMap.containsKey(entry.getKey())) {
              this.reprotStaffsMap.remove(entry.getKey());
            }
            this.reprotStaffsMap.put(entry.getKey(), entry.getValue());
            LOG.info(entry.getKey() +
                " has succeed and been removed from the runningStaffs");
            break;
          case FAULT:
            if (entry.getValue().runner.isAlive()) {
              entry.getValue().getStatus().setPhase(StaffStatus.Phase.CLEANUP);
              entry.getValue().runner.kill();
            }
            this.reportStaffStatusList.add(entry.getValue().getStatus());
            updateStaffStatistics(entry.getValue().getStatus().getJobId());
            runningStaffsIt.remove();
            finishedStaffs.put(entry.getKey(), entry.getValue());
            if (this.reprotStaffsMap.containsKey(entry.getKey())) {
              this.reprotStaffsMap.remove(entry.getKey());
            }
            this.reprotStaffsMap.put(entry.getKey(), entry.getValue());
            LOG.error(entry.getKey() +
                " is fault and has been removed from the runningStaffs");
            break;
          case STAFF_RECOVERY:
            break;
          case WORKER_RECOVERY:
            break;
          case FAILED:
            break;
          case KILLED:
            updateStaffStatistics(entry.getValue().getStatus().getJobId());
            runningStaffsIt.remove();
            finishedStaffs.put(entry.getKey(), entry.getValue());
            if (this.reprotStaffsMap.containsKey(entry.getKey())) {
              this.reprotStaffsMap.remove(entry.getKey());
            }
            this.reprotStaffsMap.put(entry.getKey(), entry.getValue());
            LOG.warn(entry.getKey() +
                " has been killed manually and removed from the runningStaffs");
            break;
          case FAILED_UNCLEAN:
            break;
          case KILLED_UNCLEAN:
            // TODO : This staff should be report and request
            // the cleanup task in the future.
            updateStaffStatistics(entry.getValue().getStatus().getJobId());
            runningStaffsIt.remove();
            finishedStaffs.put(entry.getKey(), entry.getValue());
            if (this.reprotStaffsMap.containsKey(entry.getKey())) {
              this.reprotStaffsMap.remove(entry.getKey());
            }
            this.reprotStaffsMap.put(entry.getKey(), entry.getValue());
            LOG.warn(entry.getKey() +
                " has been killed manually and removed from the runningStaffs");
            break;
          default:
            LOG.error("Unknown StaffStatus.State: " +
              entry.getValue().getStatus().getRunState());
          }
        }
        this.reportStaffStatusList.clear();
        Iterator<Entry<StaffAttemptID, StaffInProgress>> iter2 = reprotStaffsMap
            .entrySet().iterator();
        while (iter2.hasNext()) {
          Entry<StaffAttemptID, StaffInProgress> entry2 = iter2.next();
          // LOG.info(entry2.getKey()+"," + entry2.getValue().getRunState());
          this.reportStaffStatusList.add(entry2.getValue().getStatus());
        }
        reprotStaffsMap.clear();
        this.workerMangerStatus.setStaffReports(this.reportStaffStatusList);
        this.workerMangerStatus.setMaxStaffsCount(maxStaffsCount);
        this.workerMangerStatus.setRunningStaffsCount(currentStaffsCount);
        this.workerMangerStatus.setFinishedStaffsCount(finishedStaffsCount);
        this.workerMangerStatus.setFailedStaffsCount(failures);
        this.workerMangerStatus.setWorkerFaultList(workerFaultList);
        try {
          boolean ret = this.controllerClient.report(new Directive(
              this.workerMangerStatus));
          synchronized (this) {
            workerFaultList.clear();
          } // list.add() need synchronize
          if (!ret) {
            LOG.error("fail to update");
          }
        } catch (Exception ioe) {
          LOG.error("Fail to communicate with BSPController for reporting." +
            " in offerservice , Now will fresh controllerClient",
              ioe);
          this.ensureFreshControllerClient();
        }
        Thread.sleep(HEART_BEAT_INTERVAL);
      } catch (InterruptedException ie) {
        //LOG.error("[offerService]", ie);
        throw new RuntimeException("WorkerManager offerService" +
            " InterruptedException", ie);
      }
    }
    return State.NORMAL;
  }

  /**
   * Set up StaffInProgess object,
   * and call localizeJob(StaffInProgress,Directive) to local.
   * @param action {@link LaunchStaffAction}
   * @param directive {@link Directive}
   * @param recovery the flag of recovery
   * @param changeWorkerState the flag of changeWorkerState
   * @param failCounter the counter of failed staff
   */
  private void startNewStaff(LaunchStaffAction action, Directive directive,
      boolean recovery, boolean changeWorkerState, int failCounter) {
    Staff s = action.getStaff();
    LOG.info("debug: in startNewStaff jobFile is " + s.getJobFile());
    BSPJob jobConf = null;
    try {
      jobConf = new BSPJob(s.getJobID(), s.getJobFile());
      jobConf.setInt("staff.fault.superstep", directive.getFaultSSStep());
    } catch (IOException e1) {
      LOG.error(
          "Exception has been catched in WorkerManager--startNewStaff-jobConf",
          e1);
      StaffInProgress sip = runningStaffs.get(((LaunchStaffAction) action)
          .getStaff().getStaffAttemptId());
      sip.getStatus().setStage(0); // convenient for the call in
      // controller
      sip.setStaffStatus(Constants.SATAFF_STATUS.FAULT, new Fault(
          Fault.Type.DISK, Level.WARNING, sip.getStatus().getWorkerManager(),
          "IOException happened", sip.getStatus().getJobId().toString(), sip
              .getStatus().getStaffId().toString()));
    }
    StaffInProgress sip = new StaffInProgress(s, jobConf,
        this.workerManagerName);
    sip.setFailCounter(failCounter);
    sip.setMigrateSS(directive.getMigrateSSStep());
    if (recovery) {
      sip.getStatus().setRecovery(true);
    }
    if (changeWorkerState) {
      sip.setChangeWorkerState(true);
    }
    LOG.info("debug: before localizeJob(sip, directive); job type is " +
      jobConf.getJobType());
    try {
      localizeJob(sip, directive);
    } catch (IOException e) {
      LOG.error("Exception has been catched in WorkerManager" +
        "--startNewStaff-localizeJob", e);
      sip = runningStaffs.get(((LaunchStaffAction) action).getStaff()
          .getStaffAttemptId());
      // convenient for the call in
      sip.getStatus().setStage(0);
      // controller
      sip.setStaffStatus(Constants.SATAFF_STATUS.FAULT, new Fault(
          Fault.Type.DISK, Level.WARNING, sip.getStatus().getWorkerManager(),
          "IOException happened", sip.getStatus().getJobId().toString(), sip
              .getStatus().getStaffId().toString()));
    }
  }

  /**
   * Localization of job.
   * @param sip StaffInProgress
   * @param directive {@link Directive}
   */
  private void localizeJob(StaffInProgress sip, Directive directive)
      throws IOException {
    Staff staff = sip.getStaff();
    conf.addResource(staff.getJobFile());
    BSPJob defaultJobConf = new BSPJob((BSPConfiguration) conf);
    Path localJobFile = defaultJobConf
        .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/" +
          staff.getStaffID() + "/" + "job.xml");
    Path localJarFile = null;
    // systemFS.copyToLocalFile(new Path(staff.getJobFile()), localJobFile);
    bspsystemFS.copyToLocalFile(new BSPHdfsImpl().newPath(staff.getJobFile()),
        localJobFile);
    BSPConfiguration confBsp = new BSPConfiguration();
    confBsp.addResource(localJobFile);
    LOG.info("debug: conf.get(Constants.USER_BC_BSP_JOB_TYPE) " +
        confBsp.get(Constants.USER_BC_BSP_JOB_TYPE));
    BSPJob jobConf = new BSPJob(confBsp, staff.getJobID().toString());
    LOG.info("debug: conf.get(Constants.USER_BC_BSP_JOB_TYPE) " +
        confBsp.get(Constants.USER_BC_BSP_JOB_TYPE));
    LOG.info("debug: job type is " + jobConf.getJobType());
    if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(jobConf.getJobType())) {
      LOG.info("debug: in LocalizeJob job.exe");
      localJarFile = defaultJobConf
          .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/" +
            staff.getStaffID() + "/" + "jobC");
    } else {
      LOG.info("debug: in in LocalizeJob  job.jar");
      localJarFile = defaultJobConf
          .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/" +
            staff.getStaffID() + "/" + "job.jar");
    }
    Path jarFile = null;
    LOG.info("debug: job type is" + jobConf.getJobType());
    if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(jobConf.getJobType())) {
      LOG.info("debug: in LocalizeJob bofore jobConf.getJobExe =" +
        jobConf.getJobExe());
      if (jobConf.getJobExe() != null) {
        jarFile = new Path(jobConf.getJobExe());
      }
      LOG.info("jarFile is" + jarFile);
      jobConf.setJobExe(localJarFile.toString());
    } else {
      if (jobConf.getJar() != null) {
        jarFile = new Path(jobConf.getJar());
      }
      jobConf.setJar(localJarFile.toString());
    }
    if (jarFile != null) {
      LOG.info("jarFile != null");
      bspsystemFS.copyToLocalFile(jarFile, localJarFile);
      File workDir = new File(new File(localJobFile.toString()).getParent(),
          "work");
      if (!workDir.mkdirs()) {
        if (!workDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " +
            workDir.toString());
        }
      }
      if (!Constants.USER_BC_BSP_JOB_TYPE_C.equals(jobConf.getJobType())) {
        RunJar.unJar(new File(localJarFile.toString()), workDir);
        /** Add the user program jar to the system's classpath. */
        ClassLoaderUtil.addClassPath(localJarFile.toString());
      }
    }
    RunningJob rjob = addStaffToJob(staff.getJobID(), localJobFile, sip,
        directive, jobConf);
    LOG.info("debug:after addStaffToJob(staff.getJobID(), " +
        "localJobFile, sip, directive, jobConf); ");
    rjob.localized = true;
    sip.setFaultSSStep(directive.getFaultSSStep());
    LOG.info("debug:before launchStaffForJob(sip, jobConf);");
    launchStaffForJob(sip, jobConf);
  }

  /**
   * Launch staff for job.
   * @param sip StaffInProgress
   * @param jobConf BSPJob
   */
  private void launchStaffForJob(StaffInProgress sip, BSPJob jobConf) {
    try {
      sip.setJobConf(jobConf);
      sip.launchStaff();
    } catch (IOException ioe) {
      LOG.error("Exception has been catched in WorkerManager" +
        "--launchStaffForJob", ioe);
      sip.staffStatus.setRunState(StaffStatus.State.FAILED);
      sip.getStatus().setStage(0); // convenient for the call in
      // controller
      sip.setStaffStatus(Constants.SATAFF_STATUS.FAULT, new Fault(
          Fault.Type.SYSTEMSERVICE, Fault.Level.INDETERMINATE, sip.getStatus()
              .getWorkerManager(), ioe.toString(), sip.getStatus().getJobId()
              .toString(), sip.getStatus().getStaffId().toString()));
    }
  }

  /**
   * Add staff to job.
   * @param jobId BSPJobID
   * @param localJobFile the path of local job file
   * @param sip StaffInProgress
   * @param directive Directive
   * @param job BSPJob
   * @return running job
   */
  private RunningJob addStaffToJob(BSPJobID jobId, Path localJobFile,
      StaffInProgress sip, Directive directive, BSPJob job) {
    synchronized (runningJobs) {
      RunningJob rJob = null;
      if (!runningJobs.containsKey(jobId)) {
        rJob = new RunningJob(jobId, localJobFile);
        rJob.localized = false;
        rJob.staffs = new HashSet<StaffInProgress>();
        rJob.jobFile = localJobFile;
        runningJobs.put(jobId, rJob);
        // Create a new WorkerAgentForJob for a new job
        try {
          WorkerAgentForJob bspPeerForJob = new WorkerAgentForJob(conf, jobId,
              job, this);
          runningJobtoWorkerAgent.put(jobId, bspPeerForJob);
        } catch (IOException e) {
          LOG.error("Failed to create a WorkerAgentForJob for a new job" +
            jobId.toString());
        }
      } else {
        rJob = runningJobs.get(jobId);
      }
      rJob.staffs.add(sip);
      int counter = rJob.getStaffCounter();
      rJob.setStaffCounter(counter + 1);
      return rJob;
    }
  }

  /**
   * The data structure for initializing a job.
   */
  static class RunningJob {
    /** State BSPJobID */
    private BSPJobID jobId;
    /** State Path variable */
    private Path jobFile;
    /** State the set of StaffInProgress */
    private Set<StaffInProgress> staffs;
    /** Define the counter of staff */
    private int staffCounter = 0;
    /** The flag of localized */
    private boolean localized;
    /** The flag of keepJobFiles*/
    private boolean keepJobFiles;

    /**
     * constructor.
     * @param jobId BSPJobID
     * @param jobFile Path
     */
    RunningJob(BSPJobID jobId, Path jobFile) {
      this.jobId = jobId;
      localized = false;
      staffs = new HashSet<StaffInProgress>();
      this.jobFile = jobFile;
      keepJobFiles = false;
    }
    
    RunningJob(){
      
    }
    
    Path getJobFile() {
      return jobFile;
    }

    BSPJobID getJobId() {
      return jobId;
    }

    public void setStaffCounter(int counter) {
      staffCounter = counter;
    }

    public int getStaffCounter() {
      return staffCounter;
    }
  }

  /**
   * Clone and reset running staff's StaffStatus.
   * @return the list of StaffStatus
   */
  private synchronized List<StaffStatus> cloneAndResetRunningStaffStatuses() {
    List<StaffStatus> result = new ArrayList<StaffStatus>(runningStaffs.size());
    for (StaffInProgress sip : runningStaffs.values()) {
      StaffStatus status = sip.getStatus();
      result.add((StaffStatus) status.clone());
    }
    return result;
  }

  /**
   * Init file system.
   */
  public void initFileSystem() throws Exception {
    if (justInited) {
      String dir = controllerClient.getSystemDir();
      if (dir == null) {
        LOG.error("Fail to get system directory.");
        throw new IOException("Fail to get system directory.");
      }
      bspsystemFS = new BSPFileSystemImpl(dir, conf);
    }
    justInited = false;
  }

  /**
   * run() method of WorkerManager.
   */
  public void run() {
    try {
      initialize();
      initFileSystem();
      startCleanupThreads();
      boolean denied = false;
      while (running && !shuttingDown && !denied) {
        boolean staleState = false;
        try {
          while (running && !staleState && !shuttingDown && !denied) {
            try {
              State osState = offerService();
              if (osState == State.STALE) {
                staleState = true;
              } else if (osState == State.DENIED) {
                denied = true;
              }
            } catch (Exception e) {
              if (!shuttingDown) {
                LOG.warn("Lost connection to BSP Controller [" +
                  bspControllerAddr + "].  Retrying...", e);
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException ie) {
                  //LOG.error("[run]", ie);
                  throw new RuntimeException("WorkerManager run" +
                      " InterruptedException", ie);
                }
              }
            }
          }
        } catch (Exception e) {
          //LOG.error("[run]", e);
          throw new RuntimeException("WorkerManager run" +
              " Exception", e);
        }
        if (shuttingDown) {
          return;
        }
        LOG.warn("Reinitializing local state");
        initialize();
        initFileSystem();
      }
    } catch (Exception ioe) {
      LOG.error("Got fatal exception in WorkerManager: " +
        StringUtils.stringifyException(ioe));
      LOG.error("WorkerManager will quit abnormally!");
      close();
      return;
    }
  }

  /**
   * Shut down the WorkerManager.
   */
  public synchronized void shutdown() throws IOException {
    LOG.info("Prepare to shutdown the WorkerManager");
    shuttingDown = true;
    close();
  }

  /**
   * Stop all Staff Process and WorkerAgentForJob.
   * Stop all RPC Server.
   * Clean up temporary files on the local disk.
   */
  @SuppressWarnings("deprecation")
  public synchronized void close() {
    this.running = false;
    this.initialized = false;
    try {
      for (StaffInProgress sip : runningStaffs.values()) {
        if (sip.runner.isAlive()) {
          sip.killAndCleanup(true);
          LOG.info(sip.getStatus().getStaffId() + " has been killed by system");
        }
      }
      LOG.info("Succeed to stop all Staff Process");
      for (Map.Entry<BSPJobID, WorkerAgentForJob> e : runningJobtoWorkerAgent
          .entrySet()) {
        e.getValue().close();
      }
      LOG.info("Succeed to stop all WorkerAgentForJob");
      this.workerServer.stop();
      this.lsManager.stop();
      RPC.stopProxy(controllerClient);
      if (staffReportServer != null) {
        staffReportServer.stop();
        staffReportServer = null;
      }
      LOG.info("Succeed to stop all RPC Server");
      cleanupStorage();
      LOG.info("Succeed to cleanup temporary files on the local disk");
    } catch (Exception e) {
      //LOG.error("Failed to execute the close()", e);
      throw new RuntimeException("WorkerManager run" +
          " Failed to execute the close()", e);
    }
  }

  /**
   * Start the WorkerManager
   *
   * @param hrs WorkerManager
   * @return the thread of startWorkerManager
   */
  public static Thread startWorkerManager(final WorkerManager hrs) {
    return startWorkerManager(hrs, "regionserver" + hrs.workerManagerName);
  }

  /**
   * Start the WorkerManager
   *
   * @param hrs WorkerManager
   * @param name the name of thread
   * @return the thread of startWorkerManager
   */
  public static Thread startWorkerManager(final WorkerManager hrs,
      final String name) {
    Thread t = new Thread(hrs);
    t.setName(name);
    t.start();
    return t;
  }

  /**
   * StaffInProgress maintains all the info for a Staff that lives at this
   * WorkerManager. It maintains the Staff object, its StaffStatus, and the
   * BSPStaffRunner.
   */
  class StaffInProgress {
    /** State staff variable */
    private Staff staff;
    /** State WorkerAgentForStaffInterface variable */
    private WorkerAgentForStaffInterface staffAgent;
    /** BSPJob variable*/
    private BSPJob jobConf;
    /** BSPJob variable*/
    private BSPJob localJobConf;
    /** Define BSPStaffRunner variable*/
    private BSPStaffRunner runner;
    /** Flag of staff is killed */
    private volatile boolean wasKilled = false;
    /** StaffStatus variable */
    private StaffStatus staffStatus;
    /** Define error is String type */
    private String error = "no";
    /** Fault synchronization super step */
    private int faultSSStep = 0;
    /** The flag of changeWorkerState */
    private boolean changeWorkerState = false;
    /** The counter of failed staff */
    private int failCounter = 0;
    /** Migrate Super Step */
    private int migrateSuperStep = 0;

    /**
     * Constructor.
     * @param staff Staff
     * @param jobConf BSPJob
     * @param workerManagerName the name of WorkerManager
     */
    public StaffInProgress(Staff staff, BSPJob jobConf,
        String workerManagerName) {
      this.staff = staff;
      this.jobConf = jobConf;
      this.localJobConf = null;
      this.staffStatus = new StaffStatus(staff.getJobID(), staff.getStaffID(),
          0, StaffStatus.State.UNASSIGNED, "running", workerManagerName,
          StaffStatus.Phase.STARTING);
    }
    
    public StaffInProgress(){
      
    }

    /**
     * Set StaffStatus.
     * @param stateStatus the state of staff
     * @param fault Fault
     */
    public void setStaffStatus(int stateStatus, Fault fault) {
      switch (stateStatus) {
      case Constants.SATAFF_STATUS.RUNNING:
        this.staffStatus.setRunState(StaffStatus.State.RUNNING);
        break;
      case Constants.SATAFF_STATUS.SUCCEED:
        this.staffStatus.setRunState(StaffStatus.State.SUCCEEDED);
        finishTime = System.currentTimeMillis();
        this.staffStatus.setFinishTime(finishTime);
        break;
      case Constants.SATAFF_STATUS.FAULT:
        this.staffStatus.setRunState(StaffStatus.State.FAULT);
        this.staffStatus.setFault(fault);
        break;
      default:
        LOG.error("Unknown StaffStatus.State: <Constants.SATAFF_STATUS>" +
          stateStatus);
      }
    }

    public boolean getChangeWorkerState() {
      return changeWorkerState;
    }

    public void setChangeWorkerState(boolean changeWorkerState) {
      this.changeWorkerState = changeWorkerState;
    }

    public String getError() {
      return this.error;
    }

    public int getFaultSSStep() {
      return faultSSStep;
    }

    public void setFaultSSStep(int faultSSStep) {
      this.faultSSStep = faultSSStep;
    }

    public void setFailCounter(int failCounter) {
      this.failCounter = failCounter;
    }

    public int getFailCounter() {
      return this.failCounter;
    }

    /**
     * Localize staff.
     * @param task Staff
     */
    private void localizeStaff(Staff task) throws IOException {
      Path localJobFile = this.jobConf
          .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/" +
            task.getStaffID() + "/job.xml");
      // changed by chen
      Path localJarFile = null;
      if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(this.getJobType())) {
        LOG.info("*************************************");
        localJarFile = this.jobConf
            .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/" +
              task.getStaffID() + "/jobC");
      } else {
        LOG.info("debug: in localizeStaff  job.jar");
        localJarFile = this.jobConf
            .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/" +
              task.getStaffID() + "/job.jar");
      }
      String jobFile = task.getJobFile();
      // systemFS.copyToLocalFile(new Path(jobFile), localJobFile);
      bspsystemFS.copyToLocalFile(new BSPHdfsImpl().newPath(jobFile),
          localJobFile);
      task.setJobFile(localJobFile.toString());
      localJobConf = new BSPJob(task.getJobID(), localJobFile.toString());
      localJobConf.set("bsp.task.id", task.getStaffID().toString());
      String jarFile = null;
      LOG.info("debug: job type is " + this.getJobType());
      if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(this.getJobType())) {
        jarFile = localJobConf.getJobExe();
        if (jarFile != null) {
          // systemFS.copyToLocalFile(new Path(jarFile), localJarFile);
          bspsystemFS.copyToLocalFile(new BSPHdfsImpl().newPath(jarFile),
              localJarFile);
          LOG.info("debug: jobExe=" + localJarFile.toString());
          String localdir = Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER;
          Path localpath = this.jobConf
              .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER +
                  "/JobC");
          File t = new File(localpath.toString());
          if (!t.exists()) {
            // systemFS.copyToLocalFile(new Path(jarFile), localpath);
            // alter by gtt
            bspsystemFS.copyToLocalFile(new BSPHdfsImpl().newPath(jarFile),
                localpath);
          }
          LOG.info("localdir is :" + localdir);
          localJobConf.setJobExe(localJarFile.toString());
        }
      } else {
        jarFile = localJobConf.getJar();
        if (jarFile != null) {
          // systemFS.copyToLocalFile(new Path(jarFile), localJarFile);
          bspsystemFS.copyToLocalFile(new BSPHdfsImpl().newPath(jarFile),
              localJarFile);
          localJobConf.setJar(localJarFile.toString());
        }
      }
      this.staffStatus.setMaxSuperStep(Long.parseLong(String
          .valueOf(localJobConf.getNumSuperStep())));
      LOG.info("debug: localJarFile.toString() is " + localJarFile.toString());
      this.staff.setJobExeLocalPath(localJarFile.toString());
      LOG.debug("localizeStaff : " + localJobConf.getJar());
      LOG.debug("localizeStaff : " + localJobFile.toString());
      task.setConf(localJobConf);
    }

    public synchronized void setJobConf(BSPJob jobConf) {
      this.jobConf = jobConf;
    }

    public synchronized BSPJob getJobConf() {
      return localJobConf;
    }

    /**
     * Launch staff.
     * @throws IOException
     */
    public void launchStaff() throws IOException {
      LOG.info("debug:before localizeStaff(staff);");
      localizeStaff(staff);
      LOG.info("debug:after localizeStaff(staff);");
      staffStatus.setRunState(StaffStatus.State.RUNNING);

      BSPJobID jobID = localJobConf.getJobID();
      runningJobtoWorkerAgent.get(jobID).addStaffCounter(
          staff.getStaffAttemptId());
      runningJobtoWorkerAgent.get(jobID).setJobConf(jobConf);
      runningStaffs.put(staff.getStaffAttemptId(), this);
      LOG.info("in launchStaff() jobC path is " +
        this.getStaff().getJobExeLocalPath());
      synchronized (currentStaffsCount) {
        currentStaffsCount++;
      }
      LOG.info("debug:staff attemtId" + this.staff.getStaffAttemptId());
      this.runner = staff.createRunner(WorkerManager.this);
      this.runner.setFaultSSStep(this.faultSSStep);
      LOG.info("debug: + before runner start");
      this.runner.start();
    }

    /**
     * This task has run on too long, and should be killed.
     * @param wasFailure flag of failure
     */
    public synchronized void killAndCleanup(boolean wasFailure)
        throws IOException {
      onKillStaff();
      runner.kill();
    }

    /**
     * Kill staff.
     */
    private void onKillStaff() {
      if (this.staffAgent != null) {
        this.staffAgent.onKillStaff();
      }
    }

    public Staff getStaff() {
      return staff;
    }

    public synchronized StaffStatus getStatus() {
      return staffStatus;
    }

    public StaffStatus.State getRunState() {
      return staffStatus.getRunState();
    }

    /**
     * Judge that staff was killed.
     * @return flag of Staff that was killed
     */
    public boolean wasKilled() {
      return wasKilled;
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof StaffInProgress) &&
          staff.getStaffID().equals(
              ((StaffInProgress) obj).getStaff().getStaffID());
    }

    @Override
    public int hashCode() {
      return staff.getStaffID().hashCode();
    }

    public void setStaffAgent(WorkerAgentForStaffInterface staffAgent) {
      this.staffAgent = staffAgent;
    }

    public int getMigrateSS() {
      return this.migrateSuperStep;
    }

    public void setMigrateSS(int superstep) {
      this.migrateSuperStep = superstep;
    }

    public String getJobType() {
      return this.jobConf.get(Constants.USER_BC_BSP_JOB_TYPE, "");
    }
    
    /** For JUnit test. */
    public void setStaffStatus(StaffStatus stfs){
      this.staffStatus = stfs;
    }
  }

  public boolean isRunning() {
    return running;
  }

  /**
   * Construct WorkerManger.
   * @param workerManagerClass extends WorkerManager
   * @param conf Configuration
   * @return WorkerManager object
   */
  public static WorkerManager constructWorkerManager(
      Class<? extends WorkerManager> workerManagerClass,
      final Configuration conf) {
    try {
      Constructor<? extends WorkerManager> c = workerManagerClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "WorkerManager: " +
        workerManagerClass.toString(), e);
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(WorkerManagerProtocol.class.getName())) {
      return WorkerManagerProtocol.versionID;
    } else if (protocol.equals(WorkerAgentProtocol.class.getName())) {
      return WorkerAgentProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to WorkerManager: " + protocol);
    }
  }

  /**
   * The main() for child processes.
   */
  public static class Child {
    /**
     * WorkerManager's main method for
     * disposing and preserving WorkerManger.
     * @param args command parameters
     */
    public static void main(String[] args) {
      BSPConfiguration defaultConf = new BSPConfiguration();
      // report address
      String host = args[0];
      int port = Integer.parseInt(args[1]);
      InetSocketAddress address = new InetSocketAddress(host, port);
      StaffAttemptID staffid = StaffAttemptID.forName(args[2]);
      int faultSSStep = Integer.parseInt(args[3]);
      String hostName = args[4];
      String jobType = args[5];
      LOG.info(staffid + ": Child Starts");
      LOG.info("=*=*=*=*=*=*=*=*=*=*=*" +
        "=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*");
      WorkerAgentProtocol umbilical = null;
      Staff staff = null;
      BSPJob job = null;
      try {
        umbilical = (WorkerAgentProtocol) RPC.getProxy(
            WorkerAgentProtocol.class, WorkerAgentProtocol.versionID, address,
            defaultConf);

        staff = umbilical.getStaff(staffid);
        LOG.info("debug:job.xml path = " + staff.getJobFile());
        defaultConf.addResource(new Path(staff.getJobFile()));
        job = new BSPJob(staff.getJobID(), staff.getJobFile());
        LOG.info("debug:job.exe path = " + staff.getJobExeLocalPath());
        LOG.info("debug:job.jar path = " + job.getJar());
        // use job-specified working directory
        new BSPFileSystemImpl(job.getConf())
            .setWorkingDirectory(new BSPHdfsImpl().getWorkingDirectory());
        boolean recovery = umbilical.getStaffRecoveryState(staffid);
        boolean changeWorkerState = umbilical
            .getStaffChangeWorkerState(staffid);
        int failCounter = umbilical.getFailCounter(staffid);
        job.setInt("staff.fault.superstep", faultSSStep);
        int migrateStep = umbilical.getMigrateSuperStep(staffid);
        if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(jobType)) {
          staff.runC(job, staff, umbilical, recovery, changeWorkerState,
              failCounter, hostName);
        } else if (job.getComputeState() == 0){
          staff.run(job, staff, umbilical, recovery, changeWorkerState,
              migrateStep, failCounter, hostName); // run the task
        }else if(job.getComputeState() ==1 ){
          staff.runPartition(job, staff, umbilical, recovery, changeWorkerState,
              migrateStep, failCounter, hostName);
        }
        LOG.info("staff " + staffid + "run complete!");
      } catch (ClassNotFoundException cnfE) {
        LOG.error(
            "Exception has been catched in WorkerManager--Error running child",
            cnfE);
        // Report back any failures, for diagnostic purposes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        cnfE.printStackTrace(new PrintStream(baos));
        umbilical.setStaffStatus(
            staffid,
            Constants.SATAFF_STATUS.FAULT,
            new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL, umbilical
                .getWorkerManagerName(job.getJobID(), staffid),
                cnfE.toString(), job.toString(), staffid.toString()), 0);
      } catch (FSError e) {
        LOG.error(
            "Exception has been catched in WorkerManager--FSError from child",
            e);
        umbilical.setStaffStatus(
            staffid,
            Constants.SATAFF_STATUS.FAULT,
            new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL, umbilical
                .getWorkerManagerName(job.getJobID(), staffid), e.toString(),
                job.toString(), staffid.toString()), 0);
      } catch (Throwable throwable) {
        LOG.error(
            "Exception has been catched in WorkerManager--Error running child",
            throwable);
        // Report back any failures, for diagnostic purposes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        throwable.printStackTrace(new PrintStream(baos));
        umbilical.setStaffStatus(
            staffid,
            Constants.SATAFF_STATUS.FAULT,
            new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL, umbilical
                .getWorkerManagerName(job.getJobID(), staffid), throwable
                .toString(), job.toString(), staffid.toString()), 0);
      } finally {
        RPC.stopProxy(umbilical);
        MetricsContext metricsContext = MetricsUtil.getContext("mapred");
        metricsContext.close();
        // Shutting down log4j of the child-vm...
        // This assumes that on return from Staff.run()
        // there is no more logging done.
        LogManager.shutdown();
      }
    }
  }

  @Override
  public Staff getStaff(StaffAttemptID staffid) throws IOException {
    StaffInProgress sip = runningStaffs.get(staffid);
    if (sip != null) {
      return sip.getStaff();
    } else {
      LOG.warn(staffid + " is not in the runningStaffs");
      return null;
    }
  }

  @Override
  public boolean getStaffRecoveryState(StaffAttemptID staffId) {
    return runningStaffs.get(staffId).getStatus().isRecovery();
  }

  @Override
  public boolean getStaffChangeWorkerState(StaffAttemptID staffId) {
    return runningStaffs.get(staffId).getChangeWorkerState();
  }

  @Override
  public int getFailCounter(StaffAttemptID staffId) {
    return this.runningStaffs.get(staffId).getFailCounter();
  }

  @Override
  public boolean ping(StaffAttemptID staffId) throws IOException {
    return false;
  }

  @Override
  public void done(StaffAttemptID staffId, boolean shouldBePromoted)
      throws IOException {
  }

  @Override
  public void fsError(StaffAttemptID staffId, String message)
      throws IOException {
  }

  @Override
  public String getWorkerManagerName(BSPJobID jobId, StaffAttemptID staffId) {
    return runningJobtoWorkerAgent.get(jobId).getWorkerManagerName(jobId,
        staffId);
  }

  @Override
  public boolean localBarrier(BSPJobID jobId, StaffAttemptID staffId,
      int superStepCounter, SuperStepReportContainer ssrc) {
    if (this.runningStaffs.containsKey(staffId)) {
      this.runningStaffs.get(staffId).getStatus()
          .setProgress(superStepCounter + 1);
    }
    return runningJobtoWorkerAgent.get(jobId).localBarrier(jobId, staffId,
        superStepCounter, ssrc);
  }

  @Override
  public void addCounters(BSPJobID jobId, Counters pCounters) {
    this.runningJobtoWorkerAgent.get(jobId).addCounters(pCounters);
  }

  @Override
  public int getNumberWorkers(BSPJobID jobId, StaffAttemptID staffId) {
    return runningJobtoWorkerAgent.get(jobId).getNumberWorkers(jobId, staffId);
  }

  @Override
  public void setNumberWorkers(BSPJobID jobId,
      StaffAttemptID staffId, int num) {
    runningJobtoWorkerAgent.get(jobId).setNumberWorkers(jobId, staffId, num);
  }

  @Override
  public void addStaffReportCounter(BSPJobID jobId) {
    runningJobtoWorkerAgent.get(jobId).addStaffReportCounter();
  }

  public String getWorkerManagerName() {
    return this.workerManagerName;
  }

  @Override
  public BSPJobID getBSPJobID() {
    return null;
  }

  @Override
  public void setStaffStatus(StaffAttemptID staffId, int staffStatus,
      Fault fault, int stage) {
    this.runningStaffs.get(staffId).setStaffStatus(staffStatus, fault);
    this.runningStaffs.get(staffId).getStatus().setStage(stage);
  }

  /**
   * Get StaffStatus.
   * @param staffId StaffAttemptID
   * @return StaffStatus
   */
  public StaffStatus getStaffStatus(StaffAttemptID staffId) {
    return this.runningStaffs.get(staffId).getStatus();
  }

  /**
   * This method is used to set mapping table that shows the partition to the
   * worker. According to Job ID get WorkerAgentForJob and call its method to
   * set this mapping table.
   * @param jobId BSPJobID
   * @param partitionId id of partition
   * @param hostName the name of host
   */
  public void setWorkerNametoPartitions(BSPJobID jobId, int partitionId,
      String hostName) {
    this.runningJobtoWorkerAgent.get(jobId).setWorkerNametoPartitions(jobId,
        partitionId, hostName);
  }

  /**
   * Get the hostName of the workerManager.
   * @return hostName
   */
  public String getHostName() {
    return this.conf.get(Constants.BC_BSP_WORKERAGENT_HOST,
        Constants.DEFAULT_BC_BSP_WORKERAGENT_HOST);
  }

  @Override
  public void clearFailedJobList() {
    this.failedJobList.clear();
  }

  @Override
  public void addFailedJob(BSPJobID jobId) {
    this.failedJobList.add(jobId);
  }

  @Override
  public int getFailedJobCounter() {
    return this.failedJobList.size();
  }

  @Override
  public synchronized int getFreePort() {
    ServerSocket s;
    this.currentFreePort = this.currentFreePort + 1;
    int count = 0;
    for (; this.currentFreePort <= 65536; this.currentFreePort++) {
      count++;
      if (count > 5535) {
        LOG.info("[WorkerManager: getFreePort()] attempts " +
          "to get a free port over 5535 times!");
        return 60000;
      }
      if (this.currentFreePort > 65535) {
        this.currentFreePort = 60001;
      }
      try {
        LOG.info("debug:this.currentFreePort is " + this.currentFreePort);
        s = new ServerSocket(this.currentFreePort);
        s.close();
        return this.currentFreePort;
      } catch (IOException e) {
        LOG.info("debug:this.currentFreePort is " + this.currentFreePort);
        LOG.error("[WokerManager] caught", e);
      }
    }
    return 60000;
  }

  @Override
  public void setStaffAgentAddress(StaffAttemptID staffID, String addr) {
    if (this.runningStaffs.containsKey(staffID)) {
      StaffInProgress sip = this.runningStaffs.get(staffID);
      String[] addrs = addr.split(":");
      InetSocketAddress address = new InetSocketAddress(addrs[0],
          Integer.parseInt(addrs[1]));
      WorkerAgentForStaffInterface staffAgent = null;
      try {
        staffAgent = (WorkerAgentForStaffInterface) RPC.getProxy(
            WorkerAgentForStaffInterface.class,
            WorkerAgentForStaffInterface.versionID, address, this.conf);
      } catch (IOException e) {
        LOG.error("[WorkerManager] caught: ", e);
      }
      sip.setStaffAgent(staffAgent);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    LOG.info("now in process");
    LOG.info("event type is " + event.getType());
    try {
      if (event.getType().toString().equals("NodeDeleted")) {
        LOG.info("in NodeDeleted");
        if (bspzk != null) {
          if (bspzk.equaltoStat(Constants.BSPCONTROLLER_STANDBY_LEADER, true)) {
            String standControllerAddr = getData(
                Constants.BSPCONTROLLER_LEADER);
            InetSocketAddress newStandbyAddr = NetUtils
                .createSocketAddr(standControllerAddr);
            if (!this.standbyControllerAddr.equals(newStandbyAddr)) {
              this.bspControllerAddr = this.standbyControllerAddr;
              this.standbyControllerAddr = newStandbyAddr;
              this.controllerClient = (ControllerProtocol) RPC.getProxy(
                  ControllerProtocol.class, ControllerProtocol.versionID,
                  this.bspControllerAddr, conf);
              this.standbyControllerClient = (ControllerProtocol) RPC.getProxy(
                  ControllerProtocol.class, ControllerProtocol.versionID,
                  this.standbyControllerAddr, conf);
            }
            LOG.info("now the active is " + this.bspControllerAddr.toString() +
                "　and the standby is " + this.standbyControllerAddr.toString());
          }
        }
      } else if (event.getType().toString().equals("NodeDataChanged")) {
        // watch the standby
        bspzk.exists(Constants.BSPCONTROLLER_STANDBY_LEADER, true);
        // establish the communication link to standby bsp master
        this.standbyControllerClient = (ControllerProtocol) RPC.getProxy(
            ControllerProtocol.class, ControllerProtocol.versionID,
            this.standbyControllerAddr, conf);
        LOG.info("bspControllerAddr = " + bspControllerAddr +
            " standbyControllerAddr = " + standbyControllerAddr);
        if (!this.standbyControllerClient.register(workerMangerStatus)) {
          LOG.error("There is a problem in establishing communication" +
            " link with BSPController");
          throw new IOException("There is a problem in establishing" +
            " communication link with BSPController.");
        } else {
          LOG.info("have registed to standby bsp master");
        }
      }
    } catch (Exception e) {
      LOG.error("problem happened when register to standby controller " +
        e.toString());
    }
  }

  /**
   * Get data from the path of ZooKeeper.
   * @param path the path of ZooKeeper
   * @return data from the path of ZooKeeper
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String getData(String path) throws KeeperException,
      InterruptedException {
    if (bspzk != null) {
      byte[] b = bspzk.getData(path, false, null);
      return new String(b);
    }
    return null;
  }

  /**
   * Get the address of BspController whose role is Active
   */
  public void choseActiveControllerAddress() {
    String zkAddress = conf.get(Constants.ZOOKEEPER_QUORUM) + ":" +
      conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
            Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    try {
      this.bspzk = new BSPZookeeperImpl(zkAddress, Constants.SESSION_TIME_OUT,
          this);
      if (bspzk != null) {
        if (bspzk.equaltoStat(Constants.BSPCONTROLLER_LEADER, true)) {
          String controllerAddr = getData(Constants.BSPCONTROLLER_LEADER);
          LOG.info("active controller Address is " + controllerAddr);
          this.bspControllerAddr = NetUtils.createSocketAddr(controllerAddr);
        } else {
          LOG.error("could not get the active BspController's " +
            "address,please restart the System");
        }
        // s = zk.exists(Constants.BSPCONTROLLER_STANDBY_LEADER, true);
        if (bspzk.equaltoStat(Constants.BSPCONTROLLER_STANDBY_LEADER, true)) {
          String standControllerAddr = getData(
              Constants.BSPCONTROLLER_STANDBY_LEADER);
          LOG.info("standby controller Address is " + standControllerAddr);
          this.standbyControllerAddr = NetUtils
              .createSocketAddr(standControllerAddr);
        } else {
          LOG.info("could not get the standby BspController's address," +
            "please restart the standby System");
        }
      }
    } catch (Exception e) {
      LOG.error("could not get the active BspController's address," +
        "please restart the System:" + e.getMessage());
    }
  }

  /**
   * Ensure fresh jobSubmitClient.
   */
  public void ensureFreshControllerClient() {

    this.controllerClient = this.standbyControllerClient;
    InetSocketAddress temp = this.bspControllerAddr;
    this.bspControllerAddr = this.standbyControllerAddr;
    this.standbyControllerAddr = temp;
    LOG.info("now the active is " + this.bspControllerAddr.toString() +
        "　and the standby is " + this.standbyControllerAddr.toString());
    try {
      LOG.info(" in workerManager, Now  will try to connect " +
        this.standbyControllerAddr.toString());
    } catch (Exception e) {
      LOG.warn("lost connection to  " + this.bspControllerAddr.toString());
    }
  }

  @Override
  public int getMigrateSuperStep(StaffAttemptID staffId) {
    return runningStaffs.get(staffId).getMigrateSS();
  }

  @Override
  public boolean updateWorkerJobState(StaffAttemptID staffId) {
    return this.runningJobtoWorkerAgent.get(staffId.getJobID())
        .updateStaffsReporter(staffId);
  }

  @Override
  public void clearStaffRC(BSPJobID jobId) {
    runningJobtoWorkerAgent.get(jobId).clearStaffRC(jobId);
  }

  public Configuration getConf() {
    return conf;
  }

  public Integer getCurrentStaffsCount() {
    return currentStaffsCount;
  }

  public void setCurrentStaffsCount(Integer currentStaffsCount) {
    this.currentStaffsCount = currentStaffsCount;
  }

  public int getFinishedStaffsCount() {
    return finishedStaffsCount;
  }

  public void setFinishedStaffsCount(int finishedStaffsCount) {
    this.finishedStaffsCount = finishedStaffsCount;
  }

  public Map<StaffAttemptID, StaffInProgress> getFinishedStaffs() {
    return finishedStaffs;
  }

  public void setFinishedStaffs(
      Map<StaffAttemptID, StaffInProgress> finishedStaffs) {
    this.finishedStaffs = finishedStaffs;
  }

  public Map<BSPJobID, RunningJob> getRunningJobs() {
    return runningJobs;
  }

  public void setRunningJobs(Map<BSPJobID, RunningJob> runningJobs) {
    this.runningJobs = runningJobs;
  }

  public Map<BSPJobID, WorkerAgentForJob> getRunningJobtoWorkerAgent() {
    return runningJobtoWorkerAgent;
  }

  public void setRunningJobtoWorkerAgent(
      Map<BSPJobID, WorkerAgentForJob> runningJobtoWorkerAgent) {
    this.runningJobtoWorkerAgent = runningJobtoWorkerAgent;
  }

  public Map<StaffAttemptID, StaffInProgress> getRunningStaffs() {
    return runningStaffs;
  }

  public void setRunningStaffs(Map<StaffAttemptID, StaffInProgress> runningStaffs) {
    this.runningStaffs = runningStaffs;
  }

  public ArrayList<BSPJobID> getFailedJobList() {
    return failedJobList;
  }

  public void setFailedJobList(ArrayList<BSPJobID> failedJobList) {
    this.failedJobList = failedJobList;
  }

  public ControllerProtocol getControllerClient() {
    return controllerClient;
  }

  public void setControllerClient(ControllerProtocol controllerClient) {
    this.controllerClient = controllerClient;
  }

  public boolean isJustInited() {
    return justInited;
  }

  public void setJustInited(boolean justInited) {
    this.justInited = justInited;
  }

  public InetSocketAddress getBspControllerAddr() {
    return bspControllerAddr;
  }

  public void setBspControllerAddr(InetSocketAddress bspControllerAddr) {
    this.bspControllerAddr = bspControllerAddr;
  }

  public ControllerProtocol getStandbyControllerClient() {
    return standbyControllerClient;
  }

  public void setStandbyControllerClient(
      ControllerProtocol standbyControllerClient) {
    this.standbyControllerClient = standbyControllerClient;
  }

  public InetSocketAddress getStandbyControllerAddr() {
    return standbyControllerAddr;
  }

  public void setStandbyControllerAddr(InetSocketAddress standbyControllerAddr) {
    this.standbyControllerAddr = standbyControllerAddr;
  }

  public BSPZookeeper getBspzk() {
    return bspzk;
  }

  public void setBspzk(BSPZookeeper bspzk) {
    this.bspzk = bspzk;
  }
}
