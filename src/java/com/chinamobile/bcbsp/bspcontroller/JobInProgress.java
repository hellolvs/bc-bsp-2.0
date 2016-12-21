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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
//import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.eclipse.jdt.core.dom.ThisExpression;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.client.BSPJobClient;
import com.chinamobile.bcbsp.client.BSPJobClient.RawSplit;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.pipes.Application;
import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import com.chinamobile.bcbsp.sync.GeneralSSController;
import com.chinamobile.bcbsp.sync.GeneralSSControllerInterface;
import com.chinamobile.bcbsp.sync.SuperStepCommand;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileSystem;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPLocalFileSystem;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFileSystemImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPLocalFileSystemImpl;
import com.chinamobile.bcbsp.util.*;
import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.action.LaunchStaffAction;
import com.chinamobile.bcbsp.action.WorkerManagerAction;
import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Aggregator;
import com.chinamobile.bcbsp.bspcontroller.Counters.Counter;
import com.chinamobile.bcbsp.bspcontroller.Counters.Group;
import com.chinamobile.bcbsp.bspstaff.StaffInProgress;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;
//add by cui
import com.chinamobile.bcbsp.util.JobStatus.State;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * JobInProgress is the center for controlling the job running. It maintains all
 * important information about the job and the staffs, including the status.
 * 
 * @author
 * @version
 */
public class JobInProgress implements JobInProgressControlInterface {
  /**
   * Used when a kill is issued to a job which is initializing.
   */
  public static class KillInterruptedException extends InterruptedException {
    /** uncertain */
    private static final long serialVersionUID = 1L;
    
    /**
     * exceptions during kill the job
     * 
     * @param msg
     *        uncertain
     */
    public KillInterruptedException(String msg) {
      super(msg);
    }
  }
  
  // add by cui
  private MapWritable mapcounterTemp = new MapWritable();
  /** handle log file in jobInprogress */
  private static final Log LOG = LogFactory.getLog(JobInProgress.class);
  /** stare if staffs are initialied */
  private boolean staffsInited = false;
  /** job checkpoint next superstep symbol */
  private boolean checkPointNext = false;
  /** hdfs configuration for handle hdfs file */
  private Configuration conf;
  /** job ID,user,user-specified job name information */
  private JobProfile profile;
  /** job status running,failed,successed */
  private JobStatus status;
  /** job file path */
  private Path jobFile = null;
  /** local job file path */
  private Path localJobFile = null;
  /** local jar file path */
  private Path localJarFile = null;
  /** BSP local file system */
  private BSPLocalFileSystem BSPlocalFs;
  /** job start time */
  private long startTime;
  /** job launch cluster time */
  private long launchTime;
  /** job finished time */
  private long finishTime;
  /** job response ratio for adjust job between different waitQueues */
  private long hrnR;
  /** estimated job processing time */
  private long processTime;
  /** edgenum to estimate job processing time */
  private long edgeNum;
  /** BSP job id */
  private BSPJobID jobId;
  /** BSP job */
  private BSPJob job;
  /** BSP controller */
  private final BSPController controller;
  /** staffInProgress array to hold staffs information */
  private StaffInProgress staffs[] = new StaffInProgress[0];
  /** job current superstep counter */
  private int superStepCounter;
  /** job total superstep num */
  private int superStepNum;
  /** record taskID and staff status */
  private List<StaffAttemptID> attemptIDList = new ArrayList<StaffAttemptID>();
  /** total BSPstaffs num */
  private int numBSPStaffs = 0;
  /** general Synchronization conreoller */
  private GeneralSSControllerInterface gssc;
  /** different staffs on the same worker times */
  private HashMap<String, ArrayList<StaffAttemptID>> workersToStaffs = new HashMap<String, ArrayList<StaffAttemptID>>();
  /** record the same staff run on different workers' times */
  private LinkedHashMap<StaffAttemptID, Map<String, Integer>> staffToWMTimes = new LinkedHashMap<StaffAttemptID, Map<String, Integer>>();
  /** failed jobs already recovery times */
  private int checkPointFrequency = 0;
  /** staff recovery recovery times */
  private int attemptRecoveryCounter = 0;
  /** */
  private int maxAttemptRecoveryCounter = 0;
  /** max recovery times for a staff */
  private int maxStaffAttemptRecoveryCounter = 0;
  /** checkpoint able to read */
  private int ableCheckPoint = 0;
  /** fault Synchronization super step */
  private int faultSSStep;
  /** failed record for worker workermanagerstatus and failed times */
  private HashMap<WorkerManagerStatus, Integer> failedRecord = new HashMap<WorkerManagerStatus, Integer>();
  /** worker on the blacklist can't assign staffs */
  private ArrayList<WorkerManagerStatus> blackList = new ArrayList<WorkerManagerStatus>();
  /** the priority level of the job,normal is default */
  private String priority = Constants.PRIORITY.NORMAL;
  /** for load balance during assign staffs */
  private long estimateFactor = Constants.USER_BC_BSP_JOB_ESTIMATEPROCESS_FACTOR_DEFAULT;
  /** Map for user registered aggregate values. */
  private HashMap<String, Class<? extends AggregateValue<?, ?>>> nameToAggregateValue = new HashMap<String, Class<? extends AggregateValue<?, ?>>>();
  /** Map for user registered aggregatros. */
  private HashMap<String, Class<? extends Aggregator<?>>> nameToAggregator = new HashMap<String, Class<? extends Aggregator<?>>>();
  /** map for user registered aggregate values */
  @SuppressWarnings("unchecked")
  private HashMap<String, ArrayList<AggregateValue>> aggregateValues = new HashMap<String, ArrayList<AggregateValue>>();
  /** map for user registered aggregate results */
  @SuppressWarnings("unchecked")
  private HashMap<String, AggregateValue> aggregateResults = new HashMap<String, AggregateValue>();
  /** workermanager names list */
  private List<String> WMNames = new ArrayList<String>();
  // add by chen
  /** counr job messages */
  private Counters counters;
  /**
   * Accumulate slow phenomenon for migrate(3times) /* Zhicheng Liu added
   */
  private int[] staffSlowCount;
  /** migratefalg for staff */
  private boolean migrateFlag = false;
  /** open the dynamic staff migrate mode flag */
  private boolean openMigrateMode = false;
  // Baoxing Yang added
  private boolean cleanedWMNs = false;
  private boolean removedFromListener = false;
  /* songjianze added */
  private Application application;
  
  /**
   * jobInProgress construct method.
   * 
   * @param jobId
   *        BSP job id.
   * @param jobFile
   *        BSP job file.
   * @param controller
   *        BSP controller
   * @param conf
   *        BSP System configuration.
   * @throws IOException
   *         exceptions during handle BSP job file.
   */
  public JobInProgress(BSPJobID jobId, Path jobFile, BSPController controller,
      Configuration conf) throws IOException {
    this.jobId = jobId;
    // this.localFs = FileSystem.getLocal(conf);
    this.BSPlocalFs = new BSPLocalFileSystemImpl(conf);
    this.jobFile = jobFile;
    this.controller = controller;
    // this.status = new JobStatus(jobId, null, 0L, 0L,
    // JobStatus.State.PREP.value());
    this.startTime = System.currentTimeMillis();
    this.superStepCounter = 0;
    this.maxAttemptRecoveryCounter = conf.getInt(
        Constants.BC_BSP_JOB_RECOVERY_ATTEMPT_MAX, 0);
    this.maxStaffAttemptRecoveryCounter = conf.getInt(
        Constants.BC_BSP_STAFF_RECOVERY_ATTEMPT_MAX, 0);
    this.localJobFile = controller
        .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_CONTROLLER + "/" + jobId
            + ".xml");
    this.localJarFile = controller
        .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_CONTROLLER + "/" + jobId
            + ".jar");
    Path jobDir = controller.getSystemDirectoryForJob(jobId);
    // FileSystem fs = jobDir.getFileSystem(conf);
    BSPFileSystem bspfs = new BSPFileSystemImpl(controller, jobId, conf);
    bspfs.copyToLocalFile(jobFile, localJobFile);
    job = new BSPJob(jobId, localJobFile.toString());
    this.conf = job.getConf();
    this.numBSPStaffs = job.getNumBspStaff();
    this.profile = new JobProfile(job.getUser(), jobId, jobFile.toString(),
        job.getJobName());
    String jarFile = job.getJar();
    if (jarFile != null) {
      bspfs.copyToLocalFile(new BSPHdfsImpl().newPath(jarFile), localJarFile);
    }
    // chang by cui
    this.priority = job.getPriority();
    this.superStepNum = job.getNumSuperStep();
    this.status = new JobStatus(jobId, null, 0L, 0L, 0L,
        JobStatus.State.PREP.value(), (long) this.superStepNum);
    // new JobStatus(jobId, null, 0L, 0L, JobStatus.State.PREP.value());
    this.status.setUsername(job.getUser());
    this.status.setStartTime(startTime);
    // added by feng
    this.edgeNum = job.getOutgoingEdgeNum();
    setCheckPointFrequency();
    // For aggregation.
    /** Add the user program jar to the system's classpath. */
    ClassLoaderUtil.addClassPath(localJarFile.toString());
    loadAggregators();
    this.gssc = new GeneralSSController(jobId);
  }
  
  /**
   * JobInProgress construct method get the bspjob,jobid,controller staffnum and
   * staff locations information.
   * 
   * @param job
   *        BSP job
   * @param jobId
   *        BSP job id
   * @param controller
   *        BSP controller
   * @param staffNum
   *        staff num
   * @param locations
   *        staff location
   */
  public JobInProgress(BSPJob job, BSPJobID jobId, BSPController controller,
      int staffNum, HashMap<Integer, String[]> locations) {
    this.jobId = jobId;
    this.controller = controller;
    this.superStepCounter = 0;
    this.numBSPStaffs = staffNum;
    staffs = new StaffInProgress[locations.size()];
    for (int i = 0; i < this.numBSPStaffs; i++) {
      RawSplit split = new RawSplit();
      split.setLocations(locations.get(i));
      split.setClassName("yes");
      staffs[i] = new StaffInProgress(this.jobId, null, this.controller, null,
          this, i, split);
    }
    this.job = job;
    loadAggregators();
  }
  
  /**
   * For JUnit test.
   */
  public JobInProgress() {
    controller = new BSPController();
  }
  
  /**
   * load aggregators and aggregate values.
   */
  @SuppressWarnings("unchecked")
  private void loadAggregators() {
    int aggregateNum = this.job.getAggregateNum();
    String[] aggregateNames = this.job.getAggregateNames();
    for (int i = 0; i < aggregateNum; i++) {
      String name = aggregateNames[i];
      this.nameToAggregator.put(name, this.job.getAggregatorClass(name));
      this.nameToAggregateValue.put(name, job.getAggregateValueClass(name));
      this.aggregateValues.put(name, new ArrayList<AggregateValue>());
    }
  }
  
  /**
   * get the job super stepnum
   * 
   * @return job superstep num.
   */
  public int getSuperStepNum() {
    return this.superStepNum;
  }
  
  /**
   * get BSPcontroller
   * 
   * @return controller
   */
  public BSPController getController() {
    return controller;
  }
  
  /**
   * get BSP job
   * 
   * @return job
   */
  public BSPJob getJob() {
    return job;
  }
  
  /**
   * get a staff to differernt workermanaget times
   * 
   * @return staffToWMTimes
   */
  public HashMap<StaffAttemptID, Map<String, Integer>> getStaffToWMTimes() {
    return staffToWMTimes;
  }
  
  /**
   * get job profile
   * 
   * @return jobprofile
   */
  public JobProfile getProfile() {
    return profile;
  }
  
  /**
   * get BSP jobStatus
   * 
   * @return jobstatus
   */
  public JobStatus getStatus() {
    return status;
  }
  
  /**
   * BSPjob launch time.
   * 
   * @return launch time.
   */
  public synchronized long getLaunchTime() {
    return launchTime;
  }
  
  /**
   * get job start time
   * 
   * @return starttime
   */
  public long getStartTime() {
    return startTime;
  }
  
  /**
   * get job priority level
   * 
   * @return priority
   */
  public String getPriority() {
    return priority;
  }
  
  /**
   * get BSPstaff total num
   * 
   * @return numBSPstaffs
   */
  public int getNumBspStaff() {
    return numBSPStaffs;
  }
  
  /**
   * get staffInProgress inofrmation
   * 
   * @return staffs
   */
  public StaffInProgress[] getStaffInProgress() {
    return staffs;
  }
  
  /**
   * get job finish time
   * 
   * @return finish time
   */
  public long getFinishTime() {
    return finishTime;
  }
  
  /**
   * get fault Synchronization super step
   * 
   * @return faultSSStep
   */
  public int getFaultSSStep() {
    return faultSSStep;
  }
  
  /**
   * set fault Synchronization super step
   * 
   * @param faultSSStep
   *        superstep num to be set.
   */
  public void setFaultSSStep(int faultSSStep) {
    this.faultSSStep = faultSSStep;
  }
  
  /**
   * get GeneralSSController
   * 
   * @return gssc
   */
  public GeneralSSControllerInterface getGssc() {
    return gssc;
  }
  
  /**
   * If one task of this job is failed on this worker, then record the number of
   * failing to execute the job on the worker. If the failed number is more than
   * a threshold, then this worker is gray for the job. That means return
   * <code>true</code>, else return <code>false</code>.
   * 
   * @param wms
   *        workermanagerstatus of this worker
   * @return blacklist true,gray list false.
   */
  public boolean addFailedWorker(WorkerManagerStatus wms) {
    int counter = 1;
    if (this.failedRecord.containsKey(wms)) {
      counter = this.failedRecord.get(wms);
      counter++;
    }
    this.failedRecord.put(wms, counter);
    if (this.failedRecord.get(wms) > 2) {
      this.blackList.add(wms);
      LOG.warn("Warn: " + wms.getWorkerManagerName()
          + " is added into the BlackList of job " + this.jobId.toString()
          + " because the failed attempts is up to threshold:" + 2);
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * add worker to job blacklist
   * 
   * @param wms
   *        workerManagerstatus
   */
  public void addBlackListWorker(WorkerManagerStatus wms) {
    this.blackList.add(wms);
  }
  
  /**
   * @return the number of desired tasks.
   */
  public int desiredBSPStaffs() {
    return numBSPStaffs;
  }
  
  /**
   * @return The JobID of this JobInProgress.
   */
  public BSPJobID getJobID() {
    return jobId;
  }
  
  /**
   * find staffInProgress information with staffID
   * 
   * @param id
   *        staff id
   * @return if found return sip else return null
   */
  public synchronized StaffInProgress findStaffInProgress(StaffID id) {
    if (areStaffsInited()) {
      for (StaffInProgress sip : staffs) {
        if (sip.getStaffId().equals(id)) {
          return sip;
        }
      }
    }
    return null;
  }
  
  /**
   * get the job staffs initialized or not
   * 
   * @return staffsinitialized flag.
   */
  public synchronized boolean areStaffsInited() {
    return this.staffsInited;
  }
  
  /**
   * JobInProgress information toString
   * 
   * @return JobInProgress string information
   */
  public String toString() {
    return "jobName:" + profile.getJobName() + "\n" + "submit user:"
        + profile.getUser() + "\n" + "JobId:" + jobId + "\n" + "JobFile:"
        + jobFile + "\n";
  }
  
  /**
   * Create/manage tasks
   * 
   * @throws IOException
   *         exceptions during handle splitfiles
   */
  public void initStaffs() throws IOException {
    if (staffsInited) {
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("numBSPStaffs: " + numBSPStaffs);
    }
    // read the input split info from HDFS
    // Path sysDir = new Path(this.controller.getSystemDir());
    // FileSystem fs = sysDir.getFileSystem(conf);
    BSPFileSystem bspfs = new BSPFileSystemImpl(controller, conf);
    // DataInputStream splitFile = fs.open(new Path(conf
    // .get(Constants.USER_BC_BSP_JOB_SPLIT_FILE)));
    DataInputStream splitFile = bspfs.open(new BSPHdfsImpl().newPath(conf
        .get(Constants.USER_BC_BSP_JOB_SPLIT_FILE)));
    RawSplit[] splits;
    try {
      splits = BSPJobClient.readSplitFile(splitFile);
    } finally {
      splitFile.close();
    }
    // adjust number of map staffs to actual number of splits
    this.staffs = new StaffInProgress[numBSPStaffs];
    for (int i = 0; i < numBSPStaffs; i++) {
      if (i < splits.length) {
        // this staff will load data from DFS
        staffs[i] = new StaffInProgress(getJobID(), this.jobFile.toString(),
            this.controller, this.conf, this, i, splits[i]);
      } else {
        // create a disable split. this only happen in Hash.
        RawSplit split = new RawSplit();
        split.setClassName("no");
        split.setDataLength(0);
        split.setBytes("no".getBytes(), 0, 2);
        split.setLocations(new String[] {"no"});
        // this staff will not load data from DFS
        staffs[i] = new StaffInProgress(getJobID(), this.jobFile.toString(),
            this.controller, this.conf, this, i, split);
      }
    }
    // Update job status
    this.status.setRunState(JobStatus.RUNNING);
    this.status.setState(State.RUNNING);
    staffsInited = true;
    /* Zhicheng Liu added */
    this.staffSlowCount = new int[this.staffs.length];
    LOG.debug("Job is initialized.");
  }
  
  // 有 bug StaffToWMTimes 记录同一个作业在不同的机器上运行的次数，则不管该任务是否已加载数据
  // 都应该记录StaffToWMTimes否则故障恢复时就会有空指针异常
  /**
   * obtain the initialized staffs for the workers.
   * 
   * @param wmlist
   *        workermanagerStatus list
   * @param i
   *        staff num
   * @param staffsLoadFactor
   *        for load balance
   * @return staffInProgress information
   */
  public Staff obtainNewStaff(Collection<WorkerManagerStatus> wmlist, int i,
      double staffsLoadFactor) {
    WorkerManagerStatus[] wmss = (WorkerManagerStatus[]) wmlist
        .toArray(new WorkerManagerStatus[wmlist.size()]);
    // LOG.info(wmss[0] + "  " + wmss[1] + " staffsLoadFactor=" +
    // staffsLoadFactor);
    LOG.info("StaffID=" + staffs[i].getStaffID() + ":"
        + staffs[i].getRawSplit().getClassName());
    Staff result = null;
    try {
      if (!staffs[i].getRawSplit().getClassName().equals("no")) {
        // this staff need to load data according to the split info
        String[] locations = staffs[i].getRawSplit().getLocations();
        // LOG.info("staffs" + i +"   and locations size="+locations.length);
        int tmp_count = 0;
        int currentStaffs = 0;
        int maxStaffs = 0;
        int loadStaffs = 0;
        String tmp_location = locations[0];
        WorkerManagerStatus gss_tmp;
        for (String location : locations) {
          gss_tmp = findWorkerManagerStatus(wmss, location);
          if (gss_tmp == null) {
            continue;
          }
          // LOG.info("gss_tmp=" + gss_tmp);
          currentStaffs = gss_tmp.getRunningStaffsCount();
          maxStaffs = gss_tmp.getMaxStaffsCount();
          loadStaffs = Math.min(maxStaffs,
              (int) Math.ceil(staffsLoadFactor * maxStaffs));
          if ((loadStaffs - currentStaffs) > tmp_count) {
            tmp_count = loadStaffs - currentStaffs;
            tmp_location = location;
            // LOG.info("tmp_location=" + tmp_location);
          }
        }
        if (tmp_count > 0) {
          LOG.info("wmss=" + wmss + " tmp_location=" + tmp_location);
          WorkerManagerStatus status = findWorkerManagerStatus(wmss,
              tmp_location);
          result = staffs[i].getStaffToRun(status);
          // changed by chen for null bug
          // updateStaffToWMTimes(i, tmp_location);
        } else {
          result = staffs[i].getStaffToRun(findMaxFreeWorkerManagerStatus(wmss,
              staffsLoadFactor));
          // LOG.info("wmss=" + wmss + " tmp_location=" + tmp_location);
          // changed by chen for null bug
          // updateStaffToWMTimes(i, tmp_location);
        }
      } else {
        result = staffs[i].getStaffToRun(findMaxFreeWorkerManagerStatus(wmss,
            staffsLoadFactor));
      }
    } catch (IOException ioe) {
      LOG.error(
          "Exception has been catched in JobInProgress--obtainNewStaff !", ioe);
      Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, this.getJobID()
          .toString(), ioe.toString());
      this.getController().recordFault(f);
      this.getController().recovery(this.getJobID());
      try {
        this.getController().killJob(this.getJobID());
      } catch (IOException e) {
        // LOG.error("Kill Exception", e);
        throw new RuntimeException("Kill Exception", e);
      }
    }
    String name = staffs[i].getWorkerManagerStatus().getWorkerManagerName();
    LOG.info("obtainNewWorker--[Init]" + name);
    if (workersToStaffs.containsKey(name)) {
      // add by chen for null bug
      updateStaffToWMTimes(i, name);
      LOG.info("WorkerManagerName=" + name + " StaffAttemptId="
          + result.getStaffAttemptId());
      workersToStaffs.get(name).add(result.getStaffAttemptId());
      LOG.info("The workerName has already existed and add the staff directly");
    } else {
      // add by chen for null bug
      updateStaffToWMTimes(i, name);
      ArrayList<StaffAttemptID> list = new ArrayList<StaffAttemptID>();
      list.add(result.getStaffAttemptId());
      attemptIDList.add(result.getStaffAttemptId());
      workersToStaffs.put(name, list);
      LOG.info("WorkerManagerName=" + name + " StaffAttemptId="
          + result.getStaffAttemptId());
      LOG.info("Add the workerName " + name
          + " and the size of all workers is " + this.workersToStaffs.size());
    }
    return result;
  }
  
  /**
   * obtain new staffs for worker which contains recovery staff
   * 
   * @param wmss
   *        workermanager status will obtain
   * @param i
   *        staff num
   * @param tasksLoadFactor
   *        loadfactor for load balance
   * @param recovery
   *        staff recovery state
   */
  public void obtainNewStaff(WorkerManagerStatus[] wmss, int i,
      double tasksLoadFactor, boolean recovery) {
    staffs[i].setChangeWorkerState(true);
    LOG.info("obtainNewStaff" + "  " + recovery);
    try {
      staffs[i].getStaffToRun(findMaxFreeWorkerManagerStatus(wmss, 1.0), true);
    } catch (IOException ioe) {
      LOG.error(
          "Exception has been catched in JobInProgress--obtainNewStaff !", ioe);
      Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, this.getJobID()
          .toString(), ioe.toString());
      this.getController().recordFault(f);
      this.getController().recovery(this.getJobID());
      try {
        this.getController().killJob(job.getJobID());
      } catch (IOException e) {
        // LOG.error("IOException", e);
        throw new RuntimeException(" obtainNewStaff IOException", e);
      }
    }
    String name = staffs[i].getWorkerManagerStatus().getWorkerManagerName();
    LOG.info("obtainNewWorker--[recovery]" + name);
    if (workersToStaffs.containsKey(name)) {
      workersToStaffs.get(name).add(staffs[i].getS().getStaffAttemptId());
      LOG.info("The workerName has already existed and add the staff directly");
    } else {
      ArrayList<StaffAttemptID> list = new ArrayList<StaffAttemptID>();
      list.add(staffs[i].getS().getStaffAttemptId());
      workersToStaffs.put(name, list);
      LOG.info("Add the workerName " + name
          + " and the size of all workers is " + this.workersToStaffs.size());
    }
  }
  /**
   * biyahui added for obtaining staff locally
   * @param wmss
   * @param i
   * @param tasksLoadFactor
   * @param recovery
   * @param lastWMName
   */
  public void obtainNewStaffNew(WorkerManagerStatus[] wmss, int i,
	      double tasksLoadFactor, boolean recovery,String lastWMName) {
	    staffs[i].setChangeWorkerState(true);
	    LOG.info("obtainNewStaff" + "  " + recovery);
	    try {
	    	//biyahui added
	    	WorkerManagerStatus workMangerStatus=findWorkerManagerStatus(wmss,lastWMName);
	    	staffs[i].getStaffToRun(workMangerStatus, true);
	    } catch (IOException ioe) {
	      LOG.error(
	          "Exception has been catched in JobInProgress--obtainNewStaff !", ioe);
	      Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, this.getJobID()
	          .toString(), ioe.toString());
	      this.getController().recordFault(f);
	      this.getController().recovery(this.getJobID());
	      try {
	        this.getController().killJob(job.getJobID());
	      } catch (IOException e) {
	        throw new RuntimeException(" obtainNewStaff IOException", e);
	      }
	    }
	    String name = staffs[i].getWorkerManagerStatus().getWorkerManagerName();
	    LOG.info("obtainNewWorker--[recovery]" + name);
	    if (workersToStaffs.containsKey(name)) {
	      workersToStaffs.get(name).add(staffs[i].getS().getStaffAttemptId());
	      LOG.info("The workerName has already existed and add the staff directly");
	    } else {
	      ArrayList<StaffAttemptID> list = new ArrayList<StaffAttemptID>();
	      list.add(staffs[i].getS().getStaffAttemptId());
	      workersToStaffs.put(name, list);
	      LOG.info("Add the workerName " + name
	          + " and the size of all workers is " + this.workersToStaffs.size());
	    }
	  }
  
  /**
   * update staffs assign to workers information.
   * 
   * @param i
   *        staff num in staff array
   * @param WorkerManagerName
   *        workermanager name to added in the workerManagertotimes list.
   */
  private void updateStaffToWMTimes(int i, String WorkerManagerName) {
    if (staffToWMTimes.containsKey(staffs[i].getStaffID())) {
      Map<String, Integer> workerManagerToTimes = staffToWMTimes.get(staffs[i]
          .getStaffID());
      int runTimes = 0;
      if (workerManagerToTimes.containsKey(WorkerManagerName)) {
        runTimes = workerManagerToTimes.get(WorkerManagerName) + 1;
        workerManagerToTimes.remove(WorkerManagerName);
        workerManagerToTimes.put(WorkerManagerName, runTimes);
      } else {
        workerManagerToTimes.put(WorkerManagerName, 1);
      }
      staffToWMTimes.remove(staffs[i].getStaffID());
      staffToWMTimes.put(staffs[i].getStaffID(), workerManagerToTimes);
    } else {
      Map<String, Integer> workerManagerToTimes = new LinkedHashMap<String, Integer>();
      workerManagerToTimes.put(WorkerManagerName, 1);
      staffToWMTimes.put(staffs[i].getStaffID(), workerManagerToTimes);
    }
    LOG.info("updateStaffToWMTimes---staffId: " + staffs[i].getStaffID()
        + " StaffWMTimes: " + staffToWMTimes);
  }
  
  /**
   * Find the WorkerManagerStatus according to the WorkerManager name
   * 
   * @param wss
   *        workermanagerstatus list
   * @param name
   *        workermanager name to find.
   * @return if find the specific name return the workermanager status not find
   *         return null.
   */
  public WorkerManagerStatus findWorkerManagerStatus(WorkerManagerStatus[] wss,
      String name) {
    for (WorkerManagerStatus e : wss) {
      if (this.blackList.contains(e)) {
        continue;
      }
      if (e.getWorkerManagerName().indexOf(name) != -1) {
        return e;
      }
    }
    return null;
  }
  
  /**
   * find the most free worker to assign the new staff.
   * 
   * @param wss
   *        workermanager list
   * @param staffsLoadFactor
   *        locd factor to find the most free worker
   * @return return the found workerstatus
   */
  public WorkerManagerStatus findMaxFreeWorkerManagerStatus(
      WorkerManagerStatus[] wss, double staffsLoadFactor) {
    int currentStaffs = 0;
    int maxStaffs = 0;
    int loadStaffs = 0;
    int tmp_count = 0;
    WorkerManagerStatus status = null;
    for (WorkerManagerStatus wss_tmp : wss) {
      if (this.blackList.contains(wss_tmp)) {
        continue;
      }
      currentStaffs = wss_tmp.getRunningStaffsCount();
      maxStaffs = wss_tmp.getMaxStaffsCount();
      loadStaffs = Math.min(maxStaffs,
          (int) Math.ceil(staffsLoadFactor * maxStaffs));
      if ((loadStaffs - currentStaffs) > tmp_count) {
        tmp_count = loadStaffs - currentStaffs;
        status = wss_tmp;
      }
    }
    return status;
  }
  
  /**
   * update the staff status
   * 
   * @param sip
   *        staffInprogress information
   * @param staffStatus
   *        staff status to update
   */
  public synchronized void updateStaffStatus(StaffInProgress sip,
      StaffStatus staffStatus) {
    sip.updateStatus(staffStatus);
    if (superStepCounter < staffStatus.getSuperstepCount()) {
      superStepCounter = (int) staffStatus.getSuperstepCount();
    }
  }
  
  /**
   * update staff status in job status
   * 
   * @param status
   *        staff status to update
   */
  public synchronized void updateStaffs(StaffStatus status) {
    this.status.updateStaffStatus(status);
  }
  
  /**
   * set fault staff super step counter
   */
  public void setAttemptRecoveryCounter() {
    this.attemptRecoveryCounter++;
    this.setFaultSSStep(superStepCounter);
  }
  
  /**
   * get the job attempt recovery times
   * 
   * @return attempt to recovery times
   */
  public int getNumAttemptRecovery() {
    return this.attemptRecoveryCounter;
  }
  
  /**
   * get the job max attempt to recovery counter
   * 
   * @return recovery counter
   */
  public int getMaxAttemptRecoveryCounter() {
    return maxAttemptRecoveryCounter;
  }
  
  /**
   * get the staff max attempt recvovery counter
   * 
   * @return staff max recovery counter
   */
  public int getMaxStaffAttemptRecoveryCounter() {
    return maxStaffAttemptRecoveryCounter;
  }
  
  /**
   * set the job priority
   * 
   * @param priority
   *        job priority to set
   */
  public void setPriority(String priority) {
    this.priority = priority;
  }
  
  /**
   * get the job checkpoint frequency from conf
   */
  public void setCheckPointFrequency() {
    int defaultF = conf.getInt(
        Constants.DEFAULT_BC_BSP_JOB_CHECKPOINT_FREQUENCY, 0);
    if (defaultF == 0) {
      this.checkPointFrequency = defaultF;
    } else {
      this.checkPointFrequency = conf.getInt(
          Constants.USER_BC_BSP_JOB_CHECKPOINT_FREQUENCY, defaultF);
    }
  }
  
  /**
   * set the job checkpoint frequency for a certain num.
   * 
   * @param cpf
   *        check point frequency num to be set
   */
  public void setCheckPointFrequency(int cpf) {
    this.checkPointFrequency = cpf;
    LOG.info("The current [CheckPointFrequency] is:" + this.checkPointFrequency);
  }
  
  /**
   * set next checkpoint command.
   */
  public void setCheckPointNext() {
    this.checkPointNext = true;
    LOG.info("The next superstep [" + (this.superStepCounter) + " or "
        + (this.superStepCounter + 1) + "] will execute checkpoint operation");
  }
  
  /**
   * get the checkpoint frequency
   * 
   * @return checkpoint frequency
   */
  public int getCheckPointFrequency() {
    return this.checkPointFrequency;
  }
  
  /**
   * if the job is in checkpoint mode,whether the next checkpoint is true and
   * the checkpoint frequency>0
   * 
   * @return in or not checkpoint
   */
  public boolean isCheckPoint() {
    if (this.checkPointFrequency == 0 || this.superStepCounter == 0) {
      return false;
    }
    if (this.checkPointNext) {
      return true;
    }
    if ((this.superStepCounter % this.checkPointFrequency) == 0) {
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * if job status is recovery
   * 
   * @return recovery true,not false.
   */
  public boolean isRecovery() {
    return this.status.getRunState() == JobStatus.RECOVERY;
  }
  
  @Override
  public void setAbleCheckPoint(int ableCheckPoint) {
    this.ableCheckPoint = ableCheckPoint;
    LOG.info("The ableCheckPoint is " + this.ableCheckPoint);
  }
  
  // Note: Client get the progress by this.status
  @Override
  public void setSuperStepCounter(int superStepCounter) {
    this.superStepCounter = superStepCounter;
    this.status.setprogress(this.superStepCounter + 1);
  }
  
  @Override
  public int getSuperStepCounter() {
    return this.superStepCounter;
  }
  
  /**
   * get aggregate result from superstepcontainer
   * 
   * @param ssrcs
   *        contains the aggregate values after a super step
   * @return aggregate values from all superstepreportcontainers
   */
  @SuppressWarnings("unchecked")
  public String[] generalAggregate(SuperStepReportContainer[] ssrcs) {
    String[] results = null;
    if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(job.get(
        Constants.USER_BC_BSP_JOB_TYPE, ""))) {
      LOG.info("[generalAggregate]: jobtype is c++");
      if (null == this.application) {
        try {
          LOG.info("jobC path is: " + job.getJobExe());
          LOG.info("BSPController localDir is : " + controller.getLocalDirs());
          LOG.info("BSP.local.dir is: " + conf.get("bcbsp.local.dir"));
          LOG.info("localJarFile is" + localJarFile);
          LOG.info("jobID is :" + this.getJobID());
          Path jobC = new Path(conf.get("bcbsp.local.dir")
              + "/controller/jobC_" + this.getJobID());
          String jarFile = job.getJobExe();
          String dir = controller.getSystemDir();
          // Path systemDirectory=new Path(dir);
          // alter by gtt
          // FileSystem systemFS = systemDirectory.getFileSystem(conf);
          BSPFileSystem bspsystemFS = new BSPFileSystemImpl(dir, conf);
          bspsystemFS.copyToLocalFile(new Path(jarFile), jobC);
          this.application = new Application(job, "WorkerManager",
              jobC.toString());
          // LOG.info("new application");
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          // LOG.info("bspcontroller catch exception while new application" +
          // e1);
          throw new RuntimeException("bspcontroller catch"
              + "exception while new application", e1);
          // e1.printStackTrace();
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          throw new RuntimeException("bspcontroller catch"
              + "exception while new application", e1);
        }
      } else {
        LOG.info("application is not null");
      }
      for (int i = 0; i < ssrcs.length; i++) {
        String[] aggValues = ssrcs[i].getAggValues();
        if (null != aggValues) {
          // for (int j = 0; j < aggValues.length; j++) {
          // LOG.info("aggValue is: " + aggValues[j]);
          // }
          try {
            application.getDownlink().sendNewAggregateValue(aggValues);
          } catch (IOException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("send aggregateValue Exception! ", e);
          } catch (Throwable e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("send aggregateValue Exception! ", e);
          }
        }
        // results=aggValues;
      }
      LOG.info("before superstep");
      try {
        application.getDownlink().runASuperStep();
        LOG.info("after superstep");
        LOG.info("wait for superstep  to finish");
        this.application.waitForFinish();
        ArrayList<String> aggregateValue = null;
        aggregateValue = this.application.getHandler().getAggregateValue();
        LOG.info("aggregateValue size is: " + aggregateValue.size());
        results = aggregateValue.toArray(new String[0]);
        for (int k = 0; k < results.length; k++) {
          LOG.info("aggregate results is: " + results[k]);
        }
        this.application.getHandler().getAggregateValue().clear();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException("get aggvalue Exception! ", e);
      } catch (Throwable e) {
        // TODO Auto-generated catch block
        throw new RuntimeException("get aggvalue Exception! ", e);
      }
    } else {
      // To get the aggregation values from the ssrcs.
      for (int i = 0; i < ssrcs.length; i++) {
        String[] aggValues = ssrcs[i].getAggValues();
        for (int j = 0; j < aggValues.length; j++) {
          String[] aggValueRecord = aggValues[j].split(Constants.KV_SPLIT_FLAG);
          String aggName = aggValueRecord[0];
          String aggValueString = aggValueRecord[1];
          AggregateValue aggValue = null;
          try {
            aggValue = this.nameToAggregateValue.get(aggName).newInstance();
            aggValue.initValue(aggValueString); // init the aggValue from its
                                                // string form.
          } catch (InstantiationException e1) {
            // LOG.error("InstantiationException", e1);
            throw new RuntimeException("InstantiationException", e1);
          } catch (IllegalAccessException e2) {
            // LOG.error("IllegalAccessException", e2);
            throw new RuntimeException("IllegalAccessException", e2);
          } // end-try
          if (aggValue != null) {
            ArrayList<AggregateValue> list = this.aggregateValues.get(aggName);
            list.add(aggValue); // put the value to the values' list for
                                // aggregation ahead.
          } // end-if
        } // end-for
      } // end-for
      // To aggregate the values from the aggregateValues.
      this.aggregateResults.clear(); // Clear the results' container before a
                                     // new
                                     // calculation.
      // To calculate the aggregations.
      for (Entry<String, Class<? extends Aggregator<?>>> entry : this.nameToAggregator
          .entrySet()) {
        Aggregator<AggregateValue> aggregator = null;
        try {
          aggregator = (Aggregator<AggregateValue>) entry.getValue()
              .newInstance();
        } catch (InstantiationException e1) {
          // LOG.error("InstantiationException", e1);
          throw new RuntimeException("InstantiationException", e1);
        } catch (IllegalAccessException e2) {
          // LOG.error("IllegalAccessException", e2);
          throw new RuntimeException("IllegalAccessException", e2);
        }
        if (aggregator != null) {
          ArrayList<AggregateValue> aggVals = this.aggregateValues.get(entry
              .getKey());
          AggregateValue resultValue = aggregator.aggregate(aggVals);
          this.aggregateResults.put(entry.getKey(), resultValue);
          aggVals.clear(); // Clear the initial aggregate values after
                           // aggregation completes.
        }
      } // end-for
      /**
       * To encapsulate the aggregation values to the String[] results. The
       * aggValues should be in form as follows: [ AggregateName \t
       * AggregateValue.toString() ]
       */
      int aggSize = this.aggregateResults.size();
      results = new String[aggSize];
      int i_a = 0;
      for (Entry<String, AggregateValue> entry : this.aggregateResults
          .entrySet()) {
        results[i_a] = entry.getKey() + Constants.KV_SPLIT_FLAG
            + entry.getValue().toString();
        i_a++;
      }
    }
    return results;
  }
  
  /*
   * Review suggestion: allow user to determine whether to use load balance and
   * other configure information Zhicheng Liu 2013/10/9
   */
  @Override
  public SuperStepCommand generateCommand(SuperStepReportContainer[] ssrcs) {
    /* Zhicheng Liu added */
    String migrateStaffIDs = "";
    this.openMigrateMode = this.conf.get("bcbsp.loadbalance", "false").equals(
        "true") ? true : false;
    if (openMigrateMode && !isRecovery()) { // Find the slow staffs to migrate
      migrateStaffIDs = this.detectShortBoardStaffs(ssrcs);
      if (!migrateStaffIDs.equals("")) {
        LOG.info("staff " + migrateStaffIDs + "should be migrated! ");
        // Schedule new staff(using same staff id)
        this.migrateFlag = true;
        String[] ids = migrateStaffIDs.split(Constants.SPLIT_FLAG);
        for (int i = 0; i < ids.length; i++) {
          StaffInProgress sip = getStaff(Integer.parseInt(ids[i]));
          StaffStatus ss = sip.getStaffStatus(sip.getStaffID());
          controller.deleteOldStaffs(sip.getWorkerManagerStatus()
              .getWorkerManagerName(), ss); // Update the globle information
          this.migrateStaff(Integer.parseInt(ids[i])); // migrate staff
        }
      }
    }
    SuperStepCommand ssc = new SuperStepCommand();
    // Note: we must firstly judge whether the fault has happened.
    LOG.info("[generateCommand]---this.status.getRunState()"
        + this.status.getRunState());
    LOG.info("[generateCommand]---this.status.isRecovery()"
        + this.status.isRecovery());
    if (isRecovery()) {
      HashMap<Integer, String> partitionToWorkerManagerNameAndPort = convert();
      LOG.info("if (isRecovery())--partitionToWorkerManagerName :"
          + partitionToWorkerManagerNameAndPort);
      ssc.setCommandType(Constants.COMMAND_TYPE.START_AND_RECOVERY);
      ssc.setInitReadPath(conf.get(Constants.BC_BSP_CHECKPOINT_WRITEPATH) + "/"
          + this.jobId.toString() + "/" + this.ableCheckPoint);
      LOG.info("ableCheckPoint: " + ableCheckPoint);
      ssc.setAbleCheckPoint(this.ableCheckPoint);
      ssc.setNextSuperStepNum(this.ableCheckPoint + 1);
      // If the COMMAND_TYPE is START_AND_RECOVERY, then the
      // ssc.setPartitionToWorkerManagerName must be invoked.
      ssc.setPartitionToWorkerManagerNameAndPort(partitionToWorkerManagerNameAndPort);
      LOG.info("end--ssc.setPartitionToWorkerManagerName("
          + "partitionToWorkerManagerName);");
      this.status.setRunState(JobStatus.RUNNING);
      this.status.setState(State.RUNNING);
      return ssc;
    }
    String[] aggValues = generalAggregate(ssrcs); // To aggregate from the
                                                  // ssrcs.
    ssc.setAggValues(aggValues); // To put the aggregation result values into
                                 // the ssc.
    long counter = 0;
    for (int i = 0; i < ssrcs.length; i++) {
      if (ssrcs[i].getJudgeFlag() > 0) {
        counter += ssrcs[i].getJudgeFlag();
      }
    }
    if (counter > 0) {
      StringBuffer sb = new StringBuffer("[Active]" + counter);
      for (int i = 0; i < aggValues.length; i++) {
        sb.append("  ||  [AGG" + (i + 1) + "]" + aggValues[i]);
      }
      LOG.info("STATISTICS DATA : " + sb.toString());
      if (isCheckPoint()) {
        this.checkPointNext = false;
        ssc.setOldCheckPoint(this.ableCheckPoint);
        LOG.info("jip--ableCheckPoint: " + this.ableCheckPoint);
        ssc.setCommandType(Constants.COMMAND_TYPE.START_AND_CHECKPOINT);
        ssc.setInitWritePath(conf.get(Constants.BC_BSP_CHECKPOINT_WRITEPATH)
            + "/" + this.jobId.toString() + "/" + this.superStepCounter);
        ssc.setAbleCheckPoint(this.superStepCounter);
        ssc.setNextSuperStepNum(this.superStepCounter + 1);
        /* Zhicheng Liu added */
        if (this.openMigrateMode && this.migrateFlag) {
          ssc.setMigrateStaffIDs(migrateStaffIDs);
          HashMap<Integer, String> partitionToWorkerManagerNameAndPort = convert();
          LOG.info("if (isMigrate())--partitionToWorkerManagerName :"
              + partitionToWorkerManagerNameAndPort);
          ssc.setPartitionToWorkerManagerNameAndPort(partitionToWorkerManagerNameAndPort);
          LOG.info("end--ssc.setPartitionToWorkerManagerName("
              + "partitionToWorkerManagerName);");
          this.migrateFlag = false;
        }
      } else {
        ssc.setCommandType(Constants.COMMAND_TYPE.START);
        ssc.setNextSuperStepNum(this.superStepCounter + 1);
        /* Zhicheng Liu added */
        if (this.openMigrateMode && this.migrateFlag) {
          ssc.setMigrateStaffIDs(migrateStaffIDs);
          HashMap<Integer, String> partitionToWorkerManagerNameAndPort = convert();
          LOG.info("if (isMigrate())--partitionToWorkerManagerName :"
              + partitionToWorkerManagerNameAndPort);
          ssc.setPartitionToWorkerManagerNameAndPort(partitionToWorkerManagerNameAndPort);
          LOG.info("end--ssc.setPartitionToWorkerManagerName("
              + "partitionToWorkerManagerName);");
          this.migrateFlag = false;
        }
      }
    } else {
      ssc.setCommandType(Constants.COMMAND_TYPE.STOP);
      ssc.setNextSuperStepNum(this.superStepCounter);
    }
    return ssc;
  }
  
  /**
   * convert the workertoStaffs information and partition to staffs information
   * into partitionToWorkerManagerNameAndPort,convert for SScommand
   * 
   * @return partitionToWorkerManagerNameAndPort information
   */
  private HashMap<Integer, String> convert() {
    StaffInProgress[] staffs = this.getStaffInProgress();
    HashMap<String, ArrayList<StaffAttemptID>> workersToStaffs = this
        .getWorkersToStaffs();
    HashMap<Integer, String> partitionToWorkerManagerNameAndPort = new HashMap<Integer, String>();
    ArrayList<StaffAttemptID> staffAttemptIDs = null;
    StaffAttemptID staffAttemptID = null;
    for (String workerManagerName : workersToStaffs.keySet()) {
      staffAttemptIDs = workersToStaffs.get(workerManagerName);
      for (int i = 0; i < staffAttemptIDs.size(); i++) {
        staffAttemptID = staffAttemptIDs.get(i);
        for (int j = 0; j < staffs.length; j++) {
          if (staffAttemptID.equals(staffs[j].getStaffID())) {
            partitionToWorkerManagerNameAndPort.put(staffs[j].getS()
                .getPartition(), workerManagerName);
          }
        }
      }
    }
    return partitionToWorkerManagerNameAndPort;
  }
  
  /**
   * get workertostaffs information
   * 
   * @return workersToStaffs
   */
  public HashMap<String, ArrayList<StaffAttemptID>> getWorkersToStaffs() {
    return this.workersToStaffs;
  }
  
  /**
   * remove a spcific staff from the worker
   * 
   * @param workerName
   *        remove the staff from the workername worker
   * @param staffId
   *        staffId to remove
   * @return remove result
   */
  public boolean removeStaffFromWorker(String workerName, StaffAttemptID staffId) {
    boolean success = false;
    if (this.workersToStaffs.containsKey(workerName)) {
      if (this.workersToStaffs.get(workerName).contains(staffId)) {
        this.workersToStaffs.get(workerName).remove(staffId);
        if (this.workersToStaffs.get(workerName).size() == 0) {
          this.workersToStaffs.remove(workerName);
          LOG.info("removeStaffFromWorker " + workerName);
        }
        success = true;
      }
    }
    return success;
  }
  
  /**
   * The job is dead. We're now GC'ing it, getting rid of the job from all
   * tables. Be sure to remove all of this job's tasks from the various tables.
   */
  private void garbageCollect() {
    try {
      // Cleanup the ZooKeeper.
      gssc.cleanup();
      // Cleanup the local file.
      if (localJobFile != null) {
        BSPlocalFs.delete(localJobFile, true);
        localJobFile = null;
      }
      if (localJarFile != null) {
        BSPlocalFs.delete(localJarFile, true);
        localJarFile = null;
      }
      // clean up hdfs
      // FileSystem fs = FileSystem.get(conf);
      // fs.delete(new Path(profile.getJobFile()).getParent(), true);
      BSPFileSystem bspfs = new BSPFileSystemImpl(conf);
      bspfs.delete(new BSPHdfsImpl().newPath(profile.getJobFile()).getParent(),
          true);
      // clean some ha log
      if (this.jobId.getId() % 5 == 0) {
        cleanHaLog();
      }
    } catch (Exception e) {
      // LOG.error("[garbageCollect> Error cleaning up]" + e.getMessage());
      throw new RuntimeException("[garbageCollect> Error cleaning up]", e);
    }
  }
  
  @Override
  public void completedJob() {
    this.status.setRunState(JobStatus.SUCCEEDED);
    this.status.setState(State.SUCCEEDED);
    this.status.setprogress(this.superStepCounter + 1);
    this.finishTime = System.currentTimeMillis();
    this.status.setFinishTime(this.finishTime);
    this.controller.removeFromJobListener(this.jobId);
    cleanCheckpoint();
    garbageCollect();
    LOG.info("Job successfully done.");
    updCountersTJobs();
  }
  
  /**
   * update the job counters status
   * 
   * @author cui
   */
  public void updCountersTJobs() {
    for (Group group : this.counters) {
      for (Counter counter : group) {
        LOG.info(counter.getDisplayName() + "=" + counter.getCounter());
        this.mapcounterTemp.put(new Text(counter.getDisplayName()),
            new LongWritable(counter.getCounter()));
      }
    }
    this.status.setMapcounter(mapcounterTemp);
  }
  
  @Override
  public void failedJob() {
    this.status.setRunState(JobStatus.FAILED);
    this.status.setState(State.FAILED);
    this.status.setprogress(this.superStepCounter + 1);
    this.finishTime = System.currentTimeMillis();
    this.status.setFinishTime(this.finishTime);
    this.controller.removeFromJobListener(jobId);
    gssc.stop();
    cleanCheckpoint();
    garbageCollect();
    LOG.warn("Job failed.");
  }
  
  /**
   * kill the job.
   */
  public void killJob() {
    this.status.setRunState(JobStatus.KILLED);
    this.status.setState(State.KILLED);
    this.status.setprogress(this.superStepCounter + 1);
    this.finishTime = System.currentTimeMillis();
    this.status.setFinishTime(this.finishTime);
    for (int i = 0; i < staffs.length; i++) {
      staffs[i].kill();
    }
    this.controller.removeFromJobListener(jobId);
    gssc.stop();
    cleanCheckpoint();
    garbageCollect();
  }
  
  /**
   * for kill the job rapidly for test
   * 
   * @author chen
   */
  public void killJobRapid() {
    // this.status.setRunState(JobStatus.KILLED);
    this.status.setRunState(JobStatus.RUNNING);
    this.status.setState(State.KILLED);
    this.status.setprogress(this.superStepCounter);
    this.finishTime = System.currentTimeMillis();
    this.status.setFinishTime(this.finishTime);
    this.controller.removeFromJobListener(jobId);
    gssc.stop();
    cleanCheckpoint();
  }
  
  /**
   * clean checkpoint and delete the checkpoint information
   * 
   * @return chean result.
   */
  private boolean cleanCheckpoint() {
    if (job.getCheckpointType().equals("HBase")) {
      String tableName = null;
      Configuration conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.property.clientPort",
          job.getConf().get(Constants.ZOOKEPER_CLIENT_PORT));
      conf.set("hbase.zookeeper.quorum", "master");
      conf.set("hbase.master", "master:60000");
      HBaseAdmin admin;
      try {
        admin = new HBaseAdmin(conf);
        for (int i = 0; i < this.attemptIDList.size(); i++) {
          tableName = this.attemptIDList.get(i).toString();
          if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
          }
        }
        admin.close();
        return true;
      } catch (MasterNotRunningException e) {
        LOG.error("Exception has happened and been catched!", e);
        e.printStackTrace();
      } catch (ZooKeeperConnectionException e) {
        LOG.error("Exception has happened and been catched!", e);
        e.printStackTrace();
      } catch (IOException e) {
        LOG.error("Exception has happened and been catched!", e);
        e.printStackTrace();
      }
      return false;
    } else if (job.getCheckpointType().equals("HDFS")) {
      try {
        String uri = conf.get(Constants.BC_BSP_CHECKPOINT_WRITEPATH) + "/"
            + job.getJobID().toString() + "/";
        // FileSystem fs = FileSystem.get(URI.create(uri), conf);
        BSPFileSystem bspfs = new BSPFileSystemImpl(URI.create(uri), conf);
        // if(fs.exists(new Path(uri))) {
        // fs.delete(new Path(uri), true);
        // }
        if (bspfs.exists(new BSPHdfsImpl().newPath(uri))) {
          bspfs.delete(new BSPHdfsImpl().newPath(uri), true);
        }
        return true;
      } catch (IOException e) {
        LOG.error("Exception has happened and been catched!", e);
        return false;
      }
    }
    return false;
  }
  
  @Override
  public int getCheckNum() {
    // return this.workersToStaffs.size();
    //ljn add  for migrate staff change the number of worker.
    HashSet<String> workersAfter = new HashSet<String>();
    workersAfter.addAll(convert().values());
    if (workersAfter.size() == this.workersToStaffs.size()) {
      return this.workersToStaffs.size();
    } else {
      return workersAfter.size();
    }
  }
  
  @Override
  public void reportLOG(String log) {
    LOG.info("GeneralSSController: " + log);
  }
  
  /**
   * get the staffId list.
   * 
   * @return staffattemptID array.
   */
  public StaffAttemptID[] getAttemptIDList() {
    return attemptIDList.toArray(new StaffAttemptID[attemptIDList.size()]);
  }
  
  /**
   * get the recovery barrier of workers for job recovery.
   * 
   * @param WMNames
   *        workermanagername list.
   */
  public void getRecoveryBarrier(List<String> WMNames) {
    gssc.recoveryBarrier(WMNames);
  }
  
  /**
   * Only for fault-tolerance. If the command has been write on the ZooKeeper,
   * return true, else return false.
   * 
   * @return command write result.
   */
  public boolean isCommandBarrier() {
    return this.gssc.isCommandBarrier();
  }
  
  /**
   * get jobs workermanager names list.
   * 
   * @return workermanager names list
   */
  public List<String> getWMNames() {
    return WMNames;
  }
  
  /**
   * add workermanager name into list.
   * 
   * @param name
   *        workermanager name to be added.
   */
  public void addWMNames(String name) {
    WMNames.add(name);
  }
  
  /**
   * clean the workermanagernames list.
   */
  public void cleanWMNames() {
    this.WMNames.clear();
  }
  
  /**
   * clean the jobs information that are already finished and write the other
   * jobs information back for HA.
   */
  public void cleanHaLog() {
    try {
      HDFSOperator haLogOperator = new HDFSOperator();
      // delete workerManagerStatus
      haLogOperator.deleteFile(conf.get(Constants.BC_BSP_HA_LOG_DIR)
          + this.getJobID().toString());
      // clean submit log
      Map<BSPJobID, String> usingJobs = new ConcurrentHashMap<BSPJobID, String>();
      FSDataInputStream in = haLogOperator.readFile(conf
          .get(Constants.BC_BSP_HA_LOG_DIR) + Constants.BC_BSP_HA_SUBMIT_LOG);
      while (in != null) {
        try {
          // LOG.info("in first while");
          String line = in.readUTF();
          String[] temp = line.split("&");
          if (haLogOperator.isExist(temp[1])) {
            usingJobs.put(new BSPJobID().forName(temp[0]), temp[1]);
          }
        } catch (EOFException e) {
          in = null;
        }
      }
      // haLogOperator.closeFs();
      haLogOperator.createFile(conf.get(Constants.BC_BSP_HA_LOG_DIR)
          + Constants.BC_BSP_HA_SUBMIT_LOG);
      String jobFile = null;
      for (BSPJobID jobid : usingJobs.keySet()) {
        // LOG.info("refresh the submitLog jobid=" + jobid );
        jobFile = usingJobs.get(jobid);
        haLogOperator.writeFile(jobid.toString() + "&" + jobFile,
            conf.get(Constants.BC_BSP_HA_LOG_DIR)
                + Constants.BC_BSP_HA_SUBMIT_LOG);
      }
      // clean operate Log
      LOG.info("clean operate Log");
      try {
        in = haLogOperator.readFile(conf.get(Constants.BC_BSP_HA_LOG_DIR)
            + Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
        while (in != null) {
          try {
            // LOG.info("in second while");
            String line = in.readUTF();
            String[] temp = line.split("&");
            BSPJobID jobid = new BSPJobID().forName(temp[0]);
            if (usingJobs.containsKey(jobid)) {
              usingJobs.remove(jobid);
              usingJobs.put(jobid, temp[1]);
            }
          } catch (EOFException e) {
            in = null;
          }
        }
        String queueName = "";
        LOG.info("before create operateLOG");
        haLogOperator.createFile(conf.get(Constants.BC_BSP_HA_LOG_DIR)
            + Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
        for (BSPJobID jobid : usingJobs.keySet()) {
          queueName = usingJobs.get(jobid);
          if (!"".equals(queueName)) {
            LOG.info("before write operateLOG");
            haLogOperator.writeFile(jobid.toString() + "&" + queueName,
                conf.get(Constants.BC_BSP_HA_LOG_DIR)
                    + Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
          }
        }
      } catch (IOException e) {
        // do nothing;
      }
    } catch (IOException e) {
      // do nothing
      // LOG.info("you can ignore this exception " + e.getMessage());
    }
  }
  
  /**
   * get the job counters
   * 
   * @return counters
   */
  public Counters getCounters() {
    return counters;
  }
  
  @Override
  public void setCounters(Counters counters) {
    this.counters = counters;
  }
  
  /**
   * get detect the job slow staffs to migrate
   * 
   * @param ssrcs
   *        superstepreportcontainer array
   * @return slow staffs id to migrate.
   */
  private String detectShortBoardStaffs(SuperStepReportContainer[] ssrcs) {
    StaffMigrate[] runStaffs = getStaffMigrate(ssrcs);
    // Update controller for predict
    for (int i = 0; i < runStaffs.length; i++) {
      this.updateStaffSSRunTime(runStaffs[i].getStaffID(),
          runStaffs[i].getStaffRunTime());
    }
    // this.controller.test();
    Set<Integer> slowStaffs = getSlowStaffIDs(runStaffs);
    Iterator<Integer> im = slowStaffs.iterator();
    String ids = "";
    while (im.hasNext()) {
      ids += (im.next() + Constants.SPLIT_FLAG);
    }
    return ids;
  }
  
  /**
   * Staff struct for migrate
   * 
   * @author liuzhicheng
   */
  private class StaffMigrate {
    /** staffID to migrate */
    private int staffID;
    /** migrate staff runtime */
    private long runTime;
    /** migrate staff current super step */
    private int currSuperStep;
    /** staff migrate cost */
    private long migCost;
    
    /**
     * staff migrate construct method.
     * 
     * @param id
     *        staffID
     * @param currSS
     *        staff current superstep
     * @param runT
     *        staff runtime
     * @param mCost
     *        staff migrate cost.
     */
    private StaffMigrate(int id, int currSS, long runT, long mCost) {
      staffID = id;
      runTime = runT;
      currSuperStep = currSS;
      migCost = mCost;
    }
    
    /**
     * get the migrate staffID
     * 
     * @return staffId
     */
    private int getStaffID() {
      return staffID;
    }
    
    /**
     * get staff run time
     * 
     * @return runtime.
     */
    private long getStaffRunTime() {
      return runTime;
    }
    
    /**
     * get staff current superstep
     * 
     * @return current superstep.
     */
    private int getCurrentSuperStep() {
      return currSuperStep;
    }
    
    /**
     * get the staff migrate cost.
     * 
     * @return migrate cost.
     */
    private long getMigrateCost() {
      return migCost;
    }
  }
  
  /**
   * get the staffs may need migrate from superstepreport container
   * 
   * @param ssrcs
   *        superstepreportcontainer
   * @return migrate staffs
   */
  private StaffMigrate[] getStaffMigrate(SuperStepReportContainer[] ssrcs) {
    StaffMigrate[] runStaffs = new StaffMigrate[job.getNumBspStaff()];
    int index = 0;
    String migrateInfo = "";
    for (int i = 0; i < ssrcs.length; i++) {
      migrateInfo = ssrcs[i].getMigrateInfo();
      String[] infos = migrateInfo.split(Constants.MIGRATE_SPLIT);
      LOG.info("ssrc.worker is " + ssrcs[i].getMigrateInfo());
      for (int j = 0; j < infos.length; j++) {
        String[] staffMigrateInfo = infos[j].split(Constants.SPLIT_FLAG);
        runStaffs[index] = new StaffMigrate(
            Integer.parseInt(staffMigrateInfo[0]),
            Integer.parseInt(staffMigrateInfo[1]),
            Long.parseLong(staffMigrateInfo[2]),
            Long.parseLong(staffMigrateInfo[3]));
        index++;
      }
    }
    return runStaffs;
  }
  
  /**
   * Strategy for finding slow staff(note: the number of staffs is 5 at least!)
   * This strategy can be changed
   * 
   * @param runStaffs
   *        runstaffs on workermanager
   * @return slowstaffs id
   */
  private Set<Integer> getSlowStaffIDs(StaffMigrate[] runStaffs) {
    // int ID = 5;//Feng added for test
    StaffMigrate tmp;
    // Bubble sort
    for (int i = 0; i < runStaffs.length - 1; i++) {
      boolean flag = true;
      for (int j = 1; j < runStaffs.length - i; j++) {
        if (runStaffs[j - 1].getStaffRunTime() > runStaffs[j].getStaffRunTime()) {
          flag = false;
          tmp = runStaffs[j - 1];
          runStaffs[j - 1] = runStaffs[j];
          runStaffs[j] = tmp;
        }
      }
      if (flag) {
        break;
      }
    }
    long totalTime = 0;
    long avgTime;
    if (runStaffs.length > 2) {
      for (int i = 1; i < runStaffs.length - 1; i++) {
        totalTime += runStaffs[i].getStaffRunTime();
      }
      avgTime = totalTime / (runStaffs.length - 2);
    } else {
      for (int i = 0; i < runStaffs.length; i++) {
        totalTime += runStaffs[i].getStaffRunTime();
      }
      avgTime = totalTime / (runStaffs.length);
    }
    int quarterIndex = runStaffs.length / 4;
    int threeQuarterIndex = runStaffs.length * 3 / 4;
    long IRQ = runStaffs[threeQuarterIndex].getStaffRunTime()
        - runStaffs[quarterIndex].getStaffRunTime();
    LOG.info("IRQ is " + IRQ);
    long threshold = (long) (IRQ * 1.5 + runStaffs[threeQuarterIndex]
        .getStaffRunTime());
    LOG.info("staff runtime threshold is " + threshold);
    Set<Integer> setIDs = new HashSet<Integer>();
    for (int i = threeQuarterIndex; i < runStaffs.length; i++) {
      if (runStaffs[i].getStaffRunTime() > threshold) {
        LOG.info("Slow staff" + runStaffs[i].getStaffID()
            + " is first selected!");
        // Second select(using cost model)
        if ((job.getNumSuperStep() - runStaffs[i].getCurrentSuperStep())
            * (runStaffs[i].getStaffRunTime() - avgTime) > runStaffs[i]
              .getMigrateCost()) {
          LOG.info("Slow staff" + runStaffs[i].getStaffID()
              + " is second selected!");
          /*
           * Review suggestion: allow user to determine the times for finding
           * the slow staff Zhicheng Liu 2013/10/9
           */
          // Third select(predict worker's burden)
          if (shouldMigrate(runStaffs[i])) {
            int id = runStaffs[i].getStaffID();
            LOG.info("Slow staff" + id + " is third selected!");
            // Fourth select(continuous[3] testing)
            if (this.staffSlowCount[id] != this.conf.getInt(
                "bcbsp.loadbalance.findslowstaff.maxturn", 3) - 1) {
              this.staffSlowCount[id] += 1;
            } else {
              this.staffSlowCount[id] = 0;
              setIDs.add(id);
            }
          }
        }
      }
    }
    // ID = runStaffs[ID].getStaffID();
    // if(runStaffs[ID].currSuperStep==6){
    // setIDs.add(ID);//Feng added for test
    // LOG.info("Feng test getSlowStaffID! "+setIDs.toString());
    // }
    return setIDs;
  }
  
  /**
   * Update the register infomation of staffs
   * 
   * @param staffID
   *        staffID to update superstep run time.
   * @param time
   *        staff superstep runtime.
   */
  private void updateStaffSSRunTime(int staffID, long time) {
    StaffInProgress sip = getStaff(staffID);
    StaffStatus ss = sip.getStaffStatus(sip.getStaffID());
    ss.setRunTime(time);
    this.controller.updateStaff(sip.getWorkerManagerStatus()
        .getWorkerManagerName(), ss); // virtual worker
    // this.controller.updateStaff(ss.getGroomServer(), ss); //really worker
  }
  
  /**
   * get the staffInProgress information with staffId.
   * 
   * @param staffID
   *        staffID
   * @return if found return staffInProgress, not return null.
   */
  private StaffInProgress getStaff(int staffID) {
    for (int i = 0; i < staffs.length; i++) {
      if (staffs[i].getStaffID().getStaffID().getId() == staffID) {
        return staffs[i];
      }
    }
    return null;
  }
  
  /**
   * Predict the worker's burden in future,whether the staff need to migrate.
   * 
   * @param sm
   *        migrate staff to tell.
   * @return if the staff need migrate true, not false.
   */
  private boolean shouldMigrate(StaffMigrate sm) {
    LOG.info("Predict the worker's burden in future");
    StaffInProgress sip = getStaff(sm.getStaffID());
    // Check current staffs on the samework
    ArrayList<StaffStatus> onSameWorkerStaffs = this.controller.checkStaff(sip
        .getWorkerManagerStatus().getWorkerManagerName());
    LOG.info("sameWorkerStaffs is ");
    for (int i = 0; i < onSameWorkerStaffs.size(); i++) {
      LOG.info(onSameWorkerStaffs.get(i).getStaffId());
    }
    int willIdleStaffSlot = 0;
    double tolerance = 1 / 5;
    Iterator<StaffStatus> it = onSameWorkerStaffs.iterator();
    // Compute willIdleStaffSlot
    while (it.hasNext()) {
      StaffStatus ss = it.next();
      if (ss.getJobId().equals(this.jobId)) {
        continue;
      }
      // Staff in different job
      long time = ss.getRunTime();
      if (time != 0) {
        if (time
            * (this.controller.getJob(ss.getJobId()).getJob().getNumSuperStep() - ss
                .getSuperstepCount()) < (job.getNumBspStaff() - sm
            .getCurrentSuperStep()) * sm.getStaffRunTime() * tolerance) {
          willIdleStaffSlot++;
        }
      }
    }
    LOG.info("worker has max Staff count is "
        + sip.getWorkerManagerStatus().getMaxStaffsCount());
    LOG.info("current staff occupy staff slot is "
        + this.controller.checkStaff(
            sip.getWorkerManagerStatus().getWorkerManagerName()).size());
    LOG.info("willIdleStaffSlot is " + willIdleStaffSlot);
    if (willIdleStaffSlot
        + sip.getWorkerManagerStatus().getMaxStaffsCount()
        - this.controller.checkStaff(
            sip.getWorkerManagerStatus().getWorkerManagerName()).size() >= sip
        .getWorkerManagerStatus().getMaxStaffsCount() / 2) {
      return false;
    } else {
      return true;
    }
  }
  
  /**
   * migrate slowstaffs to other workers
   * 
   * @param staffID
   *        staffID to migrate
   */
  public void migrateStaff(int staffID) {
    Collection<WorkerManagerStatus> glist = controller
        .workerServerStatusKeySet();
    LOG.info("migrateSchedule--glist.size(): " + glist.size());
    WorkerManagerStatus[] gss = (WorkerManagerStatus[]) glist
        .toArray(new WorkerManagerStatus[glist.size()]);
    LOG.info("migrateSchedule-- WorkerManagerStatus[] gss.size: " + gss.length
        + "gss[0]: " + gss[0].getWorkerManagerName());
    StaffInProgress[] staffs = this.getStaffInProgress();
    WorkerManagerProtocol worker = null;
    boolean success = false;
    try {
      LOG.info("migrateSchedule --------staff " + staffID);
      this.obtainNewStaffForMigrate(gss, staffID, 1.0, true);
      worker = controller.findWorkerManager(staffs[staffID]
          .getWorkerManagerStatus());
      ArrayList<WorkerManagerAction> actions = new ArrayList<WorkerManagerAction>();
      actions.add(new LaunchStaffAction(staffs[staffID].getS()));
      Directive d = new Directive(controller.getActiveWorkerManagersName(),
          actions);
      d.setMigrateSSStep(this.getSuperStepCounter() + 1);
      LOG.info("current super step is " + this.getSuperStepCounter());
      success = worker.dispatch(staffs[staffID].getS().getJobID(), d, true,
          true, this.getNumAttemptRecovery());
      // Update the WorkerManagerStatus Cache
      WorkerManagerStatus new_gss = staffs[staffID].getWorkerManagerStatus();
      int currentStaffsCount = new_gss.getRunningStaffsCount();
      new_gss.setRunningStaffsCount((currentStaffsCount + 1));
      controller.updateWhiteWorkerManagersKey(
          staffs[staffID].getWorkerManagerStatus(), new_gss);
      LOG.info(staffs[staffID].getS().getStaffAttemptId()
          + " is divided to the " + new_gss.getWorkerManagerName());
    } catch (Exception e) {
      WorkerManagerStatus wms = staffs[staffID].getWorkerManagerStatus();
      LOG.error("Fail to assign staff-" + staffs[staffID].getStaffId() + " to "
          + wms.getWorkerManagerName());
      if (!success) {
        this.addBlackListWorker(staffs[staffID].getWorkerManagerStatus());
        worker.addFailedJob(this.getJobID());
        if (worker.getFailedJobCounter() > controller.getMaxFailedJobOnWorker()) {
          controller.removeWorkerFromWhite(wms); // white
          wms.setPauseTime(System.currentTimeMillis());
          controller.addWorkerToGray(wms, worker); // gray
          LOG.info(wms.getWorkerManagerName()
              + " will be transferred from [WhiteList] to [GrayList]");
        }
      } else {
        LOG.error("Exception has been catched in SimpleStaffScheduler--"
            + "recoverySchedule !", e);
        Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, this
            .getJobID().toString(), e.toString());
        this.getController().recordFault(f);
        this.getController().recovery(this.getJobID());
        try {
          this.getController().killJob(this.getJobID());
        } catch (IOException oe) {
          // LOG.error("Kill Job", oe);
          throw new RuntimeException("Kill Job", oe);
        }
      }
    }
  }
  
  /**
   * obtain the migrate staffs for the workermanagers
   * 
   * @param gss
   *        workermanagerstatus to obtain the migrate staffs.
   * @param i
   *        migrate staff id
   * @param tasksLoadFactor
   *        load factor to assign the staff.
   * @param migrate
   *        staff migrate flag.
   */
  public void obtainNewStaffForMigrate(WorkerManagerStatus[] gss, int i,
      double tasksLoadFactor, boolean migrate) {
    LOG.info("staffs[staffID] is " + staffs[i].getStaffID());
    String oldWorkerName = staffs[i].getWorkerManagerStatus()
        .getWorkerManagerName();
    staffs[i].setChangeWorkerState(true);
    LOG.info("obtainNewStaff" + "  " + migrate);
    try {
      staffs[i].getStaffToRunForMigrate(
          findMaxFreeWorkerManagerStatusForMigrate(gss, i, tasksLoadFactor),
          true);
    } catch (IOException ioe) {
      LOG.error(
          "Exception has been catched in JobInProgress--obtainNewStaff !", ioe);
      Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, this.getJobID()
          .toString(), ioe.toString());
      this.getController().recordFault(f);
      this.getController().recovery(this.getJobID());
      try {
        this.getController().killJob(job.getJobID());
      } catch (IOException e) {
        // LOG.error("IOException", e);
        throw new RuntimeException("IOException", e);
      }
    }
    String name = staffs[i].getWorkerManagerStatus().getWorkerManagerName();
    LOG.info("obtainNewWorker--[migrate]" + name);
    workersToStaffs.get(oldWorkerName).remove(
        staffs[i].getS().getStaffAttemptId());
    if (workersToStaffs.containsKey(name)) {
      workersToStaffs.get(name).add(staffs[i].getS().getStaffAttemptId());
      LOG.info("The workerName has already existed and add the staff directly");
    } else {
      ArrayList<StaffAttemptID> list = new ArrayList<StaffAttemptID>();
      list.add(staffs[i].getS().getStaffAttemptId());
      workersToStaffs.put(name, list);
      LOG.info("Add the workerName " + name
          + " and the size of all workers is " + this.workersToStaffs.size());
    }
  }
  
  /**
   * find the most free worker for the staffs need migrate.
   * 
   * @param wss
   *        workermanagerstatus list
   * @param staffID
   *        staffID to assign
   * @param staffsLoadFactor
   *        staffloadfactor to find free worker
   * @return workermanager status the staff has been assigned to.
   */
  public WorkerManagerStatus findMaxFreeWorkerManagerStatusForMigrate(
      WorkerManagerStatus[] wss, int staffID, double staffsLoadFactor) {
    int currentStaffs = 0;
    int maxStaffs = 0;
    int loadStaffs = 0;
    int tmp_count = 0;
    WorkerManagerStatus status = null;
    for (WorkerManagerStatus wss_tmp : wss) {
      // LOG.info("[findMaxFreeWorkerManagerStatus]Test: " +
      // gss_tmp.getWorkerManagerName());
      if (this.blackList.contains(wss_tmp)
          || staffs[staffID].getWorkerManagerStatus().equals(wss_tmp)) {
        // LOG.warn(gss_tmp.getWorkerManagerName() +
        // " is ignored because it is in the BlacList!
        // [findMaxFreeWorkerManagerStatus]");
        continue;
      }
      currentStaffs = wss_tmp.getRunningStaffsCount();
      maxStaffs = wss_tmp.getMaxStaffsCount();
      loadStaffs = Math.min(maxStaffs,
          (int) Math.ceil(staffsLoadFactor * maxStaffs));
      if ((loadStaffs - currentStaffs) > tmp_count) {
        tmp_count = loadStaffs - currentStaffs;
        status = wss_tmp;
      }
    }
    return status;
  }
  
  @Override
  public void clearStaffsForJob() {
    for (int i = 0; i < staffs.length; i++) {
      controller.deleteOldStaffs(staffs[i].getWorkerManagerStatus()
          .getWorkerManagerName(), staffs[i].getStaffStatus(staffs[i]
          .getStaffID()));
      // controller.test();
    }
  }
  
  /**
   * get the Queue name of the job in
   * 
   * @return waitQueue name.
   */
  public String getQueueNameFromPriority() {
    // TODO Auto-generated method stub
    int i = Integer.parseInt(this.priority);
    switch (i) {
    case 1:
      return "HIGHER_WAIT_QUEUE";
    case 2:
      return "HIGH_WAIT_QUEUE";
    case 3:
      return "NORMAL_WAIT_QUEUE";
    case 4:
      return "LOW_WAIT_QUEUE";
    case 5:
      return "LOWER_WAIT_QUEUE";
    default:
      return null;
    }
  }
  
  /**
   * get the job response ratio.
   * 
   * @return response ratio.
   */
  public double getHRN() {
    // TODO Auto-generated method stub
    return this.hrnR = (System.currentTimeMillis() - this.startTime + this
        .getProcessTime()) * 100 / this.getProcessTime();
  }
  
  /**
   * get the job processing time with job superstepnum,job edgenum and staff
   * num.
   * 
   * @return job processing time.
   */
  public long getProcessTime() {
    // TODO Auto-generated method stub
    this.processTime = this.estimateFactor * this.getSuperStepNum()
        * this.getEdgeNum() / this.getNumBspStaff();
    return this.processTime;
  }
  
  /**
   * get the job edgenum
   * 
   * @return edgenum.
   */
  private long getEdgeNum() {
    // TODO Auto-generated method stub
    return this.edgeNum;
  }
  
  /**
   * clean the workermanagernames information on zookeeper when the recovery
   * staff is assign to other workers for multi staffs recovery,only clean once.
   */
  /* Baoxing Yang added */
  public void setCleanedWMNs() {
    this.cleanedWMNs = true;
  }
  
  /**
   * uncleaned flag.
   */
  public void setUnCleanedWMNs() {
    this.cleanedWMNs = false;
  }
  
  /**
   * if the workermanagernames is cleaned.
   * 
   * @return cleaned flag.
   */
  public boolean isCleanedWMNs() {
    return this.cleanedWMNs;
  }
  
  /**
   * set the job removed from joblistenner flag.
   */
  public void setRemovedFromListener() {
    this.removedFromListener = true;
  }
  
  /**
   * set the job not removed from joblistenner flag.
   */
  public void setUnRemovedFromListener() {
    this.removedFromListener = false;
  }
  
  /**
   * if the job is removed from joblistenner.
   * 
   * @return removedFromListener flag.
   */
  public boolean isRemovedFromListener() {
    return this.removedFromListener;
  }
  
  /**
   * if controller's faultmap contains jobID recovery is not finished.
   * 
   * @return finished true,unfinished false.
   */
  public boolean isCompletedRecovery() {
    if (controller.getFaultMap().containsKey(getJobID())) {
      return false;
    } else {
      return true;
    }
  }
  
  /**
   * For JUnit test.
   */
  public void setJobId(BSPJobID jobId) {
    this.jobId = jobId;
  }
  
  public void setStaffs(StaffInProgress[] staffs) {
    this.staffs = staffs;
  }
}
