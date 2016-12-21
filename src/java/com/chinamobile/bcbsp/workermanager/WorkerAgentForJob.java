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

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Aggregator;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.pipes.Application;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.sync.WorkerSSController;
import com.chinamobile.bcbsp.sync.WorkerSSControllerInterface;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

/**
 * WorkerAgentForJob. It is create by WorkerManager for every job that running
 * on it. This class manages all staffs which belongs to the same job, maintains
 * public information, completes the local synchronization and aggregation.
 */
public class WorkerAgentForJob implements WorkerAgentInterface {
  /** Define Log variable for outputting messages */
  private static final Log LOG = LogFactory.getLog(WorkerAgentForJob.class);
  /** Map for including all information used during SuperStep Synchronization */
  private Map<StaffAttemptID, SuperStepReportContainer> runningStaffInformation
    = new HashMap<StaffAttemptID, SuperStepReportContainer>();
  /** The counter of staffReport */
  private volatile Integer staffReportCounter = 0;
  /** wssc is connected to WorkerAgentForJob */
  private WorkerSSControllerInterface wssc;
  /** Define workerManagerName for storing worker's name */
  private String workerManagerName;
  /** The count of workers */
  private int workerManagerNum = 0;
  /** HashMap for storing partition and workerManagerName */
  private HashMap<Integer, String> partitionToWorkerManagerName = new
      HashMap<Integer, String>();
  /** Map for storing InetSocketAddress and WorkerAgent */
  private final Map<InetSocketAddress, WorkerAgentInterface> workers = new
      ConcurrentHashMap<InetSocketAddress, WorkerAgentInterface>();
  /** The port of job */
  private int portForJob;
  /** Define InetSocketAddress */
  private InetSocketAddress workAddress;
  /** Define Server */
  private Server server = null;
  /** Define BSP Configuration */
  private Configuration conf;
  /** ID of BSPJob*/
  private BSPJobID jobId;
  /** Define BSPJob object*/
  private BSPJob jobConf;
  /** Define WorkerManager object */
  private WorkerManager workerManager;
  /** Map for registering aggregate values */
  private HashMap<String, Class<? extends AggregateValue<?, ?>>>
  nameToAggregateValue = new
      HashMap<String, Class<? extends AggregateValue<?, ?>>>();
  /** Map for registering aggregate values */
  private HashMap<String, Class<? extends Aggregator<?>>> nameToAggregator =
      new HashMap<String, Class<? extends Aggregator<?>>>();
  /** Map for registering aggregate values */
  @SuppressWarnings("rawtypes")
  private HashMap<String, ArrayList<AggregateValue>> aggregateValues =
      new HashMap<String, ArrayList<AggregateValue>>();
  /** Map for registering aggregate values */
  @SuppressWarnings("rawtypes")
  private HashMap<String, AggregateValue> aggregateResults =
      new HashMap<String, AggregateValue>();
  /** Define BSPJob object */
  private Counters counters = new Counters();
  /** Define Application object */
  private Application application;

  /**
   * Constructor.
   * @param wssci WorkerSSControllerInterface
   */
  public WorkerAgentForJob(WorkerSSControllerInterface wssci) {
    this.wssc = wssci;
  }

  /**
   * Constructor.
   *
   * @param conf  Configuration
   * @param jobId BSPJobID
   * @param jobConf BSPJob
   * @param workerManager WorkerManager
   */
  public WorkerAgentForJob(Configuration conf, BSPJobID jobId, BSPJob jobConf,
      WorkerManager workerManager) throws IOException {
    this.jobId = jobId;
    this.jobConf = jobConf;
    this.workerManager = workerManager;
    this.workerManagerName = conf.get(Constants.BC_BSP_WORKERAGENT_HOST,
        Constants.BC_BSP_WORKERAGENT_HOST);
    this.wssc = new WorkerSSController(jobId, this.workerManagerName);
    this.conf = conf;
    String bindAddress = conf.get(Constants.BC_BSP_WORKERAGENT_HOST,
        Constants.DEFAULT_BC_BSP_WORKERAGENT_HOST);
    int bindPort = conf.getInt(Constants.BC_BSP_WORKERAGENT_PORT,
        Constants.DEFAULT_BC_BSP_WORKERAGENT_PORT);
    bindPort = bindPort + Integer.parseInt(jobId.toString().substring(17));
    portForJob = bindPort;
    workAddress = new InetSocketAddress(bindAddress, bindPort);
    reinitialize();
    // For Aggregation
    loadAggregators();
  }

  /**
   * Reinitialize.
   */
  public void reinitialize() {
    try {
      LOG.info("reinitialize() the WorkerAgentForJob: " + jobId.toString());
      server = RPC.getServer(this, workAddress.getHostName(),
          workAddress.getPort(), conf);
      server.start();
      LOG.info("WorkerAgent address:" + workAddress.getHostName() + " port:" +
          workAddress.getPort());
    } catch (IOException e) {
      //LOG.error("[reinitialize]", e);
      throw new RuntimeException("WorkerAgentForJob reintialize exception", e);
    }
  }

  /**
   * Prepare to local synchronization, including computing all kinds of
   * information.
   * @return SupterStepReportContainer
   */
  private SuperStepReportContainer prepareLocalBarrier() {
    int stageFlag = 0;
    long judgeFlag = 0;
    String[] dirFlag = {"1"};
    String[] aggValues;
    ArrayList<ArrayList<String>> aggValueForC = new
        ArrayList<ArrayList<String>>();
    String[] newAggValues = null;
    String migrateInfo = "";
    for (Entry<StaffAttemptID, SuperStepReportContainer> e :
      this.runningStaffInformation.entrySet()) {
      SuperStepReportContainer tmp = e.getValue();
      stageFlag = tmp.getStageFlag();
      dirFlag = tmp.getDirFlag();
      judgeFlag += tmp.getJudgeFlag();

      ArrayList<String> aggVC = new ArrayList<String>();
      // Get the aggregation values from the ssrcs.
      aggValues = tmp.getAggValues();
      // added for c++
      LOG.info("job type is: " +
          jobConf.get(Constants.USER_BC_BSP_JOB_TYPE, ""));
      if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(jobConf.get(
          Constants.USER_BC_BSP_JOB_TYPE, ""))) {
        int superstepNum = tmp.getCurrentSuperStep();
        LOG.info("current superstep is " + superstepNum);
        if (aggValues != null) {
          for (int i = 0; i < aggValues.length; i++) {
            // AggVC.set(i, aggValues[i]);
            aggVC.add(aggValues[i]);
          } // for
           // AggValueforC.set(j, AggVC);
          aggValueForC.add(aggVC);
        } // if
        LOG.info("deal with aggValues");
      } else {
        decapsulateAggregateValues(aggValues);
      }
      migrateInfo += tmp.getStaffID() + ":" + tmp.getCurrentSuperStep() + ":" +
        tmp.getStaffRunTime() + ":" + tmp.getMigrateCost() + "/";
    } // end-for
    // Compute the aggregations for all staffs in the worker.
    if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(jobConf.get(
        Constants.USER_BC_BSP_JOB_TYPE, ""))) {
      String filename0 = this.conf.get(Constants.USER_BC_BSP_JOB_EXE);
      String filename = this.jobConf.get(Constants.USER_BC_BSP_JOB_EXE);
      Path localJarFile = null;
      try {
        localJarFile = this.jobConf.getLocalPath(
            Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/JobC");
      } catch (IOException e2) {
       // e2.printStackTrace();
        //LOG.error("[prepareLocalBarrier]", e2);
        throw new RuntimeException("WorkerAgentForJob " +
          "prepareLocalBarrier exception", e2);
      }
      String jarFile = jobConf.getJobExe();
      LOG.info("filename0 is :" + filename0);
      LOG.info("filename is :" + filename);
      LOG.info("localJarFile is :" + localJarFile.toString());
      LOG.info("JobExe is :" + jarFile);
      LOG.info("confJobExe is :" + conf.get(Constants.USER_BC_BSP_JOB_EXE));
      if (this.application == null) {
        try {
          this.application = new Application(jobConf, "WorkerManager",
              localJarFile.toString());
        } catch (IOException e1) {
          //LOG.error("[prepareLocalBarrier]", e1);
          //e1.printStackTrace();
          throw new RuntimeException("WorkerAgentForJob " +
              "prepareLocalBarrier exception", e1);
        } catch (InterruptedException e1) {
          //e1.printStackTrace();
          //LOG.error("[prepareLocalBarrier]", e1);
          throw new RuntimeException("WorkerAgentForJob " +
              "prepareLocalBarrier exception", e1);
        }
      } else {
        LOG.info("application is not null");
      }
      LOG.info("[songjianze] AggvalueforC size: " + aggValueForC.size());
      Iterator<ArrayList<String>> it = aggValueForC.iterator();
      while (it.hasNext()) {
        String[] saggV = (String[]) it.next().toArray(new String[0]);
        if (saggV != null) {
          try {
            for (int j = 0; j < saggV.length; j++) {
              LOG.info("aggvalue: " + saggV[j]);
            }
            LOG.info("[songjianze]send new aggvalue");
            application.getDownlink().sendNewAggregateValue(saggV);
            LOG.info("before superstep");
            application.getDownlink().runASuperStep();
            LOG.info("after superstep");
            LOG.info("wait for superstep  to finish");
            this.application.waitForFinish();
            ArrayList<String> aggregateValue = null;
            LOG.info("the aggregator finished");
            aggregateValue = this.application.getHandler().getAggregateValue();
            LOG.info("aggregateValue size is: " + aggregateValue.size());
            newAggValues = aggregateValue.toArray(new String[0]);
            this.application.getHandler().getAggregateValue().clear();
            for (int i = 0; i < newAggValues.length; i++) {
              LOG.info("aggregatevalue is : " + newAggValues[i]);
            }
          } catch (IOException e1) {
            //e1.printStackTrace();
            //LOG.error("[prepareLocalBarrier]", e1);
            throw new RuntimeException("WorkerAgentForJob " +
                "prepareLocalBarrier exception", e1);
          } catch (Throwable e) {
            //e.printStackTrace();
            //LOG.error("[prepareLocalBarrier]", e);
            throw new RuntimeException("WorkerAgentForJob " +
                "prepareLocalBarrier exception", e);
          }
        } else {
          LOG.info("[songjianze]  saggV is null");
        }
      } // while

    } else {
      localAggregate();
      // Encapsulate the aggregation values to String[] for the ssrc.
      newAggValues = encapsulateAggregateValues();
    }
    //newAggValues into the new ssrc.
    SuperStepReportContainer ssrc = new SuperStepReportContainer(stageFlag,
        dirFlag, judgeFlag, newAggValues, migrateInfo);
    return ssrc;
  }

  /**
   * Add counter information of staff
   */
  public void addStaffReportCounter() {
    this.staffReportCounter++;
  }

  /**
   * Clear counter information of staff
   */
  private void clearStaffReportCounter() {
    this.staffReportCounter = 0;
  }

  /**
   * All staffs belongs to the same job will use this to complete the local
   * synchronization and aggregation.
   * @param jobId BSPJobID
   * @param staffId StaffAttemptID
   * @param superStepCounter the count of superStep
   * @param ssrc SuperStepReportContainer
   * @return true or false
   */
  @Override
  public boolean localBarrier(BSPJobID jobId, StaffAttemptID staffId,
      int superStepCounter, SuperStepReportContainer ssrc) {
    this.runningStaffInformation.put(staffId, ssrc);
    LOG.info("Debug: The localBarrier's stage is " + ssrc.getStageFlag());
    LOG.info("Debug:before synchronized! and this.staffReportCounter is " +
      this.staffReportCounter);
    synchronized (this.staffReportCounter) {
      addStaffReportCounter();
      LOG.info(staffId.toString() + " [staffReportCounter]" +
        this.staffReportCounter);
      LOG.info(staffId.toString() + " [staffCounter]" +
        ssrc.getLocalBarrierNum());
      if (this.staffReportCounter == ssrc.getLocalBarrierNum()) {
        clearStaffReportCounter();
        switch (ssrc.getStageFlag()) {
        case Constants.SUPERSTEP_STAGE.FIRST_STAGE:
          wssc.firstStageSuperStepBarrier(superStepCounter, ssrc);
          break;
        case Constants.SUPERSTEP_STAGE.SECOND_STAGE:
          wssc.setCounters(this.counters);
          // LOG.info("in workerAgentForJob localBarrier!!!!");
          // this.counters.log(LOG);
          this.counters.clearCounters();
          // this.counters.log(LOG);
          wssc.secondStageSuperStepBarrier(superStepCounter,
              prepareLocalBarrier());
          break;
        case Constants.SUPERSTEP_STAGE.WRITE_CHECKPOINT_SATGE:
        case Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE:
          if (ssrc.getStageFlag() ==
            Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE) {
            String workerNameAndPort = ssrc.getPartitionId() + ":" +
              this.workerManagerName + ":" + ssrc.getPort2();
            for (Entry<StaffAttemptID, SuperStepReportContainer> e :
              this.runningStaffInformation.entrySet()) {
              if (e.getKey().equals(staffId)) {
                continue;
              } else {
                String str = e.getValue().getPartitionId() + ":" +
                  this.workerManagerName + ":" + e.getValue().getPort2();
                workerNameAndPort += Constants.KV_SPLIT_FLAG + str;
              }
            }
            ssrc.setActiveMQWorkerNameAndPorts(workerNameAndPort);
            wssc.checkPointStageSuperStepBarrier(superStepCounter, ssrc);
          } else {
            wssc.checkPointStageSuperStepBarrier(superStepCounter, ssrc);
          }
          break;
        case Constants.SUPERSTEP_STAGE.SAVE_RESULT_STAGE:
          wssc.saveResultStageSuperStepBarrier(superStepCounter, ssrc);
          break;
        default:
          LOG.error("The SUPERSTEP of " +
            ssrc.getStageFlag() + " is not known");
        }
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public int getNumberWorkers(BSPJobID jobId, StaffAttemptID staffId) {
    return this.workerManagerNum;
  }

  @Override
  public void setNumberWorkers(BSPJobID jobId, StaffAttemptID staffId,
      int num) {
    this.workerManagerNum = num;
  }

  @Override
  public void close() throws IOException {
    this.server.stop();
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return WorkerAgentInterface.versionID;
  }

  @Override
  public String getWorkerManagerName(BSPJobID jobId, StaffAttemptID staffId) {
    return this.workerManagerName;
  }

  /**
   * Add the counters information.
   * @param staffId StaffAttemptID
   */
  public void addStaffCounter(StaffAttemptID staffId) {
    SuperStepReportContainer ssrc = new SuperStepReportContainer();
    this.runningStaffInformation.put(staffId, ssrc);
  }

  /**
   * Sets the job configuration.
   * @param jobConf BSPJob
   */
  public void setJobConf(BSPJob jobConf) {
  }

  /**
   * Get WorkerAgent BSPJobID.
   * @return BSPJobID
   */
  @Override
  public BSPJobID getBSPJobID() {
    return this.jobId;
  }

  /**
   * Get WorkerAgent BSPJobID.
   * @param addr InetSocketAddress
   * @return BSPJobID
   */
  protected WorkerAgentInterface getWorkerAgentConnection(
      InetSocketAddress addr) {
    WorkerAgentInterface worker;
    synchronized (this.workers) {
      worker = workers.get(addr);
      if (worker == null) {
        try {
          worker = (WorkerAgentInterface) RPC.getProxy(
              WorkerAgentInterface.class, WorkerAgentInterface.versionID, addr,
              this.conf);
        } catch (IOException e) {
          //LOG.error("[getWorkerAgentConnection]", e);
          throw new RuntimeException("WorkerAgentForJob " +
              "getWorkerAgentConnection exception", e);
        }
        this.workers.put(addr, worker);
      }
    }
    return worker;
  }

  /**
   * This method is used to set mapping table that shows the partition to the
   * worker.
   * @param jobId BSPJobID
   * @param partitionId id of specific pattition
   * @param hostName the name of host
   */
  public void setWorkerNametoPartitions(BSPJobID jobId, int partitionId,
      String hostName) {
    this.partitionToWorkerManagerName.put(partitionId, hostName + ":" +
      this.portForJob);
  }

  /**
   * Load aggregators.
   */
  private void loadAggregators() {
    int aggregateNum = this.jobConf.getAggregateNum();
    String[] aggregateNames = this.jobConf.getAggregateNames();
    for (int i = 0; i < aggregateNum; i++) {
      String name = aggregateNames[i];
      this.nameToAggregator.put(name, this.jobConf.getAggregatorClass(name));
      this.nameToAggregateValue.put(name, jobConf.getAggregateValueClass(name));
      this.aggregateValues.put(name, new ArrayList<AggregateValue>());
    }
  }

  /**
   * To decapsulate the aggregation values from the String[]. The aggValues
   * should be in form as follows: [ AggregateName \t AggregateValue.toString()
   * ]
   * @param aggValues
   *        String[]
   */
  private void decapsulateAggregateValues(String[] aggValues) {
    for (int i = 0; i < aggValues.length; i++) {
      String[] aggValueRecord = aggValues[i].split(Constants.KV_SPLIT_FLAG);
      String aggName = aggValueRecord[0];
      String aggValueString = aggValueRecord[1];
      AggregateValue aggValue = null;
      try {
        aggValue = this.nameToAggregateValue.get(aggName).newInstance();
        aggValue.initValue(aggValueString); // init the aggValue from its string
                                            // form.
      } catch (InstantiationException e1) {
        //LOG.error("InstantiationException", e1);
        throw new RuntimeException("WorkerAgentForJob " +
            "InstantiationException exception", e1);
      } catch (IllegalAccessException e1) {
        //LOG.error("IllegalAccessException", e1);
        throw new RuntimeException("WorkerAgentForJob " +
            "IllegalAccessException exception", e1);
      } // end-try
      if (aggValue != null) {
        ArrayList<AggregateValue> list = this.aggregateValues.get(aggName);
        list.add(aggValue); // put the value to the values' list for aggregation
                            // ahead.
      } // end-if
    } // end-for
  }

  /**
   * To aggregate the values from the running staffs.
   */
  @SuppressWarnings("unchecked")
  private void localAggregate() {
    // Clear the results' container before the calculation of a new super step.
    this.aggregateResults.clear();
    // To calculate the aggregations.
    for (Entry<String, Class<? extends Aggregator<?>>> entry :
      this.nameToAggregator.entrySet()) {
      Aggregator<AggregateValue> aggregator = null;
      try {
        aggregator = (Aggregator<AggregateValue>) entry.getValue()
            .newInstance();
      } catch (InstantiationException e1) {
        //LOG.error("InstantiationException", e1);
        throw new RuntimeException("WorkerAgentForJob " +
            "InstantiationException exception", e1);
      } catch (IllegalAccessException e1) {
        //LOG.error("IllegalAccessException", e1);
        throw new RuntimeException("WorkerAgentForJob " +
            "IllegalAccessException exception", e1);
      }
      if (aggregator != null) {
        ArrayList<AggregateValue> aggVals = this.aggregateValues.get(entry
            .getKey());
        AggregateValue resultValue = aggregator.aggregate(aggVals);
        this.aggregateResults.put(entry.getKey(), resultValue);
        // Clear the initial aggregate values after aggregation completed.
        aggVals.clear();
      }
    }
  }

  /**
   * To encapsulate the aggregation values to the String[]. The aggValues should
   * be in form as follows: [ AggregateName \t AggregateValue.toString() ]
   * @return String[]
   */

  private String[] encapsulateAggregateValues() {
    int aggSize = this.aggregateResults.size();
    String[] aggValues = new String[aggSize];
    int i = 0;
    for (Entry<String, AggregateValue> entry :
      this.aggregateResults.entrySet()) {
      aggValues[i] = entry.getKey() + Constants.KV_SPLIT_FLAG +
          entry.getValue().toString();
      i++;
    }
    return aggValues;
  }

  public synchronized int getFreePort() {
    return this.workerManager.getFreePort();
  }

  @Override
  public void setStaffAgentAddress(StaffAttemptID staffID, String addr) {
    this.workerManager.setStaffAgentAddress(staffID, addr);
  }

  /**
   * Add all of counters.
   * @param pCounters {@link Counters}
   */
  public void addCounters(Counters pCounters) {
    this.counters.incrAllCounters(pCounters);

  }

  /**
   * Update staff information.
   * @param staffId StaffAttemptID
   * @return the result of update
   */
  public boolean updateStaffsReporter(StaffAttemptID staffId) {
    if (!this.runningStaffInformation.containsKey(staffId)) {
      return false;
    } else {
      this.runningStaffInformation.remove(staffId);
      return true;
    }
  }

  @Override
  public void clearStaffRC(BSPJobID jobId) {
    this.staffReportCounter = 0;
  }

  /** For JUnit test. */
  public void setJobId(BSPJobID jobId) {
	this.jobId = jobId;
  }

  public WorkerSSControllerInterface getWssc() {
    return wssc;
  }

  public void setWssc(WorkerSSControllerInterface wssc) {
    this.wssc = wssc;
  }

  public HashMap<Integer, String> getPartitionToWorkerManagerName() {
    return partitionToWorkerManagerName;
  }

  public void setPartitionToWorkerManagerName(
      HashMap<Integer, String> partitionToWorkerManagerName) {
    this.partitionToWorkerManagerName = partitionToWorkerManagerName;
  }

  public Integer getStaffReportCounter() {
    return staffReportCounter;
  }

  public void setStaffReportCounter(Integer staffReportCounter) {
    this.staffReportCounter = staffReportCounter;
  }

  public Map<StaffAttemptID, SuperStepReportContainer> getRunningStaffInformation() {
    return runningStaffInformation;
  }

  public void setRunningStaffInformation(
      Map<StaffAttemptID, SuperStepReportContainer> runningStaffInformation) {
    this.runningStaffInformation = runningStaffInformation;
  }

  public Counters getCounters() {
    return counters;
  }

  public void setCounters(Counters counters) {
    this.counters = counters;
  }
}
