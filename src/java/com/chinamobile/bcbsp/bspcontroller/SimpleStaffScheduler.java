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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffStatus;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

import com.chinamobile.bcbsp.Constants.BspControllerRole;
import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.action.LaunchStaffAction;
import com.chinamobile.bcbsp.action.WorkerManagerAction;
import com.chinamobile.bcbsp.bspcontroller.JobInProgress;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.StaffInProgress;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import com.chinamobile.bcbsp.sync.SynchronizationServerInterface;
import com.chinamobile.bcbsp.sync.SynchronizationServer;

/**
 * A simple staff scheduler. to be described by Wang Zhigang.
 * @author
 * @version
 */
public class SimpleStaffScheduler extends StaffScheduler {
  /** higher priority job waitqueue */
  public static final String HIGHER_WAIT_QUEUE = "HIGHER_WAIT_QUEUE";
  /** high priority job waitqueue */
  public static final String HIGH_WAIT_QUEUE = "HIGH_WAIT_QUEUE";
  /** normal priority job waitqueue */
  public static final String NORMAL_WAIT_QUEUE = "NORMAL_WAIT_QUEUE";
  /** low priority job waitqueue */
  public static final String LOW_WAIT_QUEUE = "LOW_WAIT_QUEUE";
  /** lower priority job waitqueue */
  public static final String LOWER_WAIT_QUEUE = "LOWER_WAIT_QUEUE";
  /** job processing queue */
  public static final String PROCESSING_QUEUE = "processingQueue";
  /** finished jobs queue */
  public static final String FINISHED_QUEUE = "finishedQueue";
  /** failed jobs queue */
  public static final String FAILED_QUEUE = "failedQueue";
  /** handle log information */
  private static final Log LOG = LogFactory.getLog(SimpleStaffScheduler.class);
  /** queues length */
  private static final int CACHE_QUEUE_LENGTH = 20;
  /** jobInProgress list */
  private List<JobInProgress> list; // added by feng
  // add by chen
  /** hdfs operator */
  private HDFSOperator haLogOperator;
  /** flag for whether sfaff scheduler is initialized */
  private volatile boolean initialized;
  /** SynchronizationServerInterface handle */
  private SynchronizationServerInterface syncServer;
  /** JobListener handle */
  private JobListener jobListener;
  /** JobProcessor handle */
  private JobProcessor jobProcessor;
  /** waitqueues total num */
  private int WaitQueuesTotal = 5;
  /** diferent waitqueues */
  private String[] waitQueues = {"HIGHER_WAIT_QUEUE","HIGH_WAIT_QUEUE",
      "NORMAL_WAIT_QUEUE","LOW_WAIT_QUEUE","LOWER_WAIT_QUEUE"};
  
  /**
   * A listener for changes in a {@link JobInProgress job}'s lifecycle in the
   * {@link BSPController}.
   * @author
   */
  private class JobListener extends JobInProgressListener {
    @Override
    public void jobAdded(JobInProgress job) throws IOException {
      queueManager.initJob(job); // init staff
      String waitQueue;
      waitQueue = job.getQueueNameFromPriority();
      /** lock control(JobProcessor.run()--find(WAIT_QUEUE)) */
      synchronized (waitQueue) {
        queueManager.addJob(waitQueue, job);
        LOG.info("JobListener: " + job.getJobID() + " is added to the "  +
           waitQueue);
        queueManager.adjustQueue();
        for (int i = 0; i < WaitQueuesTotal; i++) {
          list = new ArrayList<JobInProgress>(queueManager.findQueue(
              waitQueues[i]).getJobs());
          if (list.isEmpty()) {
            LOG.info(waitQueues[i] + " is empty!");
            continue;
          } else {
            LOG.info("the jobs are going to be shcheduled as follow");
            for (JobInProgress jib : list) {
              LOG.info(jib.getJobID());
            }
          }
        }
        LOG.info("job's processTime is " + job.getProcessTime());
      }
    }

    /**
     * Review comments: (1)If clients submit jobs continuously, FAILED_QUEUE and
     * FINISHED_QUEUE will be too large to be resident in the memory. Then the
     * BSPController will be crashed! Review time: 2011-11-30; Reviewer: Hongxu
     * Zhang. Fix log: (1)If the length of FINISHED_QUEUE or FAILED_QUEUE is
     * more than CACHE_QUEUE_LENGTH, the job in the header of QUEUE will be
     * cleanup. Fix time: 2011-12-04; Programmer: Zhigang Wang.
     */
    /* Baoxing Yang modified */
    @Override
    public ArrayList<BSPJobID> jobRemoved(JobInProgress job)
        throws IOException {
      // just for test
      if (job == null) {
        LOG.info("job is null");
      }
      if (job.getStatus() == null) {
        LOG.info("job.getStatus() is null");
      }
      if (job.getStatus().getRunState() == JobStatus.RECOVERY) {
        if (queueManager.findQueue(PROCESSING_QUEUE).contains(job)) {
          queueManager.moveJob(PROCESSING_QUEUE, FAILED_QUEUE, job);
          LOG.info("JobListener" + job.getJobID() +
             " is removed from the FAILED_QUEUE");
        } else {
          queueManager.moveJob(job.getQueueNameFromPriority(), FAILED_QUEUE,
              job);
          LOG.info("JobListener" + job.getJobID() + " is removed from the " +
             job.getQueueNameFromPriority());
        }
      } else if (job.getStatus().getRunState() != JobStatus.RUNNING) {
        // do nothing
        // queueManager.moveJob(PROCESSING_QUEUE, FINISHED_QUEUE, job);
        // }else{
        queueManager.moveJob(PROCESSING_QUEUE, FINISHED_QUEUE, job);
      }
      ArrayList<BSPJobID> removeJob = new ArrayList<BSPJobID>();
      ArrayList<JobInProgress> finished = new ArrayList<JobInProgress>(
          queueManager.findQueue(FINISHED_QUEUE).getJobs());
      ArrayList<JobInProgress> failed = new ArrayList<JobInProgress>(
          queueManager.findQueue(FAILED_QUEUE).getJobs());
      if (finished.size() > CACHE_QUEUE_LENGTH) {
        removeJob.add(finished.get(0).getJobID());
        queueManager.removeJob(FINISHED_QUEUE, finished.get(0));
      }
      if (failed.size() > CACHE_QUEUE_LENGTH) {
        removeJob.add(failed.get(0).getJobID());
        queueManager.removeJob(FAILED_QUEUE, failed.get(0));
      }
      return removeJob;
    }
  }
  
  /**
   * JobProcessor to be described by Wang Zhigang.
   * @author
   * @version
   */
  private class JobProcessor extends Thread implements Schedulable {
    /**JobProcessor construct method*/
    JobProcessor() {
      super("JobProcess");
    }
    /**
     * run: scheduler thread. Main logic scheduling staff to WorkerManager(s).
     * Also, it will move JobInProgress from WAIT_QUEUE to PROCESSING_QUEUE
     */
    public void run() {
      if (false == initialized) {
        throw new IllegalStateException("SimpleStaffScheduler initialization" +
           " is not yet finished!");
      }
      while (initialized) {
        Queue<JobInProgress> queue = null;
        // add lock to WAIT_QUEUE
        for (int j = 0; j < waitQueues.length; j++) {
          synchronized (waitQueues[j]) {
            queue = queueManager.findQueue(waitQueues[j]);
          }
          if (queue.getSize() != 0L) {
            LOG.info("queue's name is " + waitQueues[j]);
            break;
          }
        }
        if (queue == null) {
          throw new NullPointerException("All wait queues does not exist.");
        }
        if (queue.isEmpty()) {
          continue;
        }
        // remove a job from the WAIT_QUEUE and check the ClusterStatus
        // LOG.info("before getFront######");
        JobInProgress jip = queue.getFront();
        // LOG.info("after getFront######");
        if (!jip.isCompletedRecovery()) {
          try {
            LOG.info("jip is UnCompletedRecovery!");
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("[SimpleStaffSchedule]" +
                 "interrupted Exception! ", e);
          }
          continue;
        }
        jip = queue.removeJob();
        ClusterStatus cs;
        while (true) {
          try {
            Thread.sleep(2000);
            cs = controller.getClusterStatus(false);
            if (jip.getNumBspStaff() <= (cs.getMaxClusterStaffs() - cs
                .getRunningClusterStaffs())) {
              break;
            }
          } catch (Exception e) {
            // TODO : The NullPointerException maybe happen when stop the
            // thread.
            throw new RuntimeException("[SimpleStaffSchedule]" +
                 "interrupted Exception! ", e);
          }
        } // while
        if (role.equals(BspControllerRole.ACTIVE)) {
          try {
            haLogOperator = new HDFSOperator();
            haLogOperator.createFile(conf.get(Constants.BC_BSP_HA_LOG_DIR) +
               Constants.BC_BSP_HA_SCHEDULE_LOG);
            haLogOperator.writeFile(jip.getJobID().toString(),
                conf.get(Constants.BC_BSP_HA_LOG_DIR) +
                   Constants.BC_BSP_HA_SCHEDULE_LOG);
          } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error(e);
          }
        }
        // schedule the job and add it to the PROCESSING_QUEUE
        chooseScheduler(jip);
        queueManager.addJob(PROCESSING_QUEUE, jip);
      } // while
    } // run

    /**
     * schedule: Schedule job to the chosen Worker
     * @param jip
     *        JobInProgress
     */
    public void chooseScheduler(JobInProgress jip) {
      if (jip.getStatus().getRunState() == JobStatus.RUNNING) {
        normalSchedule(jip);
      } else if (jip.getStatus().getRunState() == JobStatus.RECOVERY) {
        recoverySchedule(jip);
      } else {
        LOG.warn("Currently master only shcedules job" +
          "in running state or revovery state. " +
            "This may be refined in the future. JobId:" + jip.getJobID());
      }
    }

    /**
     * Review comments: (1)The name of variables is not coherent. For examples,
     * I think the "groomServerManager" should be "controller", and the
     * "tasksLoadFactor" should be "staffsLoadFactor". Review time: 2011-11-30;
     * Reviewer: Hongxu Zhang. Fix log: (1)The conflicting name of variables has
     * been repaired. Fix time: 2011-12-04; Programmer: Zhigang Wang.
     * @param job 
     *        jobInProgress to be scheduled.
     */
    @SuppressWarnings("unchecked")
    public void normalSchedule(JobInProgress job) {
      List<JobInProgress> jip_wait;
      List<JobInProgress> jip_process;
      int remainingStaffsLoad = job.getNumBspStaff();
      for (int i = 0; i < waitQueues.length; i++) {
        synchronized (waitQueues[i]) {
          jip_wait = new ArrayList<JobInProgress>(queueManager.findQueue(
              waitQueues[i]).getJobs());
          for (JobInProgress jip : jip_wait) {
            remainingStaffsLoad += jip.getNumBspStaff();
          }
        }
      }
      int runningStaffLoad = 0;
      synchronized (PROCESSING_QUEUE) {
        jip_process = new ArrayList<JobInProgress>(queueManager.findQueue(
            PROCESSING_QUEUE).getJobs());
        for (JobInProgress jip : jip_process) {
          runningStaffLoad += jip.getNumBspStaff();
        }
      }
      ClusterStatus clusterStatus = controller.getClusterStatus(false);
      double staffsLoadFactor = ((double)
          (remainingStaffsLoad + runningStaffLoad)) /
         clusterStatus.getMaxClusterStaffs();
      // begin scheduling all staff(s) for the chosen job
      StaffInProgress[] staffs = job.getStaffInProgress();
      Collection<WorkerManagerStatus> wmlist = controller
          .workerServerStatusKeySet();
      try {
        haLogOperator.serializeWorkerManagerStatus(
            conf.get(Constants.BC_BSP_HA_LOG_DIR) + job.getJobID().toString(),
            wmlist, staffsLoadFactor);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        //LOG.error("Failed to serialize WorkerManagerStatus" + e.getMessage());
        throw new RuntimeException("Failed to serialize" +
             "WorkerManagerStatus", e);
      }
      HashMap<WorkerManagerStatus, Directive> assignTable = new
          HashMap<WorkerManagerStatus, Directive>();
      for (int i = 0; i < staffs.length; i++) {
        if (!staffs[i].isRunning() && !staffs[i].isComplete()) {
          Staff t = job.obtainNewStaff(wmlist, i, staffsLoadFactor);
          WorkerManagerStatus wmss = staffs[i].getWorkerManagerStatus();
          LaunchStaffAction action = new LaunchStaffAction(t);
          if (assignTable.containsKey(wmss)) {
            assignTable.get(wmss).addAction(action);
          } else {
            Directive d = new Directive(
                controller.getActiveWorkerManagersName(),
                new ArrayList<WorkerManagerAction>());
            d.addAction(action);
            assignTable.put(wmss, d);
          }
          job.updateStaffStatus(staffs[i], new StaffStatus(job.getJobID(),
              staffs[i].getStaffID(), 0, StaffStatus.State.UNASSIGNED,
              "running", wmss.getWorkerManagerName(),
              StaffStatus.Phase.STARTING));
          // update the WorkerManagerStatus Cache
          wmss.setRunningStaffsCount(wmss.getRunningStaffsCount() + 1);
          controller.updateWhiteWorkerManagersKey(wmss, wmss);
          LOG.info(t.getStaffAttemptId() + " is divided to the " +
             wmss.getWorkerManagerName());
        }
      }
      // dispatch staff to the corresponding workermanager.
      // ArrayList<StaffInProgress> faultStaffs = new
      // ArrayList<StaffInProgress>();
      for (Entry<WorkerManagerStatus, Directive> e : assignTable.entrySet()) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
          WorkerManagerProtocol worker = controller.findWorkerManager(e
              .getKey());
          try {
            long startTime = System.currentTimeMillis();
            worker.dispatch(job.getJobID(), e.getValue(), false, false,
                job.getNumAttemptRecovery());
            long endTime = System.currentTimeMillis();
            LOG.info("assign staffs to " + e.getKey().getWorkerManagerName() +
               " cost " + (endTime - startTime) + " ms");
          } catch (IOException ioe) {
            LOG.error("Fail to assign staffs to " +
               e.getKey().getWorkerManagerName());
            job.addBlackListWorker(e.getKey());
            worker.addFailedJob(job.getJobID());
            if (worker.getFailedJobCounter() > controller
                .getMaxFailedJobOnWorker()) {
              controller.removeWorkerFromWhite(e.getKey()); // white
              e.getKey().setPauseTime(System.currentTimeMillis());
              controller.addWorkerToGray(e.getKey(), worker); // gray
              LOG.info(e.getKey().getWorkerManagerName() +
                 " will be transferred from [WhiteList] to [GrayList]");
            }
            /*
             * for (WorkerManagerAction action : e.getValue().getActionList()) {
             * Staff staff = ((LaunchStaffAction)action).getStaff(); for
             * (StaffInProgress sip : staffs) { if
             * (sip.getStaffID().equals(staff.getStaffID())) {
             * faultStaffs.add(sip); break; } } }
             */
            LOG.error(
                "Exception has been catched in SimpleStaffScheduler--" +
                "normalSchedule !",
                ioe);
            Fault f = new Fault(Fault.Type.WORKERNODE, Fault.Level.WARNING,
                job.getJobID().toString(), ioe.toString());
            job.getController().recordFault(f);
            job.getController().recovery(job.getJobID());
            try {
              job.getController().killJob(job.getJobID());
            } catch (IOException ioe2) {
              LOG.error("Kill Job", ioe2);
            }
          }
        } else {
          LOG.warn("Currently master only shcedules job in running state. " +
             "This may be refined in the future. JobId:" + job.getJobID());
        } // if-else
      } // for
      job.getGssc().setCheckNumBase();
      job.getGssc().start();
    } // schedule

    /**
     * shcedule job when job is in recovery state.
     * @param job
     *        jobInProgress to be scheduled. 
     */
    public void recoverySchedule(JobInProgress job) {
      int remainingStaffsLoad = job.getNumBspStaff();
      List<JobInProgress> jip_list;
      // add lock to WAIT_QUEUE
      for (int i = 0; i < waitQueues.length; i++) {
        synchronized (waitQueues[i]) {
          jip_list = new ArrayList<JobInProgress>(queueManager.findQueue(
              waitQueues[i]).getJobs());
          // calculate the load-factor
          for (JobInProgress jip : jip_list) {
            remainingStaffsLoad += jip.getNumBspStaff();
          }
        }
      }
      /*
       * synchronized(WAIT_QUEUE){ jip_list = new
       * ArrayList<JobInProgress>(queueManager.findQueue(WAIT_QUEUE).getJobs());
       * } //calculate the load-factor for(JobInProgress jip : jip_list) {
       * remainingStaffsLoad += jip.getNumBspStaff(); }
       */
      ClusterStatus clusterStatus = controller.getClusterStatus(false);
      @SuppressWarnings("unused")
      double staffsLoadFactor = ((double) remainingStaffsLoad) /
         clusterStatus.getMaxClusterStaffs();
      Collection<WorkerManagerStatus> glist = controller
          .workerServerStatusKeySet();
      LOG.info("recoverySchedule--glist.size(): " + glist.size());
      WorkerManagerStatus[] gss = (WorkerManagerStatus[]) glist
          .toArray(new WorkerManagerStatus[glist.size()]);
      LOG.info("recoverySchedule-- WorkerManagerStatus[] gss.size: " +
         gss.length + "gss[0]: " + gss[0].getWorkerManagerName());
      StaffInProgress[] staffs = job.getStaffInProgress();
      for (int i = 0; i < staffs.length; i++) {
        WorkerManagerProtocol worker = null;
        boolean success = false;
        try {
          if (staffs[i].getStaffStatus(staffs[i].getStaffID()).getRunState() ==
              StaffStatus.State.WORKER_RECOVERY) {
            LOG.info("recoverySchedule ----WORKER_RECOVERY");
            job.obtainNewStaff(gss, i, 1.0, true);
            worker = controller.findWorkerManager(staffs[i]
                .getWorkerManagerStatus());
            ArrayList<WorkerManagerAction> actionList = new
                ArrayList<WorkerManagerAction>();
            actionList.add(new LaunchStaffAction(staffs[i].getS()));
            Directive d = new Directive(
                controller.getActiveWorkerManagersName(), actionList);
            d.setFaultSSStep(job.getFaultSSStep());
            if (staffs[i].getChangeWorkerState() == true) {
              worker.dispatch(staffs[i].getS().getJobID(), d, true, true,
                  job.getNumAttemptRecovery());
            } else {
              worker.dispatch(staffs[i].getS().getJobID(), d, true, false,
                  job.getNumAttemptRecovery());
            }
            // update the WorkerManagerStatus Cache
            WorkerManagerStatus new_gss = staffs[i].getWorkerManagerStatus();
            int currentStaffsCount = new_gss.getRunningStaffsCount();
            new_gss.setRunningStaffsCount((currentStaffsCount + 1));
            controller.updateWhiteWorkerManagersKey(
                staffs[i].getWorkerManagerStatus(), new_gss);
            LOG.info(staffs[i].getS().getStaffAttemptId() +
               " is divided to the " + new_gss.getWorkerManagerName());
            success = true;
          } else if (staffs[i].getStaffStatus(staffs[i].getStaffID())
              .getRunState() == StaffStatus.State.STAFF_RECOVERY) {
            LOG.info("recoverySchedule ----STAFF_RECOVERY");
            Map<String, Integer> workerManagerToTimes = job.getStaffToWMTimes()
                .get(staffs[i].getStaffID());
            @SuppressWarnings("unused")
            String lastWMName = getTheLastWMName(workerManagerToTimes);
            //biyahui revised
            //job.obtainNewStaff(gss, i, 1.0, true);
            job.obtainNewStaffNew(gss, i, 1.0, true,lastWMName);
            worker = controller.findWorkerManager(staffs[i]
                .getWorkerManagerStatus());
            ArrayList<WorkerManagerAction> actionList = new
                ArrayList<WorkerManagerAction>();
            actionList.add(new LaunchStaffAction(staffs[i].getS()));
            Directive d = new Directive(
                controller.getActiveWorkerManagersName(), actionList);
            d.setFaultSSStep(job.getFaultSSStep());
            if (staffs[i].getChangeWorkerState() == true) {
              success = worker.dispatch(staffs[i].getS().getJobID(), d, true,
                  true, job.getNumAttemptRecovery());
            } else {
              success = worker.dispatch(staffs[i].getS().getJobID(), d, true,
                  false, job.getNumAttemptRecovery());
            }
            // update the WorkerManagerStatus Cache
            WorkerManagerStatus new_gss = staffs[i].getWorkerManagerStatus();
            int currentStaffsCount = new_gss.getRunningStaffsCount();
            new_gss.setRunningStaffsCount((currentStaffsCount + 1));
            controller.updateWhiteWorkerManagersKey(
                staffs[i].getWorkerManagerStatus(), new_gss);
            LOG.info(staffs[i].getS().getStaffAttemptId() +
               " is divided to the " + new_gss.getWorkerManagerName());
          }
        } catch (Exception e) {
          WorkerManagerStatus wms = staffs[i].getWorkerManagerStatus();
          LOG.error("Fail to assign staff-" + staffs[i].getStaffId() + " to " +
             wms.getWorkerManagerName());
          if (!success) {
            job.addBlackListWorker(staffs[i].getWorkerManagerStatus());
            worker.addFailedJob(job.getJobID());
            if (worker.getFailedJobCounter() > controller
                .getMaxFailedJobOnWorker()) {
              controller.removeWorkerFromWhite(wms); // white
              wms.setPauseTime(System.currentTimeMillis());
              controller.addWorkerToGray(wms, worker); // gray
              LOG.info(wms.getWorkerManagerName() +
                 " will be transferred from [WhiteList] to [GrayList]");
            }
            i--;
          } else {
            LOG.error(
                "Exception has been catched in SimpleStaffScheduler--" +
                "recoverySchedule !",
                e);
            Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING,
                job.getJobID().toString(), e.toString());
            job.getController().recordFault(f);
            job.getController().recovery(job.getJobID());
            try {
              job.getController().killJob(job.getJobID());
            } catch (IOException oe) {
              LOG.error("Kill Job", oe);
            }
          }
        }
      }
      job.getStatus().setRecovery(true);
      job.getRecoveryBarrier(job.getWMNames());
      LOG.warn("leave---jip.getRecoveryBarrier(ts, WMNames.size());");
      while (true) {
        try {
          if (job.isCommandBarrier()) {
            LOG.info("[recoverySchedule] quit");
            break;
          }
          Thread.sleep(1000);
        } catch (Exception e) {
          Fault f = new Fault(Fault.Type.SYSTEMSERVICE,
              Fault.Level.INDETERMINATE, job.getJobID(), e.toString());
          job.getController().recordFault(f);
          job.getController().recovery(job.getJobID());
          try {
            job.getController().killJob(job.getJobID());
          } catch (IOException ioe) {
            LOG.error("[Kill Job Exception]", ioe);
          }
        }
      }
    }

    /**
     * get the fault staff last launched worker.
     * @param map
     *        workerManagerToTimes map
     * @return
     *        last launched worker name.
     */
    private String getTheLastWMName(Map<String, Integer> map) {
      String lastLWName = null;
      int lastLaunchWorker = 0;
      int i = 0;
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
      LOG.info("last---getTheLastWMName(Map<String, Integer> map:)" + " " +
         lastLWName);
      return lastLWName;
    }
  } // JobProcessor

  /**
   * simplestaffscheduler construct method,initialize syncServer jobListenner
   * and jobProcessor thread.
   */
  public SimpleStaffScheduler() {
    this.syncServer = new SynchronizationServer();
    this.jobListener = new JobListener();
    this.jobProcessor = new JobProcessor();
  }

  /**
   * start: create queues for job and the root directory on the ZooKeeper
   * cluster, and then start the simple scheduler thread
   */
  @Override
  public void start() {
    this.queueManager = new QueueManager(getConf(), role);
    // LOG.info("after  this.queueManager = new
    //QueueManager(getConf(),role); and role is "+role);
    /*
     * this.queueManager.createHRNQueue(WAIT_QUEUE);
     * this.queueManager.createHRNQueue(PROCESSING_QUEUE);
     * this.queueManager.createHRNQueue(FINISHED_QUEUE);
     * this.queueManager.createHRNQueue(FAILED_QUEUE);
     */
    // this.controller.addJobInProgressListener(this.jobListener);
    // this.initialized = true;
    this.queueManager.createHRNQueue(HIGHER_WAIT_QUEUE);
    this.queueManager.createHRNQueue(HIGH_WAIT_QUEUE);
    this.queueManager.createHRNQueue(NORMAL_WAIT_QUEUE);
    this.queueManager.createHRNQueue(LOW_WAIT_QUEUE);
    this.queueManager.createHRNQueue(LOWER_WAIT_QUEUE);
    this.queueManager.createFCFSQueue(PROCESSING_QUEUE);
    this.queueManager.createFCFSQueue(FINISHED_QUEUE);
    this.queueManager.createFCFSQueue(FAILED_QUEUE);
    this.controller.addJobInProgressListener(this.jobListener);
    this.initialized = true;
    // start the Synchronization Server and Scheduler Server.
    // LOG.info("before   this.syncServer.startServer();");
    if (role.equals(BspControllerRole.ACTIVE)) {
      this.syncServer.startServer();
      this.jobProcessor.start();
      // LOG.info("jobProcessor have started");
      // LOG.info("in  if(role.equals(BspControllerRole.ACTIVE))");
    }
  }

  // add by chen
  @Override
  public void jobProcessorStart() {
    this.jobProcessor.start();
  }

  /**
   * terminate: cleanup when close the cluster. Include: remove the
   * jobLinstener, delete the root directory on ZooKeeper, and stop the
   * scheduler thread
   */
  @SuppressWarnings("deprecation")
  @Override
  public void stop() {
    this.initialized = false;
    this.jobProcessor.stop();
    // boolean isSuccess = this.syncServer.stopServer();
    // if (isSuccess) {
    // LOG.info("Success to cleanup the nodes on ZooKeeper");
    // } else {
    // LOG.error("Fail to cleanup the nodes on ZooKeeper");
    // }
    if (this.jobListener != null) {
      this.controller.removeJobInProgressListener(this.jobListener);
    }
  }

  @Override
  public Collection<JobInProgress> getJobs(String queue) {
    return (queueManager.findQueue(queue)).getJobs();
  }
}
