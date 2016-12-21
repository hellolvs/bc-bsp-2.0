/**
 * CopyRight by Chinamobile
 *
 * JobSubmissionProtocol.java
 */

package com.chinamobile.bcbsp.jobSubmissionProtocol;

import com.chinamobile.bcbsp.Constants.BspControllerRole;
import com.chinamobile.bcbsp.bspcontroller.ClusterStatus;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.rpc.BSPRPCProtocolVersion;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.JobProfile;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffStatus;

import java.io.IOException;

/**
 * JobSubmissionProtocol Protocol that a groom server and the central BSP Master
 * use to communicate. This interface will contains several methods: submitJob,
 * killJob, and killStaff.
 *
 *
 *
 */
public interface JobSubmissionProtocol extends BSPRPCProtocolVersion {

  /**
   * Allocate a new id for the job.
   *
   * @return job id
   * @throws IOException
   */
  BSPJobID getNewJobId() throws IOException;

  /**
   * Submit a Job for execution. Returns the latest profile for that job. The
   * job files should be submitted in <b>system-dir</b>/<b>jobName</b>.
   *
   * @param jobID
   *        BSPJobID
   * @param jobFile
   *        String
   * @return jobStatus
   * @throws IOException
   */
  JobStatus submitJob(BSPJobID jobID, String jobFile) throws IOException;

  /**
   * Get the current status of the cluster
   *
   * @param detailed
   *        if true then report groom names as well
   * @return summary of the state of the cluster
   */
  ClusterStatus getClusterStatus(boolean detailed) throws IOException;

  /**
   * Grab a handle to a job that is already known to the BSPController.
   * @param jobid
   *        BSPJobID
   * @return Profile of the job, or null if not found.
   */
  JobProfile getJobProfile(BSPJobID jobid) throws IOException;

  /**
   * Grab a handle to a job that is already known to the BSPController.
   *@param jobid
   *        BSPJobID
   * @return Status of the job, or null if not found.
   */
  JobStatus getJobStatus(BSPJobID jobid) throws IOException;

  /**
   * A BSP system always operates on a single filesystem. This function returns
   * the fs name. ('local' if the localfs; 'addr:port' if dfs). The client can
   * then copy files into the right locations prior to submitting the job.
   * @return
   *       the fs name
   */
  String getFilesystemName() throws IOException;

  /**
   * Get the jobs that are not completed and not failed
   *
   * @return array of JobStatus for the running/to-be-run jobs.
   */
  JobStatus[] jobsToComplete() throws IOException;

  /**
   * Get all the jobs submitted.
   *
   * @return array of JobStatus for the submitted jobs
   */

  JobStatus[] getAllJobs() throws IOException;

  /**
   * get all the staffStatus AttemptID submitted. by yjc
   * @param jobId
   *        BSPJobID
   * @return
   *         all the staffStatus AttemptID submitted
   * @throws IOException
   */
  StaffAttemptID[] getStaffStatus(BSPJobID jobId) throws IOException;

  /**
   * get staffStatus detail information
   * @param jobId
   *        BSPJobID
   * @return
   *        staffStatus detail information
   * @throws IOException
   */
  StaffStatus[] getStaffDetail(BSPJobID jobId) throws IOException;

  /**
   * set checkpoint frquency
   *
   *
   * @param jobID
   *        BSPJobID
   * @param cf
   *        int
   * @throws IOException
   */
  void setCheckFrequency(BSPJobID jobID, int cf) throws IOException;

  /**
   * Command the job to execute the checkpoint operation at the next superstep.
   *
   * @param jobId
   *        BSPJobID
   * @throws IOException
   */
  void setCheckFrequencyNext(BSPJobID jobId) throws IOException;

  /**
   * Grab the BSPController system directory path where job-specific files are
   * to be placed.
   *
   * @return the system directory where job-specific files are to be placed.
   */
  String getSystemDir();

  /**
   * Kill the indicated job
   * @param jobid
   *        BSPJobID
   */
  void killJob(BSPJobID jobid) throws IOException;

  /**
   * Kill indicated staff attempt.
   *
   * @param staffId
   *        the id of the staff to kill.
   * @param shouldFail
   *        if true the staff is failed and added to failed staffs list,
   *        otherwise it is just killed, w/o affecting job failure status.
   * @return
   *        true or false
   */
  boolean killStaff(StaffAttemptID staffId, boolean shouldFail)
      throws IOException;

  /**
   * recovery for the controller's fault before local compute
   * @param jobId
   *        BSPJobID
   * @return
   *        true or false
   */
  boolean recovery(BSPJobID jobId);

  /**
   * record the fault message into specified file
   * @param f
   *        Fault
   */
  void recordFault(Fault f);

  // add by chen
  /**
   * Get the current role of BspController
   *
   * @return BspControllerRole
   */
  BspControllerRole getRole();

  // add by chen
  /**
   * Get Counters from BspController
   *
   * @param jobid
   *        BSPJobID
   * @return Counters
   */
  Counters getCounters(BSPJobID jobid);

}
