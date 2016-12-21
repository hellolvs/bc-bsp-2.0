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

package com.chinamobile.bcbsp.client;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.bspcontroller.ClusterStatus;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.Constants.BspControllerRole;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.rpc.JobSubmissionProtocol;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFSDataOutputStream;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFsPermission;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFSDataOutputStreamImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFspermissionImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPZookeeperImpl;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.BSPJob.JobState;
import com.chinamobile.bcbsp.util.JobProfile;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.Date;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * BSPJobClient BSPJobClient is the primary interface for the user-job to
 * interact with the BSPController. BSPJobClient provides facilities to submit
 * jobs, track their progress, access component-staffs' reports/logs, get the
 * BC-BSP cluster status information etc.
 */
public class BSPJobClient extends Configured implements Tool, Watcher {
  /** Current split file version */
  private static int CURRENT_SPLIT_FILE_VERSION = 0;
  /** Split file header */
  private static final byte[] SPLIT_FILE_HEADER = "SPL".getBytes();
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(BSPJobClient.class);
  /** Define StaffStatusFilter for storing staffStatus */
  public static enum StaffStatusFilter {
    /** Staff state */
    NONE, KILLED, FAILED, SUCCEEDED, ALL
  }
  /** Define BSPZookeeper */
  private BSPZookeeper bspzk = null;
  /** Define InetSocketAddress */
  private InetSocketAddress bspControllerAddr = null;
  /** Define Configuration */
  private Configuration conf = null;
  /** Init Object mutex */
  private Object mutex = new Object();
  /** Define CountDownLatch */
  private CountDownLatch connectedLatch = null;

  /**
   * Define NetWorkedJob
   */
  public class NetworkedJob implements RunningJob {
    /** A JobProfile tracks job's status */
    private JobProfile profile;
    /** Job's status */
    private JobStatus status;
    /** Current time */
    private long statuStime;

    /**
     *  Constructor
     *  @param job JobStatus
     */
    public NetworkedJob(JobStatus job) throws IOException {
      this.status = job;
      this.profile = jobSubmitClient.getJobProfile(job.getJobID());
      this.statuStime = System.currentTimeMillis();
    }

    /**
     * Some methods rely on having a recent job profile object. Refresh it, if
     * necessary.
     */
    synchronized void ensureFreshStatus() throws IOException {
      /** Define MAX_JOBPROFILE_AGE */
      long maxJobProfileAge = 1000 * 2;
      if (System.currentTimeMillis() - statuStime > maxJobProfileAge) {
        updateStatus();
      }
    }

    /**
     * Some methods need to update status immediately. So, refresh immediately.
     * @throws IOException
     */
    synchronized void updateStatus() throws IOException {
      BSPJobClient.this.monitorZooKeeper();
      try {
        ensureFreshjJobSubmitClient();
        this.status = jobSubmitClient.getJobStatus(profile.getJobID());
        while (this.status == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            LOG.error("[updateStatus]", e1);
          }
          this.status = jobSubmitClient.getJobStatus(profile.getJobID());
        }
        this.statuStime = System.currentTimeMillis();
      } catch (IOException e) {
        this.statuStime = System.currentTimeMillis();
        synchronized (mutex) {
          try {
            LOG.info("WARNING:IF the client long time no refresh ," +
               "please shutDown the client and submit your job again!");
            mutex.wait();
          } catch (InterruptedException e1) {
            //LOG.error("[updateStatus]", e1);
            throw new RuntimeException("BSPJobClient updateStatus Exception",
                e1);
          }
        }
      }
    }

    /**
     * @see com.chinamobile.bcbsp.bsp.RunningJob#getID()
     * @return jobID
     */
    @Override
    public BSPJobID getID() {
      return profile.getJobID();
    }

    /**
     * @see com.chinamobile.bcbsp.bsp.RunningJob#getJobName()
     * @return jobName
     */
    @Override
    public String getJobName() {
      return profile.getJobName();
    }

    /**
     * @see com.chinamobile.bcbsp.bsp.RunningJob#getJobFile()
     * @return jobFile
     */
    @Override
    public String getJobFile() {
      return profile.getJobFile();
    }

    @Override
    public long progress() throws IOException {
      ensureFreshStatus();
      return status.progress();
    }

    public Counters getCounters() {
      return jobSubmitClient.getCounters(profile.getJobID());
    }

    @Override
    public boolean isComplete() throws IOException {
      updateStatus();
      if (status.getRunState() == JobStatus.SUCCEEDED) {
        return true;
      }
      if (status.getRunState() == JobStatus.FAILED) {
        return true;
      }
      if (status.getRunState() == JobStatus.KILLED) {
        return true;
      }
      return false;
    }

    @Override
    public boolean isSuccessful() throws IOException {
      return status.getRunState() == JobStatus.SUCCEEDED;
    }

    @Override
    public boolean isKilled() throws IOException {
      return status.getRunState() == JobStatus.KILLED;
    }

    @Override
    public boolean isFailed() throws IOException {
      return status.getRunState() == JobStatus.FAILED;
    }

    @Override
    public boolean isRecovery() throws IOException {
      return status.getRunState() == JobStatus.RECOVERY;
    }

    /**
     * Get current superStep Number.
     * @return superStepNumber
     */
    public synchronized long getSuperstepCount() throws IOException {
      ensureFreshStatus();
      return status.getSuperstepCount();
    }

    /**
     * Block until the job is finished.
     */
    public void waitForCompletion() throws IOException {
      while (!isComplete()) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          //LOG.error("[waitForCompletion]", ie);
          throw new RuntimeException("BSPJobClient waitForCompletion " +
            "Exception", ie);
        }
      }
    }

    /**
     * Tell the service to get the state of the current job.
     * @return get the run state of job
     */
    public synchronized int getJobState() throws IOException {
      updateStatus();
      return status.getRunState();
    }

    /**
     * Tell the service to terminate the current job.
     */
    public synchronized void killJob() throws IOException {
      jobSubmitClient.killJob(getID());
    }

    @Override
    public void killStaff(StaffAttemptID staffId, boolean shouldFail)
        throws IOException {
      jobSubmitClient.killStaff(staffId, shouldFail);
    }
  }

  /**
   * New split Comparator.
   */
  private static class NewSplitComparator implements
      Comparator<org.apache.hadoop.mapreduce.InputSplit> {

    @Override
    public int compare(org.apache.hadoop.mapreduce.InputSplit o1,
        org.apache.hadoop.mapreduce.InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("exception in compare", ie);
      } catch (InterruptedException ie) {
        throw new RuntimeException("exception in compare", ie);
      }
    }
  }

  /**
   * New raw split.
   */
  public static class RawSplit implements Writable {
    /** Define variable splitClass */
    private String splitClass;
    /** Define BytesWritable */
    private BytesWritable bytes = new BytesWritable();
    /** Used for storing location */
    private String[] locations;
    /** The length of length */
    private long dataLength;

    /** Set bytes according to offset and length.
     *  @param data byte array
     *  @param offset where the data is supposed to begin
     *  @param length the length of data
     */
    public void setBytes(byte[] data, int offset, int length) {
      bytes.set(data, offset, length);
    }

    public void setClassName(String className) {
      splitClass = className;
    }

    public String getClassName() {
      return splitClass;
    }

    public BytesWritable getBytes() {
      return bytes;
    }

    /**
     * Clear the bytes array.
     */
    public void clearBytes() {
      bytes = null;
    }

    public void setLocations(String[] locations) {
      this.locations = locations;
    }

    public String[] getLocations() {
      return locations;
    }

    /**
     * Read splitClass,dataLength and bytes array.
     * @param in the stream to read from
     */
    public void readFields(DataInput in) throws IOException {
      splitClass = Text.readString(in);
      dataLength = in.readLong();
      bytes.readFields(in);
      int len = WritableUtils.readVInt(in);
      locations = new String[len];
      for (int i = 0; i < len; ++i) {
        locations[i] = Text.readString(in);
      }
    }

    /**
     * Write splitClass,dataLength and bytes array.
     * @param out the stream to write in
     */
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, splitClass);
      out.writeLong(dataLength);
      bytes.write(out);
      WritableUtils.writeVInt(out, locations.length);
      for (int i = 0; i < locations.length; i++) {
        Text.writeString(out, locations[i]);
      }
    }

    public long getDataLength() {
      return dataLength;
    }

    public void setDataLength(long l) {
      dataLength = l;
    }
  }
  /** Define JobSubmissionProtocol */
  private JobSubmissionProtocol jobSubmitClient = null;
  /** Define System directory */
  private Path sysDir = null;
  /** Define FileSystem */
  private FileSystem fs = null;

  /**
   * BSP Job Client.
   * @param conf job configuration information
   */
  public BSPJobClient(Configuration conf) throws IOException {
    setConf(conf);
    init(conf);
    this.closeZooKeeper();
  }

  /**
   * BSP Job Client.
   * @param conf job configuration information
   * @param commandLine console command
   */
  public BSPJobClient(Configuration conf, boolean commandLine)
      throws IOException {
    setConf(conf);
    init(conf);
    this.closeZooKeeper();
  }

  /**
   * Constructor
   */
  public BSPJobClient() {
  }

  /**
   * Init configuration information.
   * @param conf Configuration
   */
  public void init(Configuration conf) throws IOException {
    this.conf = conf;
    this.bspzk = getZooKeeper();
    ensureFreshjJobSubmitClient();
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType().toString().equals("NodeDeleted")) {
      LOG.info("Now the BspController will change");
      ensureFreshjJobSubmitClient();
      synchronized (mutex) {
        mutex.notify();
      }
    }
    // add for ZooKeeper connection Loss bug
    if (event.getState() == KeeperState.SyncConnected) {
      this.connectedLatch.countDown();
    }
  }

  /**
   * Used for get BSPZookeeper.
   * @return bspZookeeper
   */
  private BSPZookeeper getZooKeeper() {
    try {
      if (this.bspzk == null) {
        // add for ZooKeeper Connection Loss bug
        String zkAddress = conf.get(Constants.ZOOKEEPER_QUORUM) + ":" +
            conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
                Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
        this.connectedLatch = new CountDownLatch(1);
        this.bspzk = new BSPZookeeperImpl(zkAddress,
            Constants.SESSION_TIME_OUT, this);
        this.zkWaitConnected(bspzk);
        return bspzk;
      } else {
        return this.bspzk;
      }
    } catch (IOException e) {
      //LOG.error("[getZooKeeper]", e);
      throw new RuntimeException("exception in getZooKeeper", e);
      //return null;
    }
  }

  /**
   * BSPZookeeper wait connected.
   * @param bspzk BSPZookeeper
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

  /**
   * Close the <code>JobClient</code>.
   */
  public synchronized void close() throws IOException {
    RPC.stopProxy(jobSubmitClient);
  }

  /**
   * Get a fileSystem handle. We need this to prepare jobs for submission to the
   * BSP system.
   * @return the fileSystem handle.
   */
  public synchronized FileSystem getFs() throws IOException {
    if (this.fs == null) {
      Path systemDir = getSystemDir();
      this.fs = systemDir.getFileSystem(getConf());
    }
    return fs;
  }

  /**
   * Get the jobs that are submitted.
   * @return array of {@link JobStatus} for the submitted jobs.
   * @throws IOException
   */
  public JobStatus[] getAllJobs() throws IOException {
    return jobSubmitClient.getAllJobs();
  }

  public JobSubmissionProtocol getJobSubmitClient() {
    return jobSubmitClient;
  }

  /**
   * Get the jobs that are not completed and not failed.
   * @return array of {@link JobStatus} for the running/to-be-run jobs.
   * @throws IOException
   */
  public JobStatus[] jobsToComplete() throws IOException {
    return jobSubmitClient.jobsToComplete();
  }

  /**
   * Get the the current user's information.
   * @param conf Configuration
   * @return ugi UnixUserGroupInformation
   * @throws IOException
   */
  private UnixUserGroupInformation getUGI(Configuration conf)
      throws IOException {
    UnixUserGroupInformation ugi = null;
    try {
      ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException) (new IOException(
          "Failed to get the current user's information.").initCause(e));
    }
    return ugi;
  }

  /**
   * Submit a job to the BC-BSP system. This returns a handle to the
   * {@link RunningJob} which can be used to track the running-job.
   * @param job
   *        the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the
   *         running-job.
   * @throws FileNotFoundException
   * @throws IOException
   */
  public RunningJob submitJob(BSPJob job) throws
      ClassNotFoundException, InterruptedException, IOException {
    return submitJobInternal(job);
  }

  /**
   * Submit a new job to run.
   * @param job BSPJob
   * @return Review comments: (1)The content of submitJobDir is decided by the
   *         client. I think it is dangerous because two different clients maybe
   *         generate the same submitJobDir. Review time: 2011-11-30; Reviewer:
   *         Hongxu Zhang. Fix log: (1)In order to avoid the conflict, I use the
   *         jobId to generate the submitJobDir. Because the jobId is unique so
   *         this problem can be solved. Fix time: 2011-12-04; Programmer:
   *         Zhigang Wang. Review comments: (2)There, the client must submit
   *         relative information about the job. There maybe some exceptions
   *         during this process. When exceptions occur, this job should not be
   *         executed and the relative submitJobDir must be cleanup. Review
   *         time: 2011-12-04; Reviewer: Hongxu Zhang. Fix log: (2)The process
   *         of submiting files has been surrounded by try-catch. The
   *         submitJobDir will be cleanup in the catch process. Fix time:
   *         2011-12-04; Programmer: Zhigang Wang.
   */
  public RunningJob submitJobInternal(BSPJob job) {
    BSPJobID jobId = null;
    Path submitJobDir = null;
    try {
      jobId = jobSubmitClient.getNewJobId();
      submitJobDir = new Path(getSystemDir(), "submit_" + jobId.toString());
      Path submitJarFile = null;
      LOG.info("debug: job type is " + job.getJobType());
      if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(job.getJobType())) {
        submitJarFile = new Path(submitJobDir, "jobC");
        LOG.info("debug:" + submitJarFile.toString());
      } else {
        LOG.info("debug: before  submitJarFile = new " +
            "Path(submitJobDir,job.jar);");
        submitJarFile = new Path(submitJobDir, "job.jar");
        LOG.info("debug:" + submitJarFile.toString());
      }
      Path submitJobFile = new Path(submitJobDir, "job.xml");
      Path submitSplitFile = new Path(submitJobDir, "job.split");
      // set this user's id in job configuration, so later job files can
      // be accessed using this user's id
      UnixUserGroupInformation ugi = getUGI(job.getConf());
      // Create a number of filenames in the BSPController's fs namespace
      FileSystem files = getFs();
      files.delete(submitJobDir, true);
      submitJobDir = files.makeQualified(submitJobDir);
      submitJobDir = new Path(submitJobDir.toUri().getPath());
      BSPFsPermission bspSysPerms = new BSPFspermissionImpl(2);
      FileSystem.mkdirs(files, submitJobDir, bspSysPerms.getFp());
      files.mkdirs(submitJobDir);
      short replication = (short) job.getInt("bsp.submit.replication", 10);
      String originalJarPath = null;
      LOG.info("debug: job type is " + job.getJobType());
      if (Constants.USER_BC_BSP_JOB_TYPE_C.equals(job.getJobType())) {
        LOG.info("debug: originalJarPath = job.getJobExe();" + job.getJobExe());
        originalJarPath = job.getJobExe();
        LOG.info("debug:" + submitJarFile.toString());
        job.setJobExe(submitJarFile.toString());
      } else {
        LOG.info("debug: jar");
        originalJarPath = job.getJar();
        job.setJar(submitJarFile.toString());
      }
      if (originalJarPath != null) {
        // copy jar to BSPController's fs
        // use jar name if job is not named.
        if ("".equals(job.getJobName())) {
          job.setJobName(new Path(originalJarPath).getName());
        }
        // job.setJar(submitJarFile.toString());
        fs.copyFromLocalFile(new Path(originalJarPath), submitJarFile);
        fs.setReplication(submitJarFile, replication);
        fs.setPermission(submitJarFile, new BSPFspermissionImpl(0).getFp());
      } else {
        LOG.warn("No job jar file set.  User classes may not be found. " +
            "See BSPJob#setJar(String) or check Your jar file.");
      }
      // Set the user's name and working directory
      job.setUser(ugi.getUserName());
      if (ugi.getGroupNames().length > 0) {
        job.set("group.name", ugi.getGroupNames()[0]);
      }
      if (new BSPHdfsImpl().getWorkingDirectory() == null) {
        job.setWorkingDirectory(fs.getWorkingDirectory());
      }
      int maxClusterStaffs = jobSubmitClient.getClusterStatus(false)
          .getMaxClusterStaffs();
      if (job.getNumPartition() == 0) {
        job.setNumPartition(maxClusterStaffs);
      }
      if (job.getNumPartition() > maxClusterStaffs) {
        job.setNumPartition(maxClusterStaffs);
      }
      job.setNumBspStaff(job.getNumPartition());
      int splitNum = 0;
      splitNum = writeSplits(job, submitSplitFile);
      if (splitNum > job.getNumPartition() && splitNum <= maxClusterStaffs) {
        job.setNumPartition(splitNum);
        job.setNumBspStaff(job.getNumPartition());
      }
      if (splitNum > maxClusterStaffs) {
        LOG.error("Sorry, the number of files is more than maxClusterStaffs:" +
            maxClusterStaffs);
        throw new IOException("Could not launch job");
      }
      job.set(Constants.USER_BC_BSP_JOB_SPLIT_FILE, submitSplitFile.toString());
      LOG.info("[Max Staff Number] " + maxClusterStaffs);
      LOG.info("The number of splits for the job is: " + splitNum);
      LOG.info("The number of staffs for the job is: " + job.getNumBspStaff());
      BSPFSDataOutputStream bspout = new BSPFSDataOutputStreamImpl(fs,
          submitJobFile, new BSPFspermissionImpl(0).getFp());
      try {
        job.writeXml(bspout.getOut());
      } finally {
        bspout.close();
      }
      // Now, actually submit the job (using the submit name)
      JobStatus status = jobSubmitClient.submitJob(jobId,
          submitJobFile.toString());
      if (status != null) {
        return new NetworkedJob(status);
      } else {
        throw new IOException("Could not launch job");
      }
    } catch (FileNotFoundException fnfE) {
      LOG.error(
          "Exception has been catched in BSPJobClient--submitJobInternal !",
          fnfE);
      Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.INDETERMINATE,
          "null", fnfE.toString());
      jobSubmitClient.recordFault(f);
      jobSubmitClient.recovery(jobId);
      try {
        FileSystem files = getFs();
        files.delete(submitJobDir, true);
      } catch (IOException e) {
        //LOG.error("Failed to cleanup the submitJobDir:" + submitJobDir);
        throw new RuntimeException("Failed to cleanup the submitJobDir", e);
      }
      return null;
    } catch (ClassNotFoundException cnfE) {
      LOG.error(
          "Exception has been catched in BSPJobClient--submitJobInternal !",
          cnfE);
      Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.WARNING,
          "null", cnfE.toString());
      jobSubmitClient.recordFault(f);
      jobSubmitClient.recovery(jobId);
      try {
        FileSystem files = getFs();
        files.delete(submitJobDir, true);
      } catch (IOException e) {
        //LOG.error("Failed to cleanup the submitJobDir:" + submitJobDir);
        throw new RuntimeException("Failed to cleanup the submitJobDir", e);
      }
      return null;
    } catch (InterruptedException iE) {
      LOG.error(
          "Exception has been catched in BSPJobClient--submitJobInternal !",
          iE);
      Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL,
          "null", iE.toString());
      jobSubmitClient.recordFault(f);
      jobSubmitClient.recovery(jobId);
      try {
        FileSystem files = getFs();
        files.delete(submitJobDir, true);
      } catch (IOException e) {
        //LOG.error("Failed to cleanup the submitJobDir:" + submitJobDir);
        throw new RuntimeException("Failed to cleanup the submitJobDir", e);
      }
      return null;
    } catch (Exception ioE) {
      LOG.error(
          "Exception has been catched in BSPJobClient--submitJobInternal !",
          ioE);
      Fault f = new Fault(Fault.Type.DISK, Fault.Level.CRITICAL, "null",
          ioE.toString());
      jobSubmitClient.recordFault(f);
      jobSubmitClient.recovery(jobId);
      try {
        FileSystem files = getFs();
        files.delete(submitJobDir, true);
      } catch (IOException e) {
        //LOG.error("Failed to cleanup the submitJobDir:" + submitJobDir);
        throw new RuntimeException("Failed to cleanup the submitJobDir", e);
      }
      return null;
    }
  }

  /**
   * Write splits.
   * @param job BSPJob
   * @param submitSplitFile Path
   * @param <T> org.apache.hadoop.mapreduce.InputSplit
   * @return splitNum the count of split
   */
  @SuppressWarnings("unchecked")
  private <T extends org.apache.hadoop.mapreduce.InputSplit> int writeSplits(
      BSPJob job, Path submitSplitFile) throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration confs = job.getConf();
    com.chinamobile.bcbsp.io.InputFormat<?, ?> input = ReflectionUtils
        .newInstance(job.getInputFormatClass(), confs);
    input.initialize(job.getConf());
    List<org.apache.hadoop.mapreduce.InputSplit> splits = input.getSplits(job);
    int maxSplits = job.getNumPartition();
    int splitNum = splits.size();
    double factor = splitNum / (float) maxSplits;
    if (factor > 1.0) {
      job.setInt(Constants.USER_BC_BSP_JOB_SPLIT_FACTOR,
          (int) Math.ceil(factor));
      LOG.info("[Split Adjust Factor] " + (int) Math.ceil(factor));
      LOG.info("[Partition Num] " + maxSplits);
      splits = input.getSplits(job);
      splitNum = splits.size();
    }
    T[] array = (T[]) splits
        .toArray(new org.apache.hadoop.mapreduce.InputSplit[splits.size()]);
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(array, new NewSplitComparator());
    DataOutputStream out = writeSplitsFileHeader(confs, submitSplitFile,
        array.length);
    try {
      if (array.length != 0) {
        DataOutputBuffer buffer = new DataOutputBuffer();
        RawSplit rawSplit = new RawSplit();
        SerializationFactory factory = new SerializationFactory(confs);
        Serializer<T> serializer = factory.getSerializer((Class<T>) array[0]
            .getClass());
        serializer.open(buffer);
        for (T split : array) {
          rawSplit.setClassName(split.getClass().getName());
          buffer.reset();
          serializer.serialize(split);
          rawSplit.setDataLength(split.getLength());
          rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
          rawSplit.setLocations(split.getLocations());
          rawSplit.write(out);
        }
        serializer.close();
      }
    } finally {
      out.close();
    }
    return splitNum;
  }
  //private static int currentSplitFileVersion = 0;
  // private static final byte[] SPLIT_FILE_HEADER = "SPL".getBytes();
  /**
   * Write splits file header.
   * @param conf Configuration
   * @param filename path of file
   * @param length size of split
   * @return DataOutputStream
   * @throws IOException
   */
  private DataOutputStream writeSplitsFileHeader(Configuration conf,
      Path filename, int length) throws IOException {
    // write the splits to a file for the job tracker
    FileSystem files = filename.getFileSystem(conf);
    BSPFSDataOutputStream bspout = new BSPFSDataOutputStreamImpl(files,
        filename, new BSPFspermissionImpl(0).getFp());
    bspout.write(SPLIT_FILE_HEADER);
    WritableUtils.writeVInt(bspout.getOut(), CURRENT_SPLIT_FILE_VERSION);
    WritableUtils.writeVInt(bspout.getOut(), length);
    return bspout.getOut();
  }

  /**
   * Read a splits file into a list of raw splits.
   * @param in
   *        the stream to read from
   * @return the complete list of splits
   * @throws IOException
   *         NEU change in version-0.2.3 add new function
   */
  public static RawSplit[] readSplitFile(DataInput in) throws IOException {
    byte[] header = new byte[SPLIT_FILE_HEADER.length];
    in.readFully(header);
    if (!Arrays.equals(SPLIT_FILE_HEADER, header)) {
      throw new IOException("Invalid header on split file");
    }
    int vers = WritableUtils.readVInt(in);
    if (vers != CURRENT_SPLIT_FILE_VERSION) {
      throw new IOException("Unsupported split version " + vers);
    }
    int len = WritableUtils.readVInt(in);
    RawSplit[] result = new RawSplit[len];
    for (int i = 0; i < len; ++i) {
      result[i] = new RawSplit();
      result[i].readFields(in);
    }
    return result;
  }

  /**
   * Monitor a job and print status in real-time as progress is made and tasks
   * fail.
   * @param job BSPJob
   * @param info RunningJob
   * @return true, if job is successful
   * @throws IOException
   * @throws InterruptedException
   */
  public boolean monitorAndPrintJob(BSPJob job, RunningJob info)
      throws IOException, InterruptedException {
    String lastReport = null;
    LOG.info("Running job : " + info.getID());
    StringBuffer sb = new StringBuffer("JOB FINISHED");
    sb.append("\n******************************************************" +
        "*******");
    long startTime = System.currentTimeMillis();
    long step = 0;
    // the times try connect to BspController
    int times = 0;
    final int maxTimes = 1;
    while (!info.isComplete()) {
      try {
        Thread.sleep(3000);
        step = info.progress();
      } catch (IOException e) {
        times++;
        if (times > maxTimes) {
          LOG.info("ERROR:something happend when connect to BspController ," +
              "Now will break..");
          break;
        }
        LOG.info("WARN:something happend when connect to BspController ," +
            "Now will try again..");
        continue;
      }
      String report = "the current supersteps number : " + step;
      if (!report.equals(lastReport)) {
        LOG.info(report);
        lastReport = report;
      }
    }
    
    try {
      info.getCounters().log(LOG);
      if (info.isSuccessful()) {
        sb.append("\n    INFO       : The job is finished successfully");
      }
      if (info.isKilled()) {
        sb.append("\n    WARN       : The job is killed by user");
      }
      
      double totalTime = (System.currentTimeMillis() - startTime) / 1000.0;
      sb.append("\n    STATISTICS : Total supersteps   : " + info.progress());
      sb.append("\n                 Total time(seconds): " + totalTime);
      sb.append("\n****************************" +
          "*********************************");
      LOG.info(sb.toString());
      this.closeZooKeeper();
      return job.isSuccessful();
    } catch (Exception e) {
      sb.append("\n    ERROR      : " + e.getMessage());
      sb.append("\n    ERROR      : The job is viewed as killed by system");
      double totalTime = (System.currentTimeMillis() - startTime) / 1000.0;
      sb.append("\n    STATISTICS : Total supersteps    : " + lastReport);
      sb.append("\n                 Total time(seconds) : " + totalTime);
      sb.append("\n****************************" +
          "*********************************");
      LOG.info(sb.toString());
      
      return false;
    }
  }

  /**
   * Grab the controller system directory path where job-specific files are to
   * be placed.
   * @return the system directory where job-specific files are to be placed.
   */
  public Path getSystemDir() {
    if (sysDir == null) {
      sysDir = new Path(jobSubmitClient.getSystemDir());
    }
    return sysDir;
  }

  /**
   * run BSPJob
   * @param job BSPJob
   */
  public static void runJob(BSPJob job) throws
      ClassNotFoundException, InterruptedException, IOException {
    BSPJobClient jc = new BSPJobClient(job.getConf());
    RunningJob running = jc.submitJobInternal(job);
    job.setState(JobState.RUNNING);
    job.setInfo(running);
    BSPJobID jobId = running.getID();
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        //LOG.info("Occur interrupted exception");
        throw new RuntimeException("Occur interrupted exception", e);
      }
      if (running.isComplete()) {
        break;
      }
      running = jc.getJob(jobId);
      jc.monitorAndPrintJob(job, running);
    }
    jc.close();
  }

  /**
   * Get an RunningJob object to track an ongoing job. Returns null if the id
   * does not correspond to any known job.
   * @param jobId the id of BSPJob
   * @return netWorkedJob
   * @throws IOException
   */
  private RunningJob getJob(BSPJobID jobId) throws IOException {
    JobStatus status = jobSubmitClient.getJobStatus(jobId);
    if (status != null) {
      return new NetworkedJob(status);
    } else {
      return null;
    }
  }

  /**
   * Get status information about the BSP cluster.
   * @param detailed
   *        if true then get a detailed status including the groomserver names
   * @return the status information about the BSP cluster as an object of
   *         {@link ClusterStatus}.
   * @throws IOException
   */
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    return jobSubmitClient.getClusterStatus(detailed);
  }

  @SuppressWarnings("deprecation")
  @Override
  public int run(String[] args) throws Exception {
    int exitCode = -1;
    if (args.length < 1) {
      displayUsage("");
      return exitCode;
    }

    // process arguments
    String cmd = args[0];
    boolean listJobs = false;
    boolean listAllJobs = false;
    boolean listActiveWorkerManagers = false;
    boolean killJob = false;
    boolean submitJob = false;
    boolean getStatus = false;
    boolean listJobTasks = false;
    boolean listBspController = false;
    boolean setCheckPoint = false;
    String submitJobFile = null;
    String jobid = null;
    String checkpointCmd = null;
    //From console start BSPJobClient
    boolean commandLine = true;
    BSPConfiguration confs = new BSPConfiguration(getConf());
    init(confs);
    // judge the current role of BspController ,if the role is not active
    // the Systen can not provide service
    if (!jobSubmitClient.getRole().equals(BspControllerRole.ACTIVE)) {
      System.out
          .println("For taking over ,now the System can not " +
              "provide service for you,please wait...");
      return exitCode;
    }
    if ("-list".equals(cmd)) {
      if (args.length != 1 && !(args.length == 2 && "all".equals(args[1]))) {
        displayUsage(cmd);
        return exitCode;
      }
      if (args.length == 2 && "all".equals(args[1])) {
        listAllJobs = true;
      } else {
        listJobs = true;
      }
    } else if ("-workers".equals(cmd)) {
      if (args.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listActiveWorkerManagers = true;
    } else if ("-submit".equals(cmd)) {
      if (args.length == 1) {
        displayUsage(cmd);
        return exitCode;
      }
      submitJob = true;
      submitJobFile = args[1];
    } else if ("-kill".equals(cmd)) {
      if (args.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      killJob = true;
      jobid = args[1];
    } else if ("-status".equals(cmd)) {
      if (args.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = args[1];
      getStatus = true;
    } else if ("-list-staffs".equals(cmd)) {
      if (args.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = args[1];
      listJobTasks = true;
    } else if ("-setcheckpoint".equals(cmd)) {
      if (args.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = args[1];
      checkpointCmd = args[2];
      setCheckPoint = true;
    } else if ("-master".equals(cmd)) {
      if (args.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listBspController = true;
    } else if ("-kill-staff".equals(cmd)) {
      System.out
          .println("This function is not implemented yet.");
      return exitCode;
    } else if ("-fail-staff".equals(cmd)) {
      System.out
          .println("This function is not implemented yet.");
      return exitCode;
    }
    BSPJobClient jc = new BSPJobClient(new BSPConfiguration(), commandLine);
    if (listJobs) {
      listJobs();
      exitCode = 0;
    } else if (listAllJobs) {
      listAllJobs();
      exitCode = 0;
    } else if (listActiveWorkerManagers) {
      listActiveWorkerManagers();
      exitCode = 0;
    } else if (submitJob) {
      BSPConfiguration tConf = new BSPConfiguration(new Path(submitJobFile));
      RunningJob job = jc.submitJob(new BSPJob(tConf));
      System.out
          .println("Created job " + job.getID().toString());
    } else if (killJob) {
      RunningJob job = jc.getJob(new BSPJobID().forName(jobid));
      if (job == null) {
        System.out
            .println("Could not find job " + jobid);
      } else {
        job.killJob();
        System.out
            .println("Killed job " + jobid);
      }
      exitCode = 0;
    } else if (getStatus) {
      RunningJob job = jc.getJob(new BSPJobID().forName(jobid));
      if (job == null) {
        System.out
            .println("Could not find job " + jobid);
      } else {
        JobStatus jobStatus = jobSubmitClient.getJobStatus(job.getID());
        String start = "NONE";
        String finish = "NONE";
        if (jobStatus.getStartTime() != 0) {
          start = new Date(jobStatus.getStartTime()).toLocaleString();
        }
        if (jobStatus.getFinishTime() != 0) {
          finish = new Date(jobStatus.getFinishTime()).toLocaleString();
        }
        System.out.printf("States are:\n\tRunning : 1\tSucceded : 2" +
            "\tFailed : 3\tPrep : 4\n");
        System.out.printf("Job name: %s\tUserName: %s\n", job.getJobName(),
            jobStatus.getUsername());
        System.out.printf(
            "ID: %s\tState: %d\tSuperStep: %d\tStartTime: %s\tEndTime: %s\n",
            jobStatus.getJobID(), jobStatus.getRunState(),
            jobStatus.progress(), start, finish);
        exitCode = 0;
      }
    } else if (listJobTasks) {
      StaffAttemptID []id = jobSubmitClient.getStaffStatus(new BSPJobID()
          .forName(jobid));
      for (StaffAttemptID ids : id) {
        System.out
            .println(ids);
      }
      StaffStatus []ss = jobSubmitClient.getStaffDetail(new BSPJobID()
          .forName(jobid));
      System.out
          .println("array list size is" + ss.length);
    } else if (setCheckPoint) {
      if (checkpointCmd.equals("next")) {
        jobSubmitClient.setCheckFrequencyNext(new BSPJobID().forName(jobid));
      } else {
        jobSubmitClient.setCheckFrequency(new BSPJobID().forName(jobid),
            Integer.valueOf(checkpointCmd));
      }
    } else if (listBspController) {
      listBspController();
      exitCode = 0;
    }
    return 0;
  }
  /**
   * Export BspController informations.
   * @throws IOException
   */
  private void listBspController() throws IOException {
    ClusterStatus c = jobSubmitClient.getClusterStatus(true);
    System.out
        .println("Controller:" + this.bspControllerAddr.toString());
    System.out
        .println("Controller role is :" + jobSubmitClient.getRole());
    System.out
        .println("Controller state is :" + c.getBSPControllerState());
  }

  /**
   * Display usage of the command-line tool and terminate execution.
   * @param cmd command
   */
  private void displayUsage(String cmd) {
    String prefix = "Usage: bcbsp job ";
    String taskStates = "running, completed";
    if ("-submit".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-file>]");
    } else if ("-status".equals(cmd) || "-kill".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id>]");
    } else if ("-list".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " [all]]");
    } else if ("-kill-staff".equals(cmd) || "-fail-staff".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <staff-id>]");
    } else if ("-list-active-workermanagers".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "]");
    } else if ("-list-staffs".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <staff-state>]. " +
        "Valid values for <staff-state> are " + taskStates);
    } else {
      System.err.printf(prefix + "<command> <args>\n");
      System.err.printf("\t[-submit <job-file>]\n");
      System.err.printf("\t[-status <job-id>]\n");
      System.err.printf("\t[-kill <job-id>]\n");
      System.err.printf("\t[-list [all]]\n");
      System.err.printf("\t[-list-active-workermanagers]\n");
      System.err.println("\t[-list-attempt <job-id> " + "<staff-state>]\n");
      System.err.printf("\t[-kill-staff <staff-id>]\n");
      System.err.printf("\t[-fail-staff <staff-id>]\n\n");
    }
  }

  /**
   * Dump a list of currently running jobs.
   * @throws IOException
   */
  private void listJobs() throws IOException {
    JobStatus[] jobs = jobsToComplete();
    if (jobs == null) {
      jobs = new JobStatus[0];
    }
    System.out.printf("%d jobs currently running\n", jobs.length);
    displayJobList(jobs);
  }

  /**
   * Dump a list of all jobs submitted.
   * @throws IOException
   */
  private void listAllJobs() throws IOException {
    JobStatus[] jobs = getAllJobs();
    if (jobs == null) {
      jobs = new JobStatus[0];
    }
    System.out.printf("%d jobs submitted\n", jobs.length);
    System.out.printf("States are:\n\tRunning : 1\tSucceded : 2" +
        "\tFailed : 3\tPrep : 4\n");
    displayJobList(jobs);
  }
  /**
   * Display job list.
   * @param jobs JobStatus
   * @throws IOException
   */
  public void displayJobList(JobStatus[] jobs) {
    System.out.printf("JobId\tState\tStartTime\tUserName\n");
    for (JobStatus job : jobs) {
      System.out.printf("%s\t%d\t%d\t%s\n", job.getJobID(), job.getRunState(),
          job.getStartTime(), job.getUsername());
    }
  }

  /**
   * Display the list of active worker servers.
   */
  private void listActiveWorkerManagers() throws IOException {
    ClusterStatus c = jobSubmitClient.getClusterStatus(true);
    int runningClusterStaffs = c.getRunningClusterStaffs();
    String[] activeWorkerManagersName = c.getActiveWorkerManagersName();
    System.out
        .println("running ClusterStaffs is : " +
            runningClusterStaffs);
    for (String workerManagerName : activeWorkerManagersName) {
      System.out
          .println(workerManagerName + "      active");
    }
  }

  /**
   * main method.
   * @param args console parameters
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new BSPJobClient(), args);
    System.exit(res);
  }

  /**
   * Ensure fresh jobSubmitClient.
   */
  public void ensureFreshjJobSubmitClient() {
    try {
      if (bspzk != null) {
        Stat s = null;
        int count = 0;
        int max = 3;
        while (s == null && count < max) {
          count++;
          Thread.sleep(500);
          s = bspzk.exists(Constants.BSPCONTROLLER_LEADER, true);
        }
        if (s != null) {
          String controllerAddr = getData(Constants.BSPCONTROLLER_LEADER);
          InetSocketAddress newAddr = NetUtils.createSocketAddr(controllerAddr);
          if (this.bspControllerAddr == null ||
              !this.bspControllerAddr.equals(newAddr)) {
            this.bspControllerAddr = newAddr;
            // establish connection to new BspController again
            conf.set("ipc.client.connect.max.retries", "0");
            this.jobSubmitClient = (JobSubmissionProtocol) RPC.getProxy(
                JobSubmissionProtocol.class, JobSubmissionProtocol.versionID,
                bspControllerAddr, conf,
                NetUtils.getSocketFactory(conf, JobSubmissionProtocol.class));
            LOG.info("Now  connected to " + this.bspControllerAddr.toString());
          }
        }
      }
    } catch (Exception e) {
     // LOG.warn("lost connection to  " + this.bspControllerAddr.toString());
     // LOG.error("[ensureFreshjJobSubmitClient]", e);
      throw new RuntimeException("lost connection to bspControllerAddr ", e);
    }
  }

  /**
   * Get data from the path of ZooKeeper.
   * @param path  get data according to the path
   * @return data get data
   */
  public String getData(String path) throws KeeperException,
      InterruptedException {
    if (bspzk != null) {
      byte[] data = bspzk.getData(path, false, null);
      return new String(data);
    }
    return null;
  }

  /**
   * Close ZooKeeper.
   */
  public void closeZooKeeper() {
    if (this.bspzk != null) {
      try {
        this.bspzk.close();
        this.bspzk = null;
      } catch (InterruptedException e) {
        //LOG.error("[closeZooKeeper]", e);
        throw new RuntimeException("exception in closeZooKeeper ", e);
      }
    }
  }

  /**
   * Monitor ZooKeeper.
   */
  public void monitorZooKeeper() {
    if (bspzk == null) {
      try {
        bspzk = getZooKeeper();
        bspzk.exists(Constants.BSPCONTROLLER_LEADER, true);
      } catch (KeeperException e1) {
        LOG.error("[monitorZooKeeper]", e1);
      } catch (InterruptedException e1) {
        //e1.printStackTrace();
        //LOG.error("[monitorZooKeeper]", e1);
        throw new RuntimeException("exception in monitorZooKeeper ", e1);
      }
    }
  }

  /** For JUnit test. */
  public void setJobSubmitClient(JobSubmissionProtocol jobSubmitClient) {
	this.jobSubmitClient = jobSubmitClient;
  }
}
