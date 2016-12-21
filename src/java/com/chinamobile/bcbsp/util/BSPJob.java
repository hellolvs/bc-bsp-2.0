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

package com.chinamobile.bcbsp.util;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Aggregator;
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.api.assistCheckpoint;
import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.client.BSPJobClient;
import com.chinamobile.bcbsp.client.RunningJob;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.io.InputFormat;
import com.chinamobile.bcbsp.io.mysql.DBConfiguration;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.ml.Key;
import com.chinamobile.bcbsp.ml.KeyValuePair;
import com.chinamobile.bcbsp.ml.Value;
import com.chinamobile.bcbsp.partition.WritePartition;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

/**
 * BSPJob A BSP job configuration. BSPJob is the primary interface for a user to
 * describe a BSP job to the BC-BSP framework for execution.
 */
public class BSPJob extends BSPJobContext {
  /** 0 is job version for memory */
  public static final int MEMORY_VERSION = 0;
  /** 1 is job version for disk */
  public static final int DISK_VERSION = 1;
  /** 2 is job version for BDB */
  public static final int BDB_VERSION = 2;
  /** 3 is job version for ByteArray */
  public static final int BYTEARRAY_VERSION = 3;
  /** 4 is job version for Partition compute for machine learning */
  public static final int PEER_VERSION = 4;

  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(BSPJob.class);
  /** Current job state */
  public static enum JobState {
    /** DEFINE said initializes the job.  RUNNING said job running. */
    DEFINE, RUNNING
  };
  /** Job state is DEFINE */
  private JobState state = JobState.DEFINE;
  /** Statement of the jobClient */
  private BSPJobClient jobClient;
  /** The running of the job information */
  private RunningJob info;
  /** Number of Aggregators */
  private int aggregateNum = 0;
  /** Name of Aggregators */
  private ArrayList<String> aggregateNames = new ArrayList<String>();
  

  /** Constructor. */
  public BSPJob() throws IOException {
    this(new BSPConfiguration());
  }

  /**
   * Constructor.
   *
   * @param conf Job configuration
   */
  public BSPJob(BSPConfiguration conf) throws IOException {
    super(conf, null);
    jobClient = new BSPJobClient(conf);
  }

  /**
   * Constructor.
   *
   * @param conf Job configuration
   * @param jobName Job name
   */
  public BSPJob(BSPConfiguration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
    LOG.info("debug: in BspJob");
  }

  /**
   * Constructor.
   *
   * @param jobID Current job id
   * @param jobFile LocalJobFile xml
   */
  public BSPJob(BSPJobID jobID, String jobFile) throws IOException {
    super(new Path(jobFile), jobID);
  }

  /**
   * Constructor.
   *
   * @param conf Job configuration
   * @param exampleClass The user program class
   */
  public BSPJob(BSPConfiguration conf, Class<?> exampleClass)
      throws IOException {
    this(conf);
    setJarByClass(exampleClass);
  }

  /**
   * Constructor.
   *
   * @param conf Job configuration
   * @param numStaff The number of staff
   */
  public BSPJob(BSPConfiguration conf, int numStaff) {
    super(conf, null);
    this.setNumBspStaff(numStaff);
  }

  /**
   * Register an Aggregate map with an Aggregator and a Value.
   *
   * @param aggregateName Name of Aggregators.
   * @param aggregatorClass Class of Aggregator.
   * @param aggregateValueClass Class of AggregateValue.
   */
  public void registerAggregator(String aggregateName,
      Class<? extends Aggregator<?>> aggregatorClass,
      Class<? extends AggregateValue<?, ?>> aggregateValueClass) {
    conf.setClass(aggregateName + ".aggregator", aggregatorClass,
        Aggregator.class);
    conf.setClass(aggregateName + ".aggregateValue", aggregateValueClass,
        AggregateValue.class);
    this.aggregateNames.add(aggregateName);
    this.aggregateNum++;
  }

  /** Register all Aggregate map with AggregateNumber and AggregateName. */
  public void completeAggregatorRegister() {
    conf.setInt(Constants.USER_BC_BSP_JOB_AGGREGATE_NUM, this.aggregateNum);
    int size = this.aggregateNames.size();
    String[] aggNames = new String[size];
    for (int i = 0; i < size; i++) {
      aggNames[i] = this.aggregateNames.get(i);
    }
    conf.setStrings(Constants.USER_BC_BSP_JOB_AGGREGATE_NAMES, aggNames);
  }

  /** Get the current job AggregateNumber.
   *
   *  @return the current job AggregateNumber.
   */
  public int getAggregateNum() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_AGGREGATE_NUM, 0);
  }

  /** Get the current job AggregateNames.
   *
   *  @return the current job AggregateNames.
   */
  public String[] getAggregateNames() {
    return conf.getStrings(Constants.USER_BC_BSP_JOB_AGGREGATE_NAMES);
  }

  /** Get the current job AggregateClass.
   *
   *  @param aggregateName Current aggregateName.
   *
   *  @return the current job AggregateClass.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Aggregator<?>> getAggregatorClass(
      String aggregateName) {
    return (Class<? extends Aggregator<?>>) conf.getClass(aggregateName +
        ".aggregator", Aggregator.class);
  }

  /** Get the current job getAggregateValueClass.
  *
  *  @param aggregateName Current aggregateName.
  *
  *  @return the current job getAggregateValueClass.
  */
  @SuppressWarnings("unchecked")
  public Class<? extends AggregateValue<?, ?>> getAggregateValueClass(
      String aggregateName) {
    return (Class<? extends AggregateValue<?, ?>>) conf.getClass(aggregateName +
        ".aggregateValue", AggregateValue.class);
  }


  /**  @param state The state of the job. */
  private void ensureState(JobState state) {
    if (state != this.state) {
      throw new IllegalStateException("Job in state " + this.state +
          " instead of " + state);
    }
  }

  /** BC-BSP Job Configuration.
   *
   *Set job name.
   *
   *  @param name The name of the job.
   */
//  public void setJobName(String name) throws IllegalStateException {
  public void setJobName(String name) {
    ensureState(JobState.DEFINE);
    conf.set(Constants.USER_BC_BSP_JOB_NAME, name);
  }

  public String getJobName() {
    return conf.get(Constants.USER_BC_BSP_JOB_NAME, "");
  }

  /** Set job user name.
   *
   *  @param user The userName of the job.
   */
  public void setUser(String user) {
    conf.set(Constants.USER_BC_BSP_JOB_USER_NAME, user);
  }

  public String getUser() {
    return conf.get(Constants.USER_BC_BSP_JOB_USER_NAME);
  }

  /** Set Working directory.
   *
   *  @param dir The path of the HDSF.
   */
  public void setWorkingDirectory(Path dir) throws IOException {
    ensureState(JobState.DEFINE);
    dir = new BSPHdfsImpl().hdfsgetWorkingDirectory(dir);
    conf.set(Constants.USER_BC_BSP_JOB_WORKING_DIR, dir.toString());
  }

  /**
   * Set the {@link Partitioner} for the job.
   *
   * @param partitioner
   *        the <code>Partitioner</code> to use
   * @throws IllegalStateException
   *         if the job is submitted
   */
  @SuppressWarnings("unchecked")
  public void setPartitionerClass(Class<? extends Partitioner> partitioner)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS, partitioner,
        Partitioner.class);
  }

  /**
   * Set the {@link WritePartition} for the job.
   *
   * @param writePartition
   *        the <code>Partitioner</code> to use
   * @throws IllegalStateException
   *         if the job is submitted
   */
  public void setWritePartition(Class<? extends WritePartition> writePartition)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_WRITEPARTITION_CLASS,
        writePartition, WritePartition.class);
  }

  /**
   * This method is used to indicate whether the data is divided.IsDivide is
   * true not divided.
   *
   * @param isDivide Divided is true or false.
   */
  public void setIsDivide(boolean isDivide) {
    conf.setBoolean(Constants.USER_BC_BSP_JOB_ISDIVIDE, isDivide);
  }

  /**
   * This method sets the number of thread that those use to send graph data to
   * other worker.
   *
   * @param number The number of thread.
   */
  public void setSendThreadNumber(int number) {
    conf.setInt(Constants.USER_BC_BSP_JOB_SENDTHREADNUMBER, number);
  }

  /**
   * This method sets cache size. In MB. Sending data when catch is full.
   *
   * @param size
   *        cache size is number MB
   */
  public void setTotalCacheSize(int size) {
    conf.setInt(Constants.USER_BC_BSP_JOB_TOTALCACHE_SIZE, size);
  }

  /**
   * This method is used to set the balance factor.
   *
   * @param factor The balance factor is used to determine the
   * number of hash bucket‘s copy. If parameter "factor" is
   * 0.01 means that each hash bucket has 100 copies. That is, 1/0.01.
   */
  public void setBalanceFactor(float factor) {
    conf.setFloat(Constants.USER_BC_BSP_JOB_BALANCE_FACTOR, factor);
  }

  /**
   * Set the {@link RecordParse} for the job.
   *
   * @param recordParse
   *        the <code>RecordParse</code> to use
   * @throws IllegalStateException
   *         if the job is submitted
   */
  public void setRecordParse(Class<? extends RecordParse> recordParse)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS, recordParse,
        RecordParse.class);
  }

  /**
   * Set the {@link BC-BSP InputFormat} for the job.
   *
   * @param cls
   *        the <code>InputFormat</code> to use
   * @throws IllegalStateException
   */
  @SuppressWarnings("unchecked")
  public void setInputFormatClass(Class<? extends InputFormat> cls)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS, cls,
        InputFormat.class);
  }

  /**
   * Get the {@link BC-BSP InputFormat} class for the job.
   *
   * @return the {@link BC-BSP InputFormat} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends InputFormat<?, ?>> getInputFormatClass()
      throws ClassNotFoundException {
    return (Class<? extends InputFormat<?, ?>>) conf.getClass(
        Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS, InputFormat.class);
  }

  /**
   * Set the split size. The unit is MB;
   *
   * @param splitSize The size of each split.
   */
  public void setSplitSize(int splitSize) {
    conf.setLong(Constants.USER_BC_BSP_JOB_SPLIT_SIZE,
        splitSize * 1024 * 1024);
  }

  /**
   * Get the split size. The unit is byte. If user does not set it, return 0,
   * than means the system will generate split according to the actual block
   * size and the staff slots.
   *
   * @return The size of split.
   */
  public long getSplitSize() {
    return conf.getLong(Constants.USER_BC_BSP_JOB_SPLIT_SIZE, 0);
  }

  /**
   * Set the {@link BC-BSP OutputFormat} for the job.
   *
   * @param cls
   *        the <code>OutputFormat</code> to use
   * @throws IllegalStateException
   */
  @SuppressWarnings("unchecked")
  public void setOutputFormatClass(Class<? extends OutputFormat> cls)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS, cls,
        OutputFormat.class);
  }

  /**
   * Set the BC-BSP algorithm class of the job.
   *
   * @param cls The BC-BSP algorithm class.
   * @throws IllegalStateException
   */
  public void setBspClass(Class<? extends BSP> cls)
      throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(Constants.USER_BC_BSP_JOB_WORK_CLASS, cls, BSP.class);
  }

  /**
   * Get the BC-BSP algorithm class of the job.
   *
   * @return The BC-BSP algorithm class of the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends BSP> getBspClass() {
    return (Class<? extends BSP>) conf.getClass(
        Constants.USER_BC_BSP_JOB_WORK_CLASS, BSP.class);
  }

  /**
   * Set the BC-BSP application jar of the job.
   *
   * @param jar The BC-BSP application jarFile of the job.
   */
  public void setJar(String jar) {
    conf.set(Constants.USER_BC_BSP_JOB_JAR, jar);
  }

  /**
   * Set the BC-BSP application jar of the job.
   *
   * @param cls The BC-BSP application jarFileClass of the job.
   */
  public void setJarByClass(Class<?> cls) {
    String jar = findContainingJar(cls);
    if (jar != null) {
      conf.set(Constants.USER_BC_BSP_JOB_JAR, jar);
    }
  }

  public String getJar() {
    return conf.get(Constants.USER_BC_BSP_JOB_JAR);
  }

  /**
   * Set the number of staff.
   *
   * @param staffNum The number of staff.
   */
  public void setNumBspStaff(int staffNum) {
    conf.setInt(Constants.USER_BC_BSP_JOB_STAFF_NUM, staffNum);
  }

  public int getNumBspStaff() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_STAFF_NUM, 1);
  }

  /**
   * Set the priority of job.
   *
   * @param priority The PRIORITY of job.
   */
  public void setPriority(String priority) {
    conf.set(Constants.USER_BC_BSP_JOB_PRIORITY, priority);
  }

  public String getPriority() {
    return conf.get(Constants.USER_BC_BSP_JOB_PRIORITY,
        Constants.PRIORITY.NORMAL);
  }

  /**
   * Set the type of partition.
   *
   * @param partitionType The type of partition.
   */
  public void setPartitionType(String partitionType) {
    conf.set(Constants.USER_BC_BSP_JOB_PARTITION_TYPE, partitionType);
  }

  public String getPartitionType() {
    return conf.get(Constants.USER_BC_BSP_JOB_PARTITION_TYPE,
        Constants.PARTITION_TYPE.HASH);
  }

  /**
   * Set the number of partition.
   *
   * @param partitions The number of partition.
   */
  public void setNumPartition(int partitions) {
    conf.setInt(Constants.USER_BC_BSP_JOB_PARTITION_NUM, partitions);
  }

  public int getNumPartition() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_PARTITION_NUM, 0);
  }

  /**
   * Set the number of superStep.
   *
   * @param superStepNum The number of superStep.
   */
  public void setNumSuperStep(int superStepNum) {
    conf.setInt(Constants.USER_BC_BSP_JOB_SUPERSTEP_MAX, superStepNum);
  }

  public int getNumSuperStep() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_SUPERSTEP_MAX, 1);
  }

  /**
   * Find the BC-BSP application jar of the job.
   *
   * @param myClass The BC-BSP application jarFileClass of the job.
   *
   * @return The name of the jar.
   */
  private static String findContainingJar(Class<?> myClass) {
    ClassLoader loader = myClass.getClassLoader();
    String classFile = myClass.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration<URL> itr = loader.getResources(classFile); itr
          .hasMoreElements();) {
        URL url = itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      LOG.error("[findContainingJar]", e);
      throw new RuntimeException(e);
    }
    return null;
  }

  // /////////////////////////////////////
  // Methods for Job Control
  // /////////////////////////////////////
  /**
   * For the process is running job.
   *
   * @return The process is running job.
   */
  public long progress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.progress();
  }

  /** @return Whether the job status is SUCCEEDED or FAILED or KILLED. */
  public boolean isComplete() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isComplete();
  }

  /** @return Whether the job status is SUCCEEDED. */
  public boolean isSuccessful() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isSuccessful();
  }

  /** Kill the running job */
  public void killJob() throws IOException {
    ensureState(JobState.RUNNING);
    info.killJob();
  }

  /** Kill the running staff.
   * @param taskId The staff id.
   * */
  public void killStaff(StaffAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killStaff(taskId, false);
  }

  /** Kill the running staff.
   * @param taskId The staff id.
   * */
  public void failStaff(StaffAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killStaff(taskId, true);
  }

  /** Submit the job, if the job status DEFINE,
   * and sets the job status to RUNNING.
   * */
  public void submit() throws IOException, ClassNotFoundException,
      InterruptedException {
    ensureState(JobState.DEFINE);
    info = jobClient.submitJobInternal(this);
    state = JobState.RUNNING;
  }

  public JobState getState() {
    return state;
  }

  public void setState(JobState state) {
    this.state = state;
  }

  public RunningJob getInfo() {
    return info;
  }

  public void setInfo(RunningJob info) {
    this.info = info;
  }

  /** If the job ready, submit the job, and start monitor.
   *
   * @param verbose User has set true if the job ready.
   *
   * @return job state.
   * */
  public boolean waitForCompletion(boolean verbose) {
    try {
      /** To set the params for aggregators. */
      completeAggregatorRegister();
      if (state == JobState.DEFINE) {
        submit();
      }
      if (verbose) {
        jobClient.monitorAndPrintJob(this, info);
      } else {
        info.waitForCompletion();
      }
      return isSuccessful();
    } catch (ClassNotFoundException lnfE) {
      LOG.error("Exception has been catched in BSPJob--waitForCompletion !",
          lnfE);
      Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, "null",
          lnfE.toString());
      jobClient.getJobSubmitClient().recordFault(f);
      jobClient.getJobSubmitClient().recovery(
          new BSPJobID("before have an BSPJobId !", -1));
      return false;
    } catch (InterruptedException iE) {
      LOG.error("Exception has been catched in BSPJob--waitForCompletion !",
          iE);
      Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.INDETERMINATE,
          "null", iE.toString());
      jobClient.getJobSubmitClient().recordFault(f);
      jobClient.getJobSubmitClient().recovery(
          new BSPJobID("before have an BSPJobId !", -1));
      return false;
    } catch (IOException ioE) {
      LOG.error("Exception has been catched in BSPJob--waitForCompletion !",
          ioE);
      Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.INDETERMINATE,
          "null", ioE.toString());
      jobClient.getJobSubmitClient().recordFault(f);
      jobClient.getJobSubmitClient().recovery(
          new BSPJobID("before have an BSPJobId !", -1));
      return false;
    }
  }
  // /////////////////////////////////////
  // Combiner and Communication
  // /////////////////////////////////////
  /**
   * Get the user defined Combiner class.
   *
   * @return The user defined Combiner class.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Combiner> getCombiner() {
    return (Class<? extends Combiner>) conf.getClass(
        Constants.USER_BC_BSP_JOB_COMBINER_CLASS, Combiner.class);
  }

  /**
   * Check the combiner set flag.
   *
   * @return true if user has set combiner implementation, or flase otherwise.
   */
  public boolean isCombinerSetFlag() {
    return conf.getBoolean(Constants.USER_BC_BSP_JOB_COMBINER_DEFINE_FLAG,
        false);
  }

  /**
   * Check the combiner set flag specially for receiving end.
   *
   * @return true if user not set the receive combiner set flag to be false, or
   *         false otherwise.
   */
  public boolean isReceiveCombinerSetFlag() {
    boolean flag1 = conf.getBoolean(
        Constants.USER_BC_BSP_JOB_COMBINER_RECEIVE_FLAG, false);
    boolean flag2 = conf.getBoolean(
        Constants.USER_BC_BSP_JOB_COMBINER_DEFINE_FLAG, false);
    boolean flag3 = flag1 && flag2;
    return flag3;
  }

  /**
   * Set the user defined Combiner class.
   *
   * @param combinerClass
   */
  public void setCombiner(Class<? extends Combiner> combinerClass) {
    conf.setClass(Constants.USER_BC_BSP_JOB_COMBINER_CLASS, combinerClass,
        Combiner.class);
    conf.setBoolean(Constants.USER_BC_BSP_JOB_COMBINER_DEFINE_FLAG, true);
    conf.setBoolean(Constants.USER_BC_BSP_JOB_COMBINER_RECEIVE_FLAG, true);
  }

  /**
   * Set the receive combiner set flag for the receiving end.
   *
   * @param flag User set flag to be false.
   */
  public void setReceiveCombinerSetFlag(boolean flag) {
    conf.setBoolean(Constants.USER_BC_BSP_JOB_COMBINER_RECEIVE_FLAG, flag);
  }

  /**
   * Set the threshold for sending messages. A queue for one WorkerManager will
   * not be sent until its size overs the threshold. This threshold still works
   * without combiner set.
   *
   * @param threshold Send the message thresholds.
   */
  public void setSendThreshold(int threshold) {
    conf.setInt(Constants.USER_BC_BSP_JOB_SEND_THRESHOLD, threshold);
  }

  /**
   * Get the threshold for sending messages. A queue for one WorkerManager will
   * not be sent until its size overs the threshold. This threshold still works
   * without combiner set.
   *
   * @return send threshold
   */
  public int getSendThreshold() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_SEND_THRESHOLD, 0);
  }

  /**
   * Set the threshold for combining messages on sending side. A queue for one
   * WorkerManager will not be combined until its size overs the threshold. This
   * threshold only works with combiner set.
   *
   * @param threshold Send the combine threshold.
   */
  public void setSendCombineThreshold(int threshold) {
    conf.setInt(Constants.USER_BC_BSP_JOB_SEND_COMBINE_THRESHOLD, threshold);
  }

  /**
   * Get the threshold for combining messages on sending side. A queue for one
   * WorkerManager will not be combined until its size overs the threshold. This
   * threshold only works with combiner set.
   *
   * @return send combine threshold
   */
  public int getSendCombineThreshold() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_SEND_COMBINE_THRESHOLD, 0);
  }

  /**
   * Set the threshold for combining messages on receiving side. A queue for one
   * HeadNode will not be combined until its size overs the threshold. This
   * threshold only works with combiner set.
   *
   * @param threshold Receive the combine threshold.
   */
  public void setReceiveCombineThreshold(int threshold) {
    conf.setInt(Constants.USER_BC_BSP_JOB_RECEIVE_COMBINE_THRESHOLD, threshold);
  }

  /**
   * Get the threshold for combining messages on receiving side. A queue for one
   * HeadNode will not be combined until its size overs the threshold. This
   * threshold only works with combiner set.
   *
   * @return receive combine threshold
   */
  public int getReceiveCombineThreshold() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_RECEIVE_COMBINE_THRESHOLD, 0);
  }

  /**
   * Set the size for messages pack.
   *
   * @param size The size of messages pack.
   */
  public void setMessagePackSize(int size) {
    conf.setInt(Constants.USER_BC_BSP_JOB_MESSAGE_PACK_SIZE, size);
  }

  public int getMessagePackSize() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_MESSAGE_PACK_SIZE, 0);
  }

  /**
   * Set the max number of producer.
   *
   * @param num The max number of producer.
   */
  public void setMaxProducerNum(int num) {
    conf.setInt(Constants.USER_BC_BSP_JOB_MAX_PRODUCER_NUM, num);
  }

  public int getMaxProducerNum() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_MAX_PRODUCER_NUM, 0);
  }

  /**
   * Set the max number of consumer.
   *
   * @param num The max number of consumer.
   */
  public void setMaxConsumerNum(int num) {
    conf.setInt(Constants.USER_BC_BSP_JOB_MAX_CONSUMER_NUM, num);
  }

  public int getMaxConsumerNum() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_MAX_CONSUMER_NUM, 1);
  }

  // /////////////////////////////////////
  // Memory and disk administration
  // /////////////////////////////////////
  /**
   * Set the data percent in memory of the graph and messages data.
   *
   * @param dataPercent The data percent in memory of the graph and
   * messages data.
   */
  public void setMemoryDataPercent(float dataPercent) {
    conf.setFloat(Constants.USER_BC_BSP_JOB_MEMORY_DATA_PERCENT, dataPercent);
  }

  /**
   * Get the data percent in memory of the graph and messages data.
   *
   * @return The data percent in memory of the graph and messages data.
   */
  public float getMemoryDataPercent() {
    return conf.getFloat(Constants.USER_BC_BSP_JOB_MEMORY_DATA_PERCENT, 0.8f);
  }

  /**
   * Set the beta parameter for the proportion of the data memory for the graph
   * data.
   *
   * @param beta The beta parameter for the proportion of the data memory for
   * the graph data.
   */
  public void setBeta(float beta) {
    conf.setFloat(Constants.USER_BC_BSP_JOB_MEMORY_BETA, beta);
  }

  /**
   * Get the beta parameter for the proportion of the data memory for the graph
   * data. Default is 0.5 for user undefined.
   *
   * @return beta
   */
  public float getBeta() {
    return conf.getFloat(Constants.USER_BC_BSP_JOB_MEMORY_BETA, 0.5f);
  }

  /**
   * Set the hash bucket number for the memory administration of both the graph
   * data and the messages.
   *
   * @param number Hash bucket number
   */
  public void setHashBucketNumber(int number) {
    conf.setInt(Constants.USER_BC_BSP_JOB_MEMORY_HASHBUCKET_NO, number);
  }

  /**
   * Get the hash bucket number for the memory administration of both the graph
   * data and the messages. Default is 32 for user undefined.
   *
   * @return Hash bucket number
   */
  public int getHashBucketNumber() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_MEMORY_HASHBUCKET_NO, 32);
  }

  /**
   * Set the implementation version for the graph data management. There are 4
   * choices: MEMORY_VERSION, DISK_VERSION, BDB_VERSION, BYTEARRAY_VERSION.
   *
   * @param version The implementation version for the graph data management.
   */
  public void setGraphDataVersion(int version) {
    conf.setInt(Constants.USER_BC_BSP_JOB_GRAPH_DATA_VERSION, version);
  }

  /**
   * Get the implementation version for the graph data management. Default is
   * MEMORY_VERSION for user undefined.
   *
   * @return the graph data version
   */
  public int getGraphDataVersion() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_GRAPH_DATA_VERSION,
        this.BYTEARRAY_VERSION);
  }

  /**
   * Set the implementation version for the message queues management. There are
   * 4 choices: MEMEORY_VERSION, DISK_VERSION, BDB_VERSION, BYTEARRAY_VERSION.
   *
   * @param version The implementation version for the graph data management.
   */
  public void setMessageQueuesVersion(int version) {
    conf.setInt(Constants.USER_BC_BSP_JOB_MESSAGE_QUEUES_VERSION, version);
  }

  /**
   * Get the implementation version for the message queues management. Default
   * is MEMORY_VERSION for user undefined.
   *
   * @return the message queues version
   */
  public int getMessageQueuesVersion() {
    return conf.getInt(Constants.USER_BC_BSP_JOB_MESSAGE_QUEUES_VERSION,
        this.MEMORY_VERSION);
  }

  /**
   * Set the implementation version for the message comunication management.
   * There are 4 now choices: RPC_VERSION, ACTIVEMQ_VERSION,
   * RPC_BYTEARRAY_VERSION, COMMUNICATION_OPTION.
   *
   * @param commVersion The implementation version for the message
   * comunication management.
   */
  public void setCommunicationOption(String commVersion) {
    conf.set(Constants.COMMUNICATION_OPTION, commVersion);
  }

  /**
   * Get the implementation version for the message communication
   * management.Default is RPC_VERSIOM for user defined
   *
   * @return The implementation version for the message
   * communication management.
   */
  public String getCommucationOption() {
    return conf.get(Constants.COMMUNICATION_OPTION,
        Constants.RPC_BYTEARRAY_VERSION);
  }

  // /////////////////////////////////////
  // Graph Data Structure
  // ////////////////////////////////////
  /**
   * Set the Vertex class type for the job.
   *
   * @param cls The Vertex class type for the job.
   * @throws IllegalStateException
   */
  public void setVertexClass(Class<? extends Vertex<?, ?, ?>> cls)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_VERTEX_CLASS, cls, Vertex.class);
  }

  /**
   * Get the Vertex class type for the job.
   *
   * @return The Vertex class type for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Vertex<?, ?, ?>> getVertexClass() {
    return (Class<? extends Vertex<?, ?, ?>>) conf.getClass(
        Constants.USER_BC_BSP_JOB_VERTEX_CLASS, Vertex.class);
  }

  /**
   * Set the Edge class type for the job.
   *
   * @param cls The Edge class type for the job.
   * @throws IllegalStateException
   */
  public void setEdgeClass(Class<? extends Edge<?, ?>> cls)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_EDGE_CLASS, cls, Edge.class);
  }

  /**
   * Get the Edge class type for the job.
   *
   * @return The Edge class type for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Edge<?, ?>> getEdgeClass() {
    return (Class<? extends Edge<?, ?>>) conf.getClass(
        Constants.USER_BC_BSP_JOB_EDGE_CLASS, Edge.class);
  }

  /**
   * Add by chen Get the job's type (c++ or java).
   *
   * @return The job's type (c++ or java)
   */
  public String getJobType() {
    return conf.get(Constants.USER_BC_BSP_JOB_TYPE, "");
  }

  /**
   * add by chen Get the job's path.
   *
   * @return The job's path.
   */
  public String getJobExe() {
    return conf.get(Constants.USER_BC_BSP_JOB_EXE);
  }

  /**
   * add by chen Set the job's path.
   *
   * @param path The job's path.
   */
  public void setJobExe(String path) {
    conf.set(Constants.USER_BC_BSP_JOB_EXE, path);
  }

  /**
   * add by Zhicheng Liu Set HBash input the name of the table.
   *
   * @param name The HBash input the name of the table.
   */
  public void setInputTableNameForHBase(String name) {
    conf.set(Constants.USER_BC_BSP_JOB_HBASE_INPUT_TABLE_NAME, name);
  }

  /**
   * Set HBash output the name of the table.
   *
   * @param name The HBash output the name of the table.
   */
  public void setOutputTableNameForHBase(String name) {
    conf.set(Constants.USER_BC_BSP_JOB_HBASE_OUTPUT_TABLE_NAME, name);
  }

  /**
   * Set Titan input the name of the table.
   *
   * @param name The Titan input the name of the table.
   */
  public void setInputTableNameForTitan(String name) {
    conf.set(Constants.USER_BC_BSP_JOB_TITAN_INPUT_TABLE_NAME, name);
    conf.set(Constants.USER_BC_BSP_JOB_TITAN_HBASE_INPUT_TABLE_NAME, name);
  }

  /**
   * Set Titan output the name of the table.
   *
   * @param name The Titan output the name of the table.
   */
  public void setOutputTableNameForTitan(String name) {
    conf.set(Constants.USER_BC_BSP_JOB_TITAN_OUTPUT_TABLE_NAME, name);
    conf.set(Constants.USER_BC_BSP_JOB_TITAN_HBASE_OUTPUT_TABLE_NAME, name);
  }

  /**
   * Add 20140329 Liu Jinpeng Set the message class type for the job.
   *
   * @param cls The message class type for the job.
   * @throws IllegalStateException
   */
  public void setMessageClass(Class cls) throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_MESSAGE_CLASS, cls, IMessage.class);
  }

  /**
   * Get the message class type for the job.
   *
   * @return The message class type for the job.
   */
  @SuppressWarnings("unchecked")
  public Class getMessageClass() {
    return conf.getClass(Constants.USER_BC_BSP_JOB_MESSAGE_CLASS,
        BSPMessage.class);
  }

  /**
   * add by Dang Yingguang Set Mysql input the name of the table.
   *
   * @param name The Mysql input the name of the table.
   */
  public void setInputTableNameForMysql(String name) {
    conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, name);
  }

  /**
   * Set Mysql output the name of the table.
   *
   * @param name The Mysql output the name of the table.
   */
  public void setOutputTableNameForMysql(String name) {
    conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, name);
  }

  /**
   * Get the input or output class type for the job.
   *
   * @param name The input or output class key for the job.
   * @param cls The input or output class type for the job.
   *
   * @return The input or output class type for the job.
   */
  public Class<?> getClass(String name, Class<?> cls) {
    return conf.getClass(name, cls);
  }

  /**
   * Set the input or output class type for the job.
   *
   * @param name The input or output class key for the job.
   * @param theClass The input or output class for the job.
   * @param cls The input or output class type for the job.
   */
  public void setClass(String name, Class<?> theClass, Class<?> cls) {
    conf.setClass(name, theClass, cls);
  }

//  public void setOutputFormat(Class<? extends OutputFormat> theClass) {
//    conf.setClass("mapred.output.format.class", theClass, OutputFormat.class);
//  }

  /**
   * Set the outgongingEdgenum for hrn compute.
   *
   * @param outgoingEdgenum The outgongingEdgenum for hrn compute.
   */
  public void setOutgoingEdgeNum(long outgoingEdgenum) {
    conf.setLong(Constants.USER_BC_BSP_JOB_OUTGOINGEDGE_NUM, outgoingEdgenum);
  }

  /**
   * Get the outgoingEdgenum the user set.
   *
   * @return The outgoingEdgenum the user set.
   */
  public long getOutgoingEdgeNum() {
    return conf.getLong(Constants.USER_BC_BSP_JOB_OUTGOINGEDGE_NUM,
        Constants.USER_BC_BSP_JOB_OUTGOINGEDGE_NUM_DEFAULT);
  }
  
  public void setCheckpointType(String name) {
    conf.set(Constants.BC_BSP_CHECKPOINT_TYPE, name);
  }
  
  public String getCheckpointType() {
    return conf.get(Constants.BC_BSP_CHECKPOINT_TYPE, "HDFS");
  }
  /**
   * added by feng for value checkpoint
   * @param checkpoint
   * @throws IllegalStateException
   */
  public void setCheckPoint(Class<? extends assistCheckpoint> checkpoint)
  throws IllegalStateException {
	  LOG.info("feng test: checkpoint class "+checkpoint.toString());
//	  System.out.print("feng test: checkpoint class "+checkpoint.toString());
	  conf.setClass(Constants.DEFAULT_BC_BSP_JOB_CHECKPOINT_USER_DEFINE, checkpoint,
    assistCheckpoint.class);
}
  public Class getCheckpointClass(){
	  return conf.getClass(Constants.DEFAULT_BC_BSP_JOB_CHECKPOINT_USER_DEFINE, assistCheckpoint.class);
  }
  
  public int getComputeState() {
    return conf.getInt(Constants.USER_BC_BSP_COMPUTE_TYPE, 0);
  }
  
  public void setComputeState(int computestate) {
    conf.setInt(Constants.USER_BC_BSP_COMPUTE_TYPE, computestate);
    if(computestate == Constants.PEER_COMPUTE){
      this.setGraphDataVersion(PEER_VERSION);
    }
  }
  
  public void setKeyValuePairClass(Class<? extends Vertex<?, ?, ?>> cls)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_KVPAIR_CLASS, cls, KeyValuePair.class);
    setVertexClass(cls);
  }
  public void setKeyClass(Class<? extends Key> cls)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_KEY_CLASS, cls, Key.class);
  }
  public void setValueClass(Class<? extends Value> cls)
      throws IllegalStateException {
    conf.setClass(Constants.USER_BC_BSP_JOB_VALUE_CLASS, cls, Value.class);
  }
  
  public Class<?> getKeyValuePairClass() {
    return (Class<? extends Vertex<?, ?, ?>>) conf.getClass(
        Constants.USER_BC_BSP_JOB_KVPAIR_CLASS, KeyValuePair.class);  }
  
  
}
