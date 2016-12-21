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

package com.chinamobile.bcbsp.pipes;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.Communicator;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;
/**The class TaskHandler implenments UpwardProtocol.
 * Send data to c++ process.
 * */
public class TaskHandler implements UpwardProtocol {
  /**A log variable for user to write log.*/
  private static final Log LOG = LogFactory.getLog(TaskHandler.class.getName());
  /**The communicator in java process.*/
  private CommunicatorInterface communicator;
  /**To save the aggregateValue.*/
  private ArrayList<String> aggregateValue;
  /**A flag to notice whether the job done.*/
  private boolean done = false;
  /**The Throwable class is the superclass of all errors and exceptions in
   * the Java language.*/
  private Throwable exception = null;
  /**A BSP staff.*/
  private Staff staff;
  /**Protocol that staff child process uses to contact its parent process.*/
  private WorkerAgentProtocol workerAgent;
  /**BSPJob A BSP job configuration.*/
  private BSPJob job;
  /**Staff attempt id.*/
  private StaffAttemptID sid;
  /**RecordWriter This class can write a record in the format of Key-Value.*/
  private RecordWriter output;
  /**The constructor.
   * @param communicator The communicator in java process.*/
  public TaskHandler(Communicator communicator) {
    super();
    aggregateValue = new ArrayList<String>();
    this.communicator = communicator;
  }
  /**The constructor.*/
  public TaskHandler() {
    super();
    this.aggregateValue = new ArrayList<String>();
  }
  /**The constructor.
   * @param job BSP job configuration
   * @param staff a BSP staff
   * @param workerAgent Protocol that staff child process
   *           uses to contact its parent process*/
  public TaskHandler(BSPJob job, Staff staff, WorkerAgentProtocol workerAgent) {
    this();
    this.job = job;
    this.staff = staff;
    this.sid = staff.getStaffAttemptId();
    this.workerAgent = workerAgent;
  }

  @Override
  public void sendNewMessage(String msg) throws IOException {
    BSPMessage message = new BSPMessage();
    message.fromString(msg);

    //LOG.info("message parse over" + msg);

    // LOG.info("message parse over");

    this.communicator.send(message);
  }

  @Override
  public void setAgregateValue(String aggregateValue) throws IOException {
    this.aggregateValue.add(aggregateValue);
    LOG.info("add aggregateValue");
  }

  @Override
  public void currentSuperStepDone() throws IOException {
    synchronized (this) {
      this.done = true;
      notify();
      LOG.info("have notify");
    }
  }

  @Override
  public void failed(Throwable e) {
    synchronized (this) {
      this.exception = e;
      notify();
    }
  }

  /**
   * Wait for the superStep to finish or abort.
   * @return did the superStep finish correctly?
   * @throws Throwable
   */
  public synchronized boolean waitForFinish() throws Throwable {
    if (!done && exception == null) {
      LOG.info("in wait");
      wait();
    }
    LOG.info("out of wait");
    if (exception != null) {
      throw exception;
    }
    this.done = false;
    return true;
  }

  @Override
  public ConcurrentLinkedQueue<IMessage> getMessage(String vertexId)
      throws IOException {
    return this.communicator.getMessageQueue(vertexId);
  }
  /**Get the aggregate value.
   * @return aggregateValue*/
  public ArrayList<String> getAggregateValue() {
    return aggregateValue;
  }
  
  /**Add aggregate.*/
  public void addAggregateValue(String aggValue) {
    this.aggregateValue.add(aggValue);
  }
  
  /**Set the aggregateValue.*/
  public void setAggregateValue(ArrayList<String> aggregateValue) {
    this.aggregateValue = aggregateValue;
  }
  
  // @Override
  // public int getPartitionId(int id) throws IOException {
  // // TODO Auto-generated method stub
  // return id;
  // }

  /**Get the communicator.
   * @return the communicator*/
  public CommunicatorInterface getCommunicator() {
    return communicator;
  }

  @Override
  public void setCommunicator(CommunicatorInterface communicator) {
    this.communicator = communicator;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean saveResult(String vertexId, String vertexValue
      , String[] edges) {
    try {
      OutputFormat outputformat = (OutputFormat) ReflectionUtils.newInstance(
          job.getConf().getClass(Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS,
              OutputFormat.class), job.getConf());
      outputformat.initialize(job.getConf());
      RecordWriter newoutput = outputformat.getRecordWriter(job, sid);
      // for (int i = 0; i < graphData.sizeForAll(); i++) {
      // Vertex<?, ?, Edge> vertex = graphData.getForAll(i);
      StringBuffer outEdges = new StringBuffer();
      for (String edge : edges) {
        outEdges.append(vertexId + Constants.SPLIT_FLAG + vertexValue
            + Constants.SPACE_SPLIT_FLAG);
      }
      if (outEdges.length() > 0) {
        int j = outEdges.length();
        outEdges.delete(j - 1, j - 1);
      }
      newoutput.write(new Text(vertexId + Constants.SPLIT_FLAG + vertexValue),
          new Text(outEdges.toString()));
      // }
      newoutput.close(job);
      // graphData.clean();
    } catch (Exception e) {
      LOG.error("Exception has been catched in BSPStaff--saveResult !", e);
      BSPConfiguration conf = new BSPConfiguration();
      if (staff.getRecoveryTimes() < conf.getInt(
          Constants.BC_BSP_JOB_RECOVERY_ATTEMPT_MAX, 0)) {
        staff.recovery(job, staff, workerAgent);
      } else {
        workerAgent.setStaffStatus(
            sid,
            Constants.SATAFF_STATUS.FAULT,
            new Fault(Fault.Type.DISK, Fault.Level.INDETERMINATE, workerAgent
                .getWorkerManagerName(job.getJobID(), sid), e.toString(), job
                .toString(), sid.toString()), 2);
        LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*="
            + "*=*=*=*");
        LOG.error("Other Exception has happened and been catched, "
            + "the exception will be reported to WorkerManager", e);
      }
    }
    return true;
  }

  @Override
  public void openDFS() {
    OutputFormat outputformat = (OutputFormat) ReflectionUtils.newInstance(
        job.getConf().getClass(Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS,
            OutputFormat.class), job.getConf());
    outputformat.initialize(job.getConf());
    try {
      output = outputformat.getRecordWriter(job, sid);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean saveResult(ArrayList<String> vertexEdge) {
    try {
      OutputFormat outputformat = (OutputFormat) ReflectionUtils.newInstance(
          job.getConf().getClass(Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS,
              OutputFormat.class), job.getConf());
      outputformat.initialize(job.getConf());
      RecordWriter output1 = outputformat.getRecordWriter(job, sid);
      // for (int i = 0; i < graphData.sizeForAll(); i++) {
      // Vertex<?, ?, Edge> vertex = graphData.getForAll(i);
      for (int i = 0; i < vertexEdge.size(); i++) {
        LOG.info("+++++++++++vertexEdge++++++++" + vertexEdge.get(i));
        output1.write(new Text(vertexEdge.get(i)));
      }
      output1.close(job);
      vertexEdge.clear();
      // graphData.clean();
    } catch (Exception e) {
      LOG.error("Exception has been catched in BSPStaff--saveResult !", e);
      BSPConfiguration conf = new BSPConfiguration();
      if (staff.getRecoveryTimes() < conf.getInt(
          Constants.BC_BSP_JOB_RECOVERY_ATTEMPT_MAX, 0)) {
        staff.recovery(job, staff, workerAgent);
      } else {
        workerAgent.setStaffStatus(
            sid,
            Constants.SATAFF_STATUS.FAULT,
            new Fault(Fault.Type.DISK, Fault.Level.INDETERMINATE, workerAgent
                .getWorkerManagerName(job.getJobID(), sid), e.toString(), job
                .toString(), sid.toString()), 2);
        LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*"
            + "=*=*=*=*=*");
        LOG.error("Other Exception has happened and been catched, "
            + "the exception will be reported to WorkerManager", e);
      }
    }
    return true;
  }

  @Override
  public boolean saveResult(String vertex) {
    try {
      try {
        String vertexEdge;

       // String path = "/home/bcbsp/vertex.txt";
        //File filename = new File(path);
       // filename.createNewFile();
       // RandomAccessFile mm = null;

       // mm = new RandomAccessFile(filename, "rw");
      //  mm.writeBytes(output.toString());
       // mm.writeBytes("vertexEdge = " + vertex);
        vertexEdge = vertex;
        output.write(new Text(vertex));
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (IOException e) {
      LOG.error("Exception has been catched in BSPStaff--saveResult !", e);
      BSPConfiguration conf = new BSPConfiguration();
      if (staff.getRecoveryTimes() < conf.getInt(
          Constants.BC_BSP_JOB_RECOVERY_ATTEMPT_MAX, 0)) {
        staff.recovery(job, staff, workerAgent);
      } else {
        workerAgent.setStaffStatus(
            sid,
            Constants.SATAFF_STATUS.FAULT,
            new Fault(Fault.Type.DISK, Fault.Level.INDETERMINATE, workerAgent
                .getWorkerManagerName(job.getJobID(), sid), e.toString(), job
                .toString(), sid.toString()), 2);
        LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*="
            + "*=*=*=*=*=*");
        LOG.error("Other Exception has happened and been catched, "
            + "the exception will be reported to WorkerManager", e);
      }
    }
    return true;
  }

  @Override
  public void closeDFS() throws IOException, InterruptedException {
    output.close(job);
  }
  // public RecordWriter getOutput() {
  // return output;
  // }
  // public void setOutput(RecordWriter output) {
  // this.output = output;
  // }
}
