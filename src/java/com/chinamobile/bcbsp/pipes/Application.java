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

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.comm.Communicator;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.StringUtils;



/**
 * this class is used to create a c++ process.
 * @author Chen Changning
 */
public class Application {
  /** get a Log class from LogFactory. */
  private static final Log LOG = LogFactory.getLog(Application.class.getName());
  /**
   * This class implements server sockets. A server socket waits for requests to
   * come in over the network. It performs some operation based on that request,
   * and then possibly returns a result to the requester. .
   */
  private ServerSocket serverSocket;
  /**
   * This class implements client sockets (also called just "sockets"). A socket
   * is an endpoint for communication between two machines.
   */
  private Socket clientSocket;
  /**
   * This class implements UpwardProtocol,contains some variable for the
   * application class.
   */
  private TaskHandler handler;
  /**
   * This variable is used to save the class BinaryProtocol that used to
   * communicate with c++ process.
   */
  private DownwardProtocol downlink;
  /**
   * The ProcessBuilder.start() and Runtime.exec methods create a native process
   * and return an instance of a subclass of Process that can be used to control
   * the process and obtain information about it. The class Process provides
   * methods for performing input from the process, performing output to the
   * process, waiting for the process to complete, checking the exit status of
   * the process, and destroying (killing) the process.
   */
  private Process process;
  
  /**
   * This method is the constructor.
   * @param job
   *        contains BSPJob configuration
   * @param communicator
   *        the communicator between different work node
   */
  public Application(BSPJob job, Communicator communicator) throws IOException,
      InterruptedException {
    serverSocket = new ServerSocket(0);
    Map<String, String> env = new HashMap<String, String>();
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
    env.put("bcbsp.pipes.command.port",
        Integer.toString(serverSocket.getLocalPort()));
    List<String> cmd = new ArrayList<String>();
    String executable = job.getJobExe();
    FileUtil.chmod(executable, "a+x");
    cmd.add(executable);
    process = runClient(cmd, env);
    clientSocket = serverSocket.accept();
    this.handler = new TaskHandler(communicator);
    this.downlink = new BinaryProtocol(clientSocket, handler);
    this.downlink.start();
  }
  
  /**
   * This method is the constructor.
   * @param job
   *        contains BSPJob configuration
   */
  public Application(BSPJob job) throws IOException, InterruptedException {
    serverSocket = new ServerSocket(0);
    Map<String, String> env = new HashMap<String, String>();
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
    env.put("bcbsp.pipes.command.port",
        Integer.toString(serverSocket.getLocalPort()));
    List<String> cmd = new ArrayList<String>();
    String executable = job.getJobExe();
    FileUtil.chmod(executable, "a+x");
    cmd.add(executable);
    
    process = runClient(cmd, env);
    clientSocket = serverSocket.accept();
    this.handler = new TaskHandler();
    this.downlink = new BinaryProtocol(clientSocket, handler);
    this.downlink.start();
  }
  
  /**
   * This method is the constructor.
   * @param job
   *        contains BSPJob configuration
   * @param processType
   *        the type of c++ process(for staff or workmanager)
   */
  public Application(BSPJob job, String processType) throws IOException,
      InterruptedException {
    serverSocket = new ServerSocket(0);
    Map<String, String> env = new HashMap<String, String>();
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
    env.put("bcbsp.pipes.command.port",
        Integer.toString(serverSocket.getLocalPort()));
    env.put("processType", processType);
    String bcbspdir = job.getConf().get("bcbsp.log.dir");
    LOG.info("bcbsp log dir : " + bcbspdir);
    env.put("bcbsp.log.dir", bcbspdir);
    List<String> cmd = new ArrayList<String>();
    String executable = job.getJobExe();
    FileUtil.chmod(executable, "a+x");
    cmd.add(executable);
    
    process = runClient(cmd, env);
    clientSocket = serverSocket.accept();
    this.handler = new TaskHandler();
    this.downlink = new BinaryProtocol(clientSocket, handler);
    this.downlink.start();
  }
  
  /**
   * This method is the constructor.
   * @param job
   *        contains BSPJob configuration
   * @param processType
   *        the type of c++ process(for staff or workmanager)
   * @param jobCpath
   *        c++ executable file path
   */
  public Application(BSPJob job, String processType, String jobCpath)
      throws IOException, InterruptedException {
    serverSocket = new ServerSocket(0);
    Map<String, String> env = new HashMap<String, String>();
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
    env.put("bcbsp.pipes.command.port",
        Integer.toString(serverSocket.getLocalPort()));
    env.put("processType", processType);
    String bcbspdir = job.getConf().get("bcbsp.log.dir");
    LOG.info("bcbsp log dir : " + bcbspdir);
    env.put("bcbsp.log.dir", bcbspdir);
    List<String> cmd = new ArrayList<String>();
    String executable = jobCpath;
    LOG.info("processType is :"+processType);
    LOG.info("executable is :"+executable);
    // String executable = job.getJobExe();
    FileUtil.chmod(executable, "a+x");
    cmd.add(executable);
    process = runClient(cmd, env);
    clientSocket = serverSocket.accept();
    this.handler = new TaskHandler();
    this.downlink = new BinaryProtocol(clientSocket, handler);
    this.downlink.start();
  }
  
  /**
   * This method is the constructor.
   * @param conf
   *        contains Job configuration
   * @param processType
   *        the type of c++ process(for staff or workmanager)
   */
  public Application(Configuration conf, String processType)
      throws IOException, InterruptedException {
    serverSocket = new ServerSocket(0);
    Map<String, String> env = new HashMap<String, String>();
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
    env.put("bcbsp.pipes.command.port",
        Integer.toString(serverSocket.getLocalPort()));
    env.put("processType", processType);
    List<String> cmd = new ArrayList<String>();
    String executable = conf.get(Constants.USER_BC_BSP_JOB_EXE);
    FileUtil.chmod(executable, "a+x");
    cmd.add(executable);
    process = runClient(cmd, env);
    clientSocket = serverSocket.accept();
    this.handler = new TaskHandler();
    this.downlink = new BinaryProtocol(clientSocket, handler);
    this.downlink.start();
  }
  
  /**
   * This method is the constructor.
   * @param job
   *        contains BSPJob configuration
   * @param staff
   *        the java compute process
   * @param workerAgent
   *        Protocol that staff child process uses to contact its parent process
   */
  public Application(BSPJob job, Staff staff, WorkerAgentProtocol workerAgent)
      throws IOException, InterruptedException {
    serverSocket = new ServerSocket(0);
    Map<String, String> env = new HashMap<String, String>();
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
    LOG.info("Application: System.getProterty: "
        + System.getProperty("java.io.tmpdir"));
    env.put("bcbsp.pipes.command.port",
        Integer.toString(serverSocket.getLocalPort()));
    env.put("staffID", staff.getStaffID().toString());
    LOG.info("staffID is :" + staff.getStaffID().toString());
    List<String> cmd = new ArrayList<String>();
    String executable = staff.getJobExeLocalPath();
    FileUtil.chmod(executable, "a+x");
    cmd.add(executable);
    process = runClient(cmd, env);
    LOG.info("waiting for connect cpp process ");
    clientSocket = serverSocket.accept();
    LOG.info("=========run C++ ======");
    this.handler = new TaskHandler(job, staff, workerAgent);
    this.downlink = new BinaryProtocol(clientSocket, handler);
    this.downlink.start();
  }
  
  /**
   * This method is the constructor.
   * @param job
   *        contains BSPJob configuration
   * @param staff
   *        the java compute process
   * @param workerAgent
   *        Protocol that staff child process uses to contact its parent process
   * @param processType
   *        the type of c++ process(for staff or workmanager)
   */
  public Application(BSPJob job, Staff staff, WorkerAgentProtocol workerAgent,
      String processType) throws IOException, InterruptedException {
    serverSocket = new ServerSocket(0);
    Map<String, String> env = new HashMap<String, String>();
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
    LOG.info("Application: System.getProterty: "
        + System.getProperty("java.io.tmpdir"));
    env.put("processType", processType);
    env.put("bcbsp.pipes.command.port",
        Integer.toString(serverSocket.getLocalPort()));
    String bcbspdir = job.getConf().get("bcbsp.log.dir");
    String userdefine = job.getConf().get("userDefine");
    env.put("userDefine", userdefine);
    LOG.info("bcbsp log dir : " + bcbspdir);
    env.put("bcbsp.log.dir", bcbspdir);
    if (processType.equalsIgnoreCase("staff")) {
      env.put("staffID", staff.getStaffID().toString());
      LOG.info("staffID is :" + staff.getStaffID().toString());
    } else {
    }
    List<String> cmd = new ArrayList<String>();
    String executable = staff.getJobExeLocalPath();
    File file=new File(executable);
    if(file.exists()){
    	LOG.info("the jobC is exist");
    }else{
    	LOG.info("the jobC is not exist");
    }
    FileUtil.chmod(executable, "a+x");
    cmd.add(executable);
    
    process = runClient(cmd, env);
    LOG.info("waiting for connect cpp process");
    clientSocket = serverSocket.accept();
    LOG.info("=========run C++ ======");
    this.handler = new TaskHandler(job, staff, workerAgent);
    this.downlink = new BinaryProtocol(clientSocket, handler);
    this.downlink.start();
  }
  
  /**
   * Run a given command in a subprocess, including threads to copy its stdout
   * and stderr to our stdout and stderr.
   * @param command
   *        the command and its arguments
   * @param env
   *        the environment to run the process in
   * @return a handle on the process
   * @throws IOException
   */
  static Process runClient(List<String> command, Map<String, String> env)
      throws IOException {
    ProcessBuilder builder = new ProcessBuilder(command);
    if (env != null) {
      builder.environment().putAll(env);
    }
    Process result = builder.start();
    if(result==null){
    	LOG.info("Application : result is null");
    }else{
    	LOG.info("Application : result is not null");
    }
    return result;
  }
  
  /**
   * Get the downward protocol object that can send commands down to the
   * application.
   * @return the downlink proxy
   */
  public DownwardProtocol getDownlink() {
    return downlink;
  }
  
  /**
   * Get the partitionId for vertex.
   * @return the partition ID
   * */
  public synchronized int getPartitionId(String key, int num)
      throws IOException, InterruptedException {
    this.downlink.sendKey(key, num);
    return this.downlink.getPartionId();
  }
  
  /**
   * Wait for the application to finish.
   * @return did the application finish correctly?
   * @throws Throwable
   */
  public boolean waitForFinish() throws Throwable {
    // downlink.flush();
    return handler.waitForFinish();
  }
  
  /**
   * Abort the application and wait for it to finish.
   * @param t
   *        the exception that signalled the problem
   * @throws IOException
   *         A wrapper around the exception that was passed in
   */
  void abort(Throwable t) throws IOException {
    LOG.info("Aborting because of " + StringUtils.stringifyException(t));
    try {
      downlink.abort();
      downlink.flush();
    } catch (IOException e) {
      // IGNORE cleanup problems
    }
    try {
      handler.waitForFinish();
    } catch (Throwable ignored) {
      process.destroy();
    }
    IOException wrapper = new IOException("pipe child exception");
    wrapper.initCause(t);
    throw wrapper;
  }
  
  /**
   * Clean up the child procress and socket.
   * @throws IOException
   */
  void cleanup() throws IOException {
    serverSocket.close();
    try {
      downlink.close();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
  
  /**
   * Set the Communicator.
   * */
  public void setCommunicator(CommunicatorInterface communicator) {
    this.handler.setCommunicator(communicator);
  }
  
  /**
   * Get TaskHandler.
   * @return TaskHandler
   * */
  public TaskHandler getHandler() {
    return handler;
  }
  
  /**
   * Set TaskHandler.
   * */
  public void setHandler(TaskHandler handler) {
    this.handler = handler;
  }
  
}
