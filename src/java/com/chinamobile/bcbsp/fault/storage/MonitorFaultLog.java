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

package com.chinamobile.bcbsp.fault.storage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;

/**
 * monitor fault log information and write it into hdfs.
 * @author hadoop
 */
public class MonitorFaultLog {
  /**
   * default constructor the fault log is stored in logs of bsp root directory
   */
  /**handle log in MonitorFaultLog class*/
  public static final Log LOG = LogFactory.getLog(MonitorFaultLog.class);
  private BSPConfiguration conf = new BSPConfiguration();
  public static boolean emailSendFlag =false;
  /**
   * MonitorFaultLog construct method.
   */
  public MonitorFaultLog() {
    this(getFaultStoragePath(), getFaultStoragePath());
  }
  /**
   * @param localFaultDir
   *        local faultlog dir
   */
  public MonitorFaultLog(String localFaultDir) {
    this(localFaultDir, localFaultDir);
  }

  /**
   * get the faultlog storage path
   * @return faultlog storage path
   */
  private static String getFaultStoragePath() {
    String BCBSP_HOME = System.getenv("BCBSP_HOME");
    String FaultStoragePath = BCBSP_HOME + "/logs/faultlog/";
    return FaultStoragePath;
  }

  /**
   * @param localDirPath
   *        local faultlog dir path.
   * @param hdfsDir
   *        ManageFaultLog is used to manage the storage of fault log
   */
  private MonitorFaultLog(String localDirPath, String hdfsDir) {
    String hdfsNamenodehostName = getHdfsNameNodeHostName();
    if (!localDirPath.substring(localDirPath.length() - 1).equals("/") &&
        !localDirPath.substring(localDirPath.length() - 2).equals("\\")) {
      localDirPath = localDirPath + File.separator;
    }
    if (!hdfsDir.substring(hdfsDir.length() - 1).equals("/") &&
        !hdfsDir.substring(hdfsDir.length() - 2).equals("\\")) {
      hdfsDir = hdfsDir + File.separator;
    }
    this.localDirPath = localDirPath;
    this.domainName = hdfsNamenodehostName;
    this.hdfsDir = hdfsDir;
    this.mfl = new ManageFaultLog(domainName, hdfsDir);
  }

  /**
   * get the hdfs namenode hostname
   * @return hdfsNamenodehostName
   */
  private String getHdfsNameNodeHostName() {
    Configuration conf = new Configuration(false);
    String HADOOP_HOME = System.getenv("HADOOP_HOME");
    String corexml = HADOOP_HOME + "/conf/core-site.xml";
    conf.addResource(new Path(corexml));
    String hdfsNamenodehostName = conf.get("fs.default.name");
    return hdfsNamenodehostName;
  }

  /** The default format to use when formating dates */
  static protected final String DEFAULT_DATE_TIME_FORMAT =
      "yyyy/MM/dd HH:mm:ss,SSS";
  /**faultlog timezone information*/
  static protected TimeZone timezone = null;
  /**faultlog stroage path*/
  static protected String storagePath = null;
  /**PrintWriter handle*/
  static private PrintWriter out = null;
  /**local faultlog dirpath*/
  private String localDirPath = null;
  /**domain name*/
  private String domainName = null;
  /**hdfs dir*/
  private String hdfsDir = null;
  /**ManageFaultLog handle*/
  private ManageFaultLog mfl = null;
  /** ? */
  protected int num = 0;
  
  /**
   * faultlog struct.
   * @param fault
   *        record some kinds fault message record the fault message into
   *        specified file
   */
  public void faultLog(Fault fault) {
    StringBuffer buf = new StringBuffer();
    emailSendFlag= conf.getBoolean(Constants.DEFAULT_BC_BSP_JOB_EMAIL_SEND_FLAG, false);
    buf.append(fault.getTimeOfFailure());
    buf.append(" -- ");
    buf.append(fault.getType());
    buf.append(" -- ");
    buf.append(fault.getLevel());
    buf.append(" -- ");
    buf.append(fault.getWorkerNodeName());
    buf.append(" -- ");
    if (fault.getJobName() != "" && fault.getJobName() != null) {
      buf.append(fault.getJobName());
      buf.append(" -- ");
    } else {
      buf.append("null");
      buf.append(" -- ");
    }
    if (fault.getStaffName() != "" && fault.getStaffName() != null) {
      buf.append(fault.getStaffName());
      buf.append(" -- ");
    } else {
      buf.append("null");
      buf.append(" -- ");
    }
    if (fault.getExceptionMessage() != "" &&
        fault.getExceptionMessage() != null) {
      buf.append(" [");
      buf.append(fault.getExceptionMessage());
      buf.append("]");
      buf.append(" -- ");
    } else {
      buf.append("null");
      buf.append(" -- ");
    }
    buf.append(fault.isFaultStatus());
    write(buf);
    if(emailSendFlag == true){
    EmailSender eSender = new EmailSender();
    eSender.sendEmail();
    }
  }

  /**
   * write fault log.
   * @param buffer
   *        according fault time to create directory and write fault file
   */
  protected void write(StringBuffer buffer) {
    Date now = new Date(System.currentTimeMillis());
    timezone = TimeZone.getTimeZone("GMT+08:00");
    Calendar currentTime = Calendar.getInstance();
    currentTime.setTimeZone(timezone);
    currentTime.setTime(now);
    String dateText = null;
    String logFileName = "faultLog.txt";
    String YEAR = String.valueOf(currentTime.get(Calendar.YEAR));
    String MONTH = String.valueOf(currentTime.get(Calendar.MONTH) + 1);
    String DAY = String.valueOf(currentTime.get(Calendar.DAY_OF_MONTH));
    dateText = YEAR + "/" + MONTH + "/" + DAY + "--";
    storagePath = localDirPath + dateText + logFileName;
    write(buffer, storagePath);
  }

  /**
   * write log and manage fault directory
   * @param buffer
   *        faultlog to write
   * @param filePath
   *        fault log path
   */
  protected void write(StringBuffer buffer, String filePath) {
    try {
      File f = new File(filePath);
      if (!f.getParentFile().exists()) {
        f.getParentFile().mkdirs();
        mfl.record(f.getParentFile());
      }
      out = new PrintWriter(new FileWriter(f, true));
      out.println(buffer.toString());
      out.flush();
      out.close();
    } catch (IOException e) {
      //LOG.error("[write]", e);
      throw new RuntimeException("[write]", e);
    }
  }

  /**
   * get the domain name
   * @return domainName
   */
  public String getDomainName() {
    return domainName;
  }

  /**
   * get hdfs dir
   * @return hdfsDir
   */
  public String getHdfsDir() {
    return hdfsDir;
  }

  /**
   * set the hdfs dir
   * @param hdfsDir
   *        hdfsDir to be set.
   */
  public void setHdfsDir(String hdfsDir) {
    this.hdfsDir = hdfsDir;
  }

  /**
   * get local faultdir path
   * @return
   *        local dir path
   */
  public String getLocaldirpath() {
    return localDirPath;
  }

  /**
   * get the fault storage path
   * @return fault storage path
   */
  public static String getStoragePath() {
    return storagePath;
  }

  /**
   * set the fault storage path
   * @param storagePath
   *        storage path to be set.
   */
  public static void setStoragePath(String storagePath) {
    MonitorFaultLog.storagePath = storagePath;
  }
}
