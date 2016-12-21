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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.chinamobile.bcbsp.util.BSPJobID;
/**
 * this class contains all kinds of fault that may happed during
 * job running and the fault type and information.
 * @author hadoop
 *
 */
public class Fault implements Writable, Cloneable {
  /**handle log fault class*/
  static final Log LOG = LogFactory.getLog(Fault.class);
  /**fault type*/
  public static enum Type {
    WORKERNODE, DISK, SYSTEMSERVICE, NETWORK, FORCEQUIT
  }
  /**fault level*/
  public static enum Level {
    INDETERMINATE, WARNING, MINOR, MAJOR, CRITICAL
  }
  /**fault default date and time*/
  protected static final String DEFAULT_DATE_TIME_FORMAT =
      "yyyy/MM/dd HH:mm:ss,SSS";
  /**SimpleDateFormat handle*/
  private static SimpleDateFormat dateFormat = new SimpleDateFormat(
      DEFAULT_DATE_TIME_FORMAT);
  /**fault type disk fault*/
  private Type type = Type.DISK;
  /**fault level critical*/
  private Level level = Level.CRITICAL;
  /**fault happened time*/
  private String timeOfFailure = "";
  /**fault happened workername*/
  private String workerNodeName = "";
  /**fault happened jobname*/
  private String jobName = "";
  /**fault happened staffname*/
  private String staffName = "";
  /**fault exception message*/
  private String exceptionMessage = "";
  /**fault status true default*/
  private boolean faultStatus = true;
  /**fault happened superstep num 0 default*/
  private int superStep_Stage = 0;

  /**
   * fault construct method.
   */
  public Fault() {
  }

  /**
   * used for staff without super step stage
   * The order of parameter :Type Level ,workerName
   * ,ExceptionMessage,JobName,StaffName.null maybe used when one or more
   * parameter is not needed;
   * @param type
   *        fault type
   * @param level
   *        fault level
   * @param workerNodeName
   *        fault happened workernoe name
   * @param exceptionMessage
   *        exceptions message
   * @param jobName
   *        fault happened job name
   * @param staffName
   *        fault happened staff name
   */
  public Fault(Type type, Level level, String workerNodeName,
      String exceptionMessage, String jobName, String staffName) {
    this(type, level, workerNodeName, exceptionMessage, jobName, staffName, 0);
  }

  /**
   * used for workernode
   * @param type
   *        fault type
   * @param level
   *        fault level
   * @param workerNodeName
   *        fault happened workernoe name
   * @param exceptionMessage
   *        exceptions message
   */
  public Fault(Type type, Level level, String workerNodeName,
      String exceptionMessage) {
    this(type, level, workerNodeName, exceptionMessage, "null", "null", 0);
  }

  /**
   * used for staff with superstep stage.
   * @param type
   *        fault type
   * @param level
   *        fault level
   * @param workerNodeName
   *        fault happened workernoe name
   * @param exceptionMessage
   *        exceptions message
   * @param jobName
   *        fault happened job name
   * @param staffName
   *        fault happened staff name
   * @param superStep_Stage
   *        fault happened superstep stage.
   */
  public Fault(Type type, Level level, String workerNodeName,
      String exceptionMessage, String jobName, String staffName,
      int superStep_Stage) {
    this.type = type;
    this.level = level;
    this.timeOfFailure = dateFormat.format(new Date());
    this.jobName = jobName;
    this.staffName = staffName;
    this.exceptionMessage = exceptionMessage;
    this.workerNodeName = workerNodeName;
    this.superStep_Stage = superStep_Stage;
  }

  /**
   * fault construct method
   * @param type
   *        fault type
   * @param level
   *        fault level
   * @param jobID
   *        fault happened job ID
   * @param exceptionMessage
   *        exceptions message
   */
  public Fault(Type type, Level level, BSPJobID jobID,
      String exceptionMessage) {
    this.type = type;
    this.level = level;
    this.timeOfFailure = dateFormat.format(new Date());
    this.jobName = jobID.toString();
    this.exceptionMessage = exceptionMessage;
  }

  /**
   * get the fault happened superstep stage
   * @return superstep stage.
   */
  public int getSuperStep_Stage() {
    return superStep_Stage;
  }

  /**
   * set the job happened superstep stage.
   * @param superStep_Stage
   *        superstep stage to be set.
   */
  public void setSuperStep_Stage(int superStep_Stage) {
    this.superStep_Stage = superStep_Stage;
  }

  /**
   * set fault type
   * @param type
   *        fault type to be set.
   */
  public void setType(Type type) {
    this.type = type;
  }

  /**
   * set fault level
   * @param level
   *        fault level to be set.
   */
  public void setLevel(Level level) {
    this.level = level;
  }

  /**
   * set fault happened time.
   * @param timeOfFailure
   *        failure happened time.
   */
  public void setTimeOfFailure(String timeOfFailure) {
    this.timeOfFailure = timeOfFailure;
  }

  /**
   * set exception message.
   * @param exceptionMessage
   *        exception message to be set.
   */
  public void setExceptionMessage(String exceptionMessage) {
    this.exceptionMessage = exceptionMessage;
  }

  /**
   * get the fault type
   * @return
   *        fault type.
   */
  public Type getType() {
    return type;
  }

  /**
   * get the fault level.
   * @return
   *        fault level.
   */
  public Level getLevel() {
    return level;
  }

  /**
   * get the time of fault happened.
   * @return time of failure.
   */
  public String getTimeOfFailure() {
    return timeOfFailure;
  }

  /**
   * get the fault happened workernode name.
   * @return workernode name.
   */
  public String getWorkerNodeName() {
    return workerNodeName;
  }

  /**
   * set the workernode name
   * @param workerNodeName
   *        workernode name to be set
   */
  public void setWorkerNodeName(String workerNodeName) {
    this.workerNodeName = workerNodeName;
  }

  /**
   * get the fault job name.
   * @return job name.
   */
  public String getJobName() {
    return jobName;
  }

  /**
   * get the staff name
   * @return fault happened staff name.
   */
  public String getStaffName() {
    return staffName;
  }

  /**
   * set the fault job name
   * @param jobName
   *        job name to be set.
   */
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  /**
   * set the fault happened staff name.
   * @param staffName
   *        staff name to be set.
   */
  public void setStaffName(String staffName) {
    this.staffName = staffName;
  }

  /**
   * set fault status
   * @param faultStatus
   *        fault status to be set.
   */
  public void setFaultStatus(boolean faultStatus) {
    this.faultStatus = faultStatus;
  }

  /**
   * get fault status.
   * @return faultStauts
   */
  public boolean isFaultStatus() {
    return faultStatus;
  }

  /**
   * get fault happened exception message
   * @return exception message.
   */
  public String getExceptionMessage() {
    return exceptionMessage;
  }

  /**
   * write fault information.
   * @param out
   *        data to output.
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, this.type);
    WritableUtils.writeEnum(out, this.level);
    Text.writeString(out, this.timeOfFailure);
    Text.writeString(out, this.workerNodeName);
    Text.writeString(out, this.jobName);
    Text.writeString(out, this.staffName);
    Text.writeString(out, this.exceptionMessage);
    out.writeInt(this.superStep_Stage);
  }

  /**
   * read fault information.
   * @param in
   *        fault information to be read.
   */
  public void readFields(DataInput in) throws IOException {
    this.type = WritableUtils.readEnum(in, Type.class);
    this.level = WritableUtils.readEnum(in, Level.class);
    this.timeOfFailure = Text.readString(in);
    this.workerNodeName = Text.readString(in);
    this.jobName = Text.readString(in);
    this.staffName = Text.readString(in);
    this.exceptionMessage = Text.readString(in);
    this.superStep_Stage = in.readInt();
  }

  @Override
  public String toString() {
    return this.timeOfFailure + "--" + this.type.toString() +
        "--" + this.level +
        "--" + this.workerNodeName + "--" + this.jobName + "--" +
        this.staffName + "--" + this.exceptionMessage + "--" +
        this.faultStatus + "--" + this.superStep_Stage;
  }

  /**
   * judge whether two fault objects
   * are equal according to fault time of failure
   * and workernode name.
   * @param obj fault object to be judged
   * @return equal true not false.
   */
  public boolean equals(Object obj) {
    Fault fault = (Fault) obj;
    if (this.timeOfFailure.equals(fault.timeOfFailure) &&
        this.workerNodeName.equals(fault.workerNodeName)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * corresponding definition of
   *  'hashCode()' for equal()
   * @return sum of timeOfFailure and workerNodeName hashcode.
   */
  public int hashCode() {
    return timeOfFailure.hashCode() + workerNodeName.hashCode();
  }

  /**
   * clone the fault object.
   * @return cloned fault.
   */
  public Fault clone() throws CloneNotSupportedException {
    Fault fault;
    fault = (Fault) super.clone();
    return fault;
  }
}
