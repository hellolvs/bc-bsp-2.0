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

package com.chinamobile.bcbsp.sync;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.chinamobile.bcbsp.Constants;

/**
 * SuperStepCommand SuperStepCommand maintains the actions which JobInProgress
 * generates and the aggregation information
 * @author
 * @version
 */

public class SuperStepCommand implements Writable {
  /**The log of the class.*/
  private static final Log LOG = LogFactory.getLog(SuperStepCommand.class);
  /**The Type of the command.*/
  private int commandType;
  /**The init WritePath.*/
  private String initWritePath = "initWritePath";
  /**The init ReadPath.*/
  private String initReadPath = "initReadPath";
  /**The able of the check point.*/
  private int ableCheckPoint = 0;
  /**The next superstep number.*/
  private int nextSuperStepNum = 0;
  /**The old checkpoint.*/
  private int oldCheckPoint = 0;
  /**The hashmap of the partition to workmanagernameandport.*/
  private HashMap<Integer, String> partitionToWorkerManagerNameAndPort = null;
  /**The staffid of the migrated.*/
  private String migrateStaffIDs = "";
  /**The array of the aggvalues.*/
  private String[] aggValues;
  public SuperStepCommand() {
    this.aggValues = new String[0];
    this.partitionToWorkerManagerNameAndPort = new HashMap<Integer, String>();
  }
  /**
   * @param aCommandType the type of the command.
   */
  public SuperStepCommand(int aCommandType) {
    this.commandType = aCommandType;
  }
  /**
   * @param commandType the type of the command.
   * @param initWritePath The init writepath.
   * @param initReadPath The init readpath.
   * @param ableCheckPoint The ablecheckpoint.
   * @param nextSuperStepNum The number of the nextsuperstep.
   */
  public SuperStepCommand(int commandType, String initWritePath,
      String initReadPath, int ableCheckPoint, int nextSuperStepNum) {
    this.commandType = commandType;
    this.initWritePath = initWritePath;
    this.initReadPath = initReadPath;
    this.ableCheckPoint = ableCheckPoint;
    this.nextSuperStepNum = nextSuperStepNum;
  }
  /**
   * @param commandType the type of the command.
   * @param initWritePath The init writepath.
   * @param initReadPath The init readpath.
   * @param ableCheckPoint The ablecheckpoint.
   * @param nextSuperStepNum The number of the nextsuperstep.
   * @param aggValues The array of the aggvalues.
   */
  public SuperStepCommand(int commandType, String initWritePath,
      String initReadPath, int ableCheckPoint, int nextSuperStepNum,
      String[] aggValues) {
    this.commandType = commandType;
    this.initWritePath = initWritePath;
    this.initReadPath = initReadPath;
    this.ableCheckPoint = ableCheckPoint;
    this.nextSuperStepNum = nextSuperStepNum;
    this.aggValues = aggValues;
  }
  public SuperStepCommand(String s) {
    String[] infos = s.split("@");
    if (infos.length == 2) {
      this.migrateStaffIDs = infos[1];
    }
    String[] tmp = infos[0].split(Constants.SSC_SPLIT_FLAG);
    int length = tmp.length;
    int index = 0;
    if (length < 6) {
    } else {
      this.commandType = Integer.valueOf(tmp[index++]);
      this.initWritePath = tmp[index++];
      this.initReadPath = tmp[index++];
      this.ableCheckPoint = Integer.valueOf(tmp[index++]);
      this.nextSuperStepNum = Integer.valueOf(tmp[index++]);
      this.oldCheckPoint = Integer.valueOf(tmp[index++]);
    }
    LOG.info("[SuperStepCommand]--[index]" + index);
    LOG.info("[SuperStepCommand]--[CommandType]" + this.commandType);
    if (this.commandType == Constants.COMMAND_TYPE.START_AND_RECOVERY
        || !this.migrateStaffIDs.equals("")) {
      String str = tmp[index++];
      LOG.info("[SuperStepCommand]--[routeString]" + str);
      String regEx = "[\\{\\}]";
      Pattern p = Pattern.compile(regEx);
      Matcher m = p.matcher(str);
      str = m.replaceAll("");
      str = str.replace(" ", "");
      this.partitionToWorkerManagerNameAndPort = new HashMap<Integer, String>();
      String[] strMap = str.split(",");
      for (int i = 0; i < strMap.length; i++) {
        String[] strKV = strMap[i].split("=");
        if (strKV.length == 2) {
          this.partitionToWorkerManagerNameAndPort.put(
              Integer.parseInt(strKV[0]), strKV[1]);
        } else {
          continue;
        }
      }
    }
    if (index < length) {
      int count = length - index;
      this.aggValues = new String[count];
      for (int i = 0; i < count; i++) {
        this.aggValues[i] = tmp[index++];
      }
    }
  }
  public void setMigrateStaffIDs(String ids) {
    this.migrateStaffIDs = ids;
  }
  public String getMigrateStaffIDs() {
    return this.migrateStaffIDs;
  }
  public int getOldCheckPoint() {
    return oldCheckPoint;
  }
  public void setOldCheckPoint(int oldCheckPoint) {
    this.oldCheckPoint = oldCheckPoint;
  }
  public void setCommandType(int commandType) {
    this.commandType = commandType;
  }
  public int getCommandType() {
    return this.commandType;
  }
  public void setInitWritePath(String initWritePath) {
    this.initWritePath = initWritePath;
  }
  public String getInitWritePath() {
    return this.initWritePath;
  }
  public void setInitReadPath(String initReadPath) {
    this.initReadPath = initReadPath;
  }
  public String getInitReadPath() {
    return this.initReadPath;
  }
  public void setAbleCheckPoint(int ableCheckPoint) {
    this.ableCheckPoint = ableCheckPoint;
  }
  public int getAbleCheckPoint() {
    return this.ableCheckPoint;
  }
  public void setNextSuperStepNum(int nextSuperStepNum) {
    this.nextSuperStepNum = nextSuperStepNum;
  }
  public int getNextSuperStepNum() {
    return this.nextSuperStepNum;
  }
  public void setAggValues(String[] aggValues) {
    this.aggValues = aggValues;
  }
  public String[] getAggValues() {
    return aggValues;
  }
  public void setPartitionToWorkerManagerNameAndPort(
      HashMap<Integer, String> partitionToWorkerManagerNameAndPort) {
    this.partitionToWorkerManagerNameAndPort = partitionToWorkerManagerNameAndPort;
  }
  public HashMap<Integer, String> getPartitionToWorkerManagerNameAndPort() {
    return partitionToWorkerManagerNameAndPort;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    this.commandType = in.readInt();
    this.initWritePath = Text.readString(in);
    this.initReadPath = Text.readString(in);
    this.ableCheckPoint = in.readInt();
    this.nextSuperStepNum = in.readInt();
    this.oldCheckPoint = in.readInt();
    int count = in.readInt();
    this.aggValues = new String[count];
    for (int i = 0; i < count; i++) {
      this.aggValues[i] = Text.readString(in);
    }
    int size = WritableUtils.readVInt(in);
    if (size > 0) {
      String[] partitionToWMName = WritableUtils.readCompressedStringArray(in);
      this.partitionToWorkerManagerNameAndPort = new HashMap<Integer, String>();
      for (int j = 0; j < size; j++) {
        this.partitionToWorkerManagerNameAndPort.put(j, partitionToWMName[j]);
      }
    }
    this.migrateStaffIDs = in.readUTF();

  }
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.commandType);
    Text.writeString(out, this.initWritePath);
    Text.writeString(out, this.initReadPath);
    out.writeInt(this.ableCheckPoint);
    out.writeInt(this.nextSuperStepNum);
    out.writeInt(this.oldCheckPoint);
    out.writeInt(this.aggValues.length);
    int count = this.aggValues.length;
    for (int i = 0; i < count; i++) {
      Text.writeString(out, this.aggValues[i]);
    }
    if (partitionToWorkerManagerNameAndPort == null) {
      WritableUtils.writeVInt(out, 0);
    } else {
      WritableUtils.writeVInt(out, partitionToWorkerManagerNameAndPort.size());
      String[] partitionToWMName = null;
      for (Integer i : this.partitionToWorkerManagerNameAndPort.keySet()) {
        partitionToWMName[i] = partitionToWorkerManagerNameAndPort.get(i);
      }
      WritableUtils.writeCompressedStringArray(out, partitionToWMName);
    }
    out.writeUTF(this.migrateStaffIDs);
  }
  @Override
  public String toString() {
    String content = this.commandType + Constants.SSC_SPLIT_FLAG
        + this.initWritePath + Constants.SSC_SPLIT_FLAG + this.initReadPath
        + Constants.SSC_SPLIT_FLAG + this.ableCheckPoint
        + Constants.SSC_SPLIT_FLAG + this.nextSuperStepNum
        + Constants.SSC_SPLIT_FLAG + this.oldCheckPoint;
    if (this.commandType == Constants.COMMAND_TYPE.START_AND_RECOVERY
        || !this.migrateStaffIDs.equals("")) {
      if (this.partitionToWorkerManagerNameAndPort == null) {
        LOG.error("This partitionToWorkerManagerNameAndPort is null");
      } else {
        content = content + Constants.SSC_SPLIT_FLAG
            + this.partitionToWorkerManagerNameAndPort;
      }
    }
    for (int i = 0; i < this.aggValues.length; i++) {
      content = content + Constants.SSC_SPLIT_FLAG + this.aggValues[i];
    }
    content += ("@" + this.migrateStaffIDs);
    return content;
  }
}
