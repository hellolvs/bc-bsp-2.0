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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.chinamobile.bcbsp.Constants;

/**
 * SuperStepReportContainer This class is a container which includes all
 * information used during SuperStep Synchronization. Such as the aggregation
 * values and the synchronization stage.
 */
public class SuperStepReportContainer implements Writable {
  /**The stageflag of the staff.*/
  private int stageFlag = 0;
  /**The dir subaddress while synchronization.*/
  private String[] dirFlag;
  /**The judgeflag of the staff.*/
  private long judgeFlag = 0;
  /**The check files number while synchronization.*/
  private int checkNum = 0;
  /**The partition id of the staff.*/
  private int partitionId = 0;
  /**The local Barrier num while synchronization.*/
  private int localBarrierNum = 0;
  /**The max range of the split.*/
  private int maxRange = 0;
  /**The min range of the split.*/
  private int minRange = 0;
  /**The number of the copy bucket.*/
  private int numCopy = 0;
  /**The port of the worker.*/
  private int port1 = 60000;
  /**The port of the worker.*/
  private int port2 = 60001;
  /**The activeMQ worker name and ports.*/
  private String activeMQWorkerNameAndPorts;
  /**The array of the aggvalues.*/
  private String[] aggValues;
  /**The run time of the staff.*/
  private long staffRunTime = 0;
  /**The id of the staff.*/
  private int sid;
  /**The current superstep of the job.*/
  private int currentSuperStep = -1;
  /**The migratecost of the job.*/
  private long migrateCost = 0;
  /**The migrate information.*/
  private String migrateInfo;
  /**The hashmap contains the vertex count and the partition id.*/
  private HashMap<Integer, Integer> counter = new HashMap<Integer, Integer>();
  /**The number of the edge of the bucket to Partition.*/
  private int[][] bucketToPartitionEdge = new int[this.getCheckNum()
      * this.getNumCopy()][this.getCheckNum()];
  /**The number of the edge of the bucket to bucket.*/
  private int[][] bucketToBucketEdge = new int[this.getCheckNum()
      * this.getNumCopy()][this.getCheckNum() * this.getNumCopy()];
  public void setBucketToPartitionEdgeCounter(
      int[][] bucketToPartitionEdgeCounter) {
    this.bucketToPartitionEdge = bucketToPartitionEdgeCounter;
  }
  public int[][] getBucketToPartitionEdgeCounter() {
    return bucketToPartitionEdge;
  }
  public void setBucketToBucketEdgeCounter(int[][] bucketToBucketEdgeCounter) {
    this.bucketToBucketEdge = bucketToBucketEdgeCounter;
  }
  public int[][] getBucketToBucketEdgeCounter() {
    return bucketToBucketEdge;
  }
  public SuperStepReportContainer(int stageFlag, String[] dirFlag,
      long judgeFlag, String[] aggValues, String migrateInfo) {
    this.stageFlag = stageFlag;
    this.dirFlag = dirFlag;
    this.judgeFlag = judgeFlag;
    this.aggValues = aggValues;
    this.migrateInfo = migrateInfo;
  }
  public SuperStepReportContainer() {
    this.dirFlag = new String[0];
    this.aggValues = new String[0];
  }
  public SuperStepReportContainer(int stageFlag, String[] dirFlag,
      long judgeFlag) {
    this.stageFlag = stageFlag;
    this.dirFlag = dirFlag;
    this.judgeFlag = judgeFlag;
    this.aggValues = new String[0];
  }
  public SuperStepReportContainer(int stageFlag, String[] dirFlag,
      long judgeFlag, String[] aggValues) {
    this.stageFlag = stageFlag;
    this.dirFlag = dirFlag;
    this.judgeFlag = judgeFlag;
    this.aggValues = aggValues;
  }
  public SuperStepReportContainer(String s) {
    if (s.equals("RECOVERY")) {
      return;
    }
    String[] strs = s.split("/");
    if (strs.length != 1) {
      this.migrateInfo = "";
      for (int i = 1; i < strs.length; i++) {
        this.migrateInfo += (strs[i] + "/");
      }
    }
    String[] content = strs[0].split(Constants.SPLIT_FLAG);
    this.judgeFlag = Integer.valueOf(content[0]);
    int count = content.length - 1;
    this.aggValues = new String[count];
    for (int i = 0; i < count; i++) {
      this.aggValues[i] = content[i + 1];
    }
  }
  public void setActiveMQWorkerNameAndPorts(String str) {
    this.activeMQWorkerNameAndPorts = str;
  }
  public String getActiveMQWorkerNameAndPorts() {
    return this.activeMQWorkerNameAndPorts;
  }
  public int getLocalBarrierNum() {
    return localBarrierNum;
  }
  public void setLocalBarrierNum(int localBarrierNum) {
    this.localBarrierNum = localBarrierNum;
  }
  public void setStageFlag(int stageFlag) {
    this.stageFlag = stageFlag;
  }
  public int getStageFlag() {
    return this.stageFlag;
  }
  public void setDirFlag(String[] dirFlag) {
    this.dirFlag = dirFlag;
  }
  public String[] getDirFlag() {
    return this.dirFlag;
  }
  public void setJudgeFlag(long judgeFlag) {
    this.judgeFlag = judgeFlag;
  }
  public long getJudgeFlag() {
    return this.judgeFlag;
  }
  public void setCheckNum(int checkNum) {
    this.checkNum = checkNum;
  }
  public int getCheckNum() {
    return this.checkNum;
  }
  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }
  public int getPartitionId() {
    return this.partitionId;
  }
  public int getPort1() {
    return port1;
  }
  public void setPort1(int port) {
    this.port1 = port;
  }
  public int getPort2() {
    return port2;
  }
  public void setPort2(int port) {
    this.port2 = port;
  }
  public void setMaxRange(int maxRange) {
    this.maxRange = maxRange;
  }
  public int getMaxRange() {
    return this.maxRange;
  }
  public void setMinRange(int minRange) {
    this.minRange = minRange;
  }
  public int getMinRange() {
    return this.minRange;
  }
  public String[] getAggValues() {
    return aggValues;
  }
  public void setAggValues(String[] aggValues) {
    this.aggValues = aggValues;
  }
  public HashMap<Integer, Integer> getCounter() {
    return counter;
  }
  public void setCounter(HashMap<Integer, Integer> counter) {
    this.counter = counter;
  }
  public int getNumCopy() {
    return numCopy;
  }
  public void setNumCopy(int numCopy) {
    this.numCopy = numCopy;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    this.partitionId = in.readInt();
    this.stageFlag = in.readInt();
    int count = in.readInt();
    this.dirFlag = new String[count];
    for (int i = 0; i < count; i++) {
      this.dirFlag[i] = Text.readString(in);
    }
    this.judgeFlag = in.readLong();
    this.localBarrierNum = in.readInt();
    this.port1 = in.readInt();
    this.port2 = in.readInt();
    count = in.readInt();
    this.aggValues = new String[count];
    for (int i = 0; i < count; i++) {
      this.aggValues[i] = Text.readString(in);
    }
    this.staffRunTime = in.readLong();
    this.sid = in.readInt();
    this.currentSuperStep = in.readInt();
    this.migrateCost = in.readLong();
  }
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.partitionId);
    out.writeInt(this.stageFlag);
    out.writeInt(this.dirFlag.length);
    int count = this.dirFlag.length;
    for (int i = 0; i < count; i++) {
      Text.writeString(out, this.dirFlag[i]);
    }
    out.writeLong(this.judgeFlag);
    out.writeInt(this.localBarrierNum);
    out.writeInt(this.port1);
    out.writeInt(this.port2);
    out.writeInt(this.aggValues.length);
    count = this.aggValues.length;
    for (int i = 0; i < count; i++) {
      Text.writeString(out, this.aggValues[i]);
    }
    out.writeLong(this.staffRunTime);
    out.writeInt(this.sid);
    out.writeInt(this.currentSuperStep);
    out.writeLong(this.migrateCost);
  }
  @Override
  public String toString() {
    String content = Long.toString(this.judgeFlag);
    for (int i = 0; i < this.aggValues.length; i++) {
      content = content + Constants.SPLIT_FLAG + this.aggValues[i];
    }
    return content;
  }
  public void setStaffRunTime(long time) {
    this.staffRunTime = time;
  }
  public long getStaffRunTime() {
    return this.staffRunTime;
  }
  public void setStaffID(int id) {
    this.sid = id;
  }
  public int getStaffID() {
    return sid;
  }
  public void setCurrentSuperStep(int ss) {
    this.currentSuperStep = ss;
  }
  public int getCurrentSuperStep() {
    return this.currentSuperStep;
  }
  public void setMigrateCost(long time) {
    this.migrateCost = time;
  }
  public long getMigrateCost() {
    return this.migrateCost;
  }
  public String getMigrateInfo() {
    return this.migrateInfo;
  }
  public String toStringForMigrate() {
    String content = Long.toString(this.judgeFlag);
    for (int i = 0; i < this.aggValues.length; i++) {
      content = content + Constants.SPLIT_FLAG + this.aggValues[i];
    }
    content = content + "/" + this.migrateInfo;
    return content;
  }
  
  /** For JUnit test. */
  public void setMigrateInfo(String migrateInfo) {
    this.migrateInfo = migrateInfo;
  }
}
