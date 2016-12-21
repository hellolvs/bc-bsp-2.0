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

package com.chinamobile.bcbsp.bspcontroller;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * ClusterStatus for the workers status.
 * @author
 * @version
 */
public class ClusterStatus implements Writable {
  /**active workermanager num*/
  private int activeWorkerManagersCount;
  /**active workermanagers name*/
  private String[] activeWorkerManagersName;
  /**max staffs cluster can hold*/
  private int maxClusterStaffs;
  /**num of cluster running staffs*/
  private int runningClusterStaffs;
  /**BSPcontroller state*/
  private BSPController.State state;
  /**
   * default construction method of cluster status.
   */
  public ClusterStatus() {
  }

  /**
   * construction method depend on the follow parameters
   * @param activeWorkerManagersCount
   *        active workermanager num
   * @param maxClusterStaffs
   *        num of cluster running staffs
   * @param runningClusterStaffs
   *        num of cluster running staffs
   * @param state
   *        BSPcontroller state
   */
  public ClusterStatus(int activeWorkerManagersCount, int maxClusterStaffs,
      int runningClusterStaffs, BSPController.State state) {
    this.activeWorkerManagersCount = activeWorkerManagersCount;
    this.maxClusterStaffs = maxClusterStaffs;
    this.runningClusterStaffs = runningClusterStaffs;
    this.state = state;
  }

  /**
   * construction method depend on the follow parameters
   * @param activeWorkerManagersName
   *        active workermanagers name
   * @param maxClusterStaffs
   *        num of cluster running staffs
   * @param runningClusterStaffs
   *        num of cluster running staffs
   * @param state
   *        BSPcontroller state
   */
  public ClusterStatus(String[] activeWorkerManagersName, int maxClusterStaffs,
      int runningClusterStaffs, BSPController.State state) {
    this(activeWorkerManagersName.length, maxClusterStaffs,
        runningClusterStaffs, state);
    this.activeWorkerManagersName = activeWorkerManagersName;
  }

  /**
   * get active workermanager num.
   * @return
   *        active workermanager num
   */
  public int getActiveWorkerManagersCount() {
    return this.activeWorkerManagersCount;
  }

  /**
   * get workermanager name.
   * @return
   *        return workermanager name.
   */
  public String[] getActiveWorkerManagersName() {
    return this.activeWorkerManagersName;
  }

  /**
   * get cluster max staffs num.
   * @return
   *        return max staff num the cluster can hold.
   */
  public int getMaxClusterStaffs() {
    return this.maxClusterStaffs;
  }

  /**
   * get running staffs num.
   * @return
   *        return running staffs num.
   */
  public int getRunningClusterStaffs() {
    return this.runningClusterStaffs;
  }

  /**
   * get BSPcontroller state
   * @return
   *        BSPcontroller state.
   */
  public BSPController.State getBSPControllerState() {
    return this.state;
  }

  /**
   * rewrite the write method to write the information into hdfs.
   * @param out
   *        DataOutput object to handle hdfs.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.activeWorkerManagersCount);
    if (this.activeWorkerManagersCount == 0) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      WritableUtils.writeCompressedStringArray(out,
          this.activeWorkerManagersName);
    }
    out.writeInt(this.maxClusterStaffs);
    out.writeInt(this.runningClusterStaffs);
    WritableUtils.writeEnum(out, this.state);
  }

  /**
   * rewrite the read method to read information from hdfs.
   * @param in
   *        DataInput object to read from hdfs.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.activeWorkerManagersCount = in.readInt();
    boolean detailed = in.readBoolean();
    if (detailed) {
      this.activeWorkerManagersName = WritableUtils
          .readCompressedStringArray(in);
    }
    this.maxClusterStaffs = in.readInt();
    this.runningClusterStaffs = in.readInt();
    this.state = WritableUtils.readEnum(in, BSPController.State.class);
  }
}
