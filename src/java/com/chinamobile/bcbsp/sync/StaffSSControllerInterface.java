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

import java.util.HashMap;
import java.util.List;

import com.chinamobile.bcbsp.bspcontroller.Counters;

/**
 * StaffSSControllerInterface StaffSSController for completing the staff
 * SuperStep synchronization control. This class is connected to BSPStaff.
 * @author
 * @version
 */
public interface StaffSSControllerInterface {
  /**
   * Make sure than all staffs have started successfully. This function should
   * create the route table.
   * @param ssrc
   *        SuperStepReportContainer
   * @return hashmap
   */
  public HashMap<Integer, String> scheduleBarrier(SuperStepReportContainer
      ssrc);
  /**
   * Make sure that all staffs complete loading data This function is used for
   * Constatns.PARTITION_TYPE.RANGE.
   * @param ssrc
   *        SuperStepReportContainer
   * @return hashmap
   */
  public HashMap<Integer, List<Integer>> loadDataBarrier(
      SuperStepReportContainer ssrc);
  /**
   * Make sure that all staffs complete loading data This function is used for
   * Constatns.PARTITION_TYPE.HASH.
   * @param ssrc
   *        SuperStepReportContainer
   * @param partitionType
   *        the type of the partition.
   * @return true while finished.
   */
  public boolean loadDataBarrier(SuperStepReportContainer ssrc,
      String partitionType);
  /**
   * Make sure that all staffs complete Balancing data This function is used for
   * Constatns.PARTITION_TYPE.HASH.
   * @param ssrc
   *        SuperStepReportContainer
   * @param partitionType
   *        the type of the partition.
   * @return hashmap of bucket to partition.
   */
  public HashMap<Integer, Integer> loadDataInBalancerBarrier(
      SuperStepReportContainer ssrc, String partitionType);
  /**
   * Make sure that all staffs have completed the local computation and
   * message-receiving.
   * @param superStepCounter
   *        superStepCounter
   * @param ssrc
   *        SuperStepReportContainer
   * @return true while finished.
   */
  public boolean firstStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc);
  /**
   * Report the local information and get the next SuperStep command from
   * JobInProgress.
   * @param superStepCounter
   *        superStepCounter
   * @param ssrc
   *        SuperStepReportContainer
   * @return superstepcommand.
   */
  public SuperStepCommand secondStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc);
  /**
   * Make sure that all staffs have completed the checkpoint-write operation.
   * @param superStepCounter
   *        superStepCounter
   * @param ssrc
   *        SuperStepReportContainer
   * @return hashmap
   */
  public HashMap<Integer, String> checkPointStageSuperStepBarrier(
      int superStepCounter, SuperStepReportContainer ssrc);
  /**
   * Make sure that all staffs have saved the computation result and the job
   * finished successfully.
   * @param superStepCounter
   *        superStepCounter
   * @param ssrc
   *        SuperStepReportContainer
   * @return true while finished.
   */
  public boolean saveResultStageSuperStepBarrier(int superStepCounter,
      SuperStepReportContainer ssrc);
  /**
   * @param superStepCounter
   *        superStepCounter
   * @return superstepcommand.
   */
  public SuperStepCommand secondStageSuperStepBarrierForRecovery(
      int superStepCounter);
  /**
   * set counters.
   * @param counters
   *        The parameter while sync
   */
  public void setCounters(Counters counters);
  /**
   * @param ssrc
   *        SuperStepReportContainer
   * @return hashmap
   */
  public HashMap<Integer, String> scheduleBarrierForMigrate(
      SuperStepReportContainer ssrc);
  /**
   * @param ssrc
   *        SuperStepReportContainer
   * @param hash
   *        hash method.
   * @return hashmap
   */
  public HashMap<Integer, Integer> loadDataInBalancerBarrieraForEdgeBalance(
      SuperStepReportContainer ssrc, String hash);
  /**
   * @param ssrc
   *        SuperStepReportContainer
   * @param partitionType
   *        hash method.
   * @return hashmap
   */
  public HashMap<Integer, Integer> loadDataInBalancerBarrieraForIBHP(
      SuperStepReportContainer ssrc, String partitionType);
  /**
   * @param ssrc
   *        SuperStepReportContainer
   * @return hashmap
   */
  public HashMap<Integer, Integer> rangerouter(SuperStepReportContainer ssrc);
}
