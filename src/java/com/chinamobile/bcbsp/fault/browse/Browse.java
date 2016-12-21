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

package com.chinamobile.bcbsp.fault.browse;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPHdfs;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;

/**
 * the browse reduce the List<fault> that can use in the jsp --list List<Fault>
 * sortrecords = new ArrayList<Fault>();
 */
public class Browse {
  // private String Path = null;
  /**readFaultlog handle*/
  public ReadFaultlog readFaultlog = null;
  /**SortList<Fault> handle*/
  public SortList<Fault> sortFault = new SortList<Fault>();
  /**List<Fault> handle*/
  public List<Fault> sortrecords = new ArrayList<Fault>();
  /**if the fault is recorded default flag?*/
  boolean recordDefaultflag = false;
  /**fault recorded flag?*/
  boolean recordflag = false;
  /**date of last fault log?*/
  Date before = null;
  /**date of right now*/
  Date now = null;
  /**time interval to get the fault log from worker*/
  long bufferedTime = 30000;
  /**real time interval to get the fault log*/
  long time = 0;
  // -----------------retrieveByLevel(int month);--------------------

  /**
   * browse faultlog dir
   */
  public Browse() {
    String BCBSP_HOME = System.getenv("BCBSP_HOME");
    String FaultStoragePath = BCBSP_HOME + "/logs/faultlog";
    readFaultlog = new ReadFaultlog(FaultStoragePath, getHdfshostName());
  }

  /**
   * get the record location futhur more to get the faultlog in localdisk or
   * hdfs
   * @return HDFS namenode hostname.
   */
  public String getHdfshostName() {
    // alter by gtt
    // Configuration conf = new Configuration(false);
    String HADOOP_HOME = System.getenv("HADOOP_HOME");
    String corexml = HADOOP_HOME + "/conf/core-site.xml";
    // conf.addResource(new Path(corexml));
    // String hdfsNamenodehostName = conf.get("fs.default.name");
    BSPHdfs HdfsConf = new BSPHdfsImpl(false);
    HdfsConf.hdfsConf(corexml);
    String hdfsNamenodehostName = HdfsConf.hNhostname();
    return hdfsNamenodehostName;
  }

  /**
   * get the list of Fault ordered by Level
   * @return sorted records with level.
   */
  public List<Fault> retrieveByLevel() {
    if (!recordDefaultflag) {
      sortrecords = readFaultlog.read();
      before = new Date();
      recordDefaultflag = true;
    } else {
      now = new Date();
      time = now.getTime() - before.getTime();
      if (time > bufferedTime) {
        sortrecords = readFaultlog.read();
        before = new Date();
      }
    }
    sortFault.Sort(sortrecords, "getLevel", null);
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by Worker
   * @return sorted records with worker.
   */
  public List<Fault> retrieveByPosition() {
    if (!recordDefaultflag) {
      sortrecords = readFaultlog.read();
      before = new Date();
      recordDefaultflag = true;
    } else {
      now = new Date();
      time = now.getTime() - before.getTime();
      if (time > bufferedTime) {
        sortrecords = readFaultlog.read();
        before = new Date();
      }
    }
    sortFault.Sort(sortrecords, "getWorkerNodeName", null);
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by record type.
   * @return sorted records
   */
  public List<Fault> retrieveByType() {
    if (!recordDefaultflag) {
      sortrecords = readFaultlog.read();
      before = new Date();
      recordDefaultflag = true;
    } else {
      now = new Date();
      time = now.getTime() - before.getTime();
      if (time > bufferedTime) {
        sortrecords = readFaultlog.read();
        before = new Date();
      }
    }
    sortFault.Sort(sortrecords, "getType", null);
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by time of failure.
   * @return sorted records
   */
  public List<Fault> retrieveByTime() {
    if (!recordDefaultflag) {
      sortrecords = readFaultlog.read();
      before = new Date();
      recordDefaultflag = true;
    } else {
      now = new Date();
      time = now.getTime() - before.getTime();
      if (time > bufferedTime) {
        sortrecords = readFaultlog.read();
        before = new Date();
      }
    }
    sortFault.Sort(sortrecords, "getTimeOfFailure", "desc");
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by Level in monthnum month
   * @param monthnum month num of the records.
   * @return sorted records.
   */
  public List<Fault> retrieveByLevel(int monthnum) {
    recordDefaultflag = false;
    sortrecords = readFaultlog.read(monthnum);
    sortFault.Sort(sortrecords, "getLevel", null);
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by position in monthnum month
   * @param monthnum month num of the records.
   * @return sorted records
   */
  public List<Fault> retrieveByPosition(int monthnum) {
    recordDefaultflag = false;
    sortrecords = readFaultlog.read(monthnum);
    sortFault.Sort(sortrecords, "getWorkerNodeName", null);
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by type in monthnum month
   * @param monthnum month num of the records.
   * @return sorted records.
   */
  public List<Fault> retrieveByType(int monthnum) {
    recordDefaultflag = false;
    sortrecords = readFaultlog.read(monthnum);
    sortFault.Sort(sortrecords, "getType", null);
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by time of failure in monthnum month
   * @param monthnum month num of the records.
   * @return sorted records.
   */
  public List<Fault> retrieveByTime(int monthnum) {
    recordDefaultflag = false;
    sortrecords = readFaultlog.read(monthnum);
    sortFault.Sort(sortrecords, "getTimeOfFailure", "desc");
    return sortrecords;
  }

  /**
   *  retrieve by one key level and sort by Level
   * @param faultLevel
   *        fault record level.
   * @return sorted records.
   */
  public List<Fault> retrieveByLevel(String faultLevel) {
    recordDefaultflag = false;
    String[] keys = {faultLevel};
    sortrecords = readFaultlog.read(keys);
    sortFault.Sort(sortrecords, "getLevel", null);
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by Worker
   * @param faultPostion
   *        fautl position in which worker,
   * @return sorted records.
   */
  public List<Fault> retrieveByPosition(String faultPostion) {
    recordDefaultflag = false;
    String[] keys = {faultPostion};
    sortrecords = readFaultlog.read(keys);
    sortFault.Sort(sortrecords, "getWorkerNodeName", null);
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by record type in fault type.
   * @param faultType get the fault type of records.
   * @return sorted records.
   */
  public List<Fault> retrieveByType(String faultType) {
    recordDefaultflag = false;
    String[] keys = {faultType};
    sortrecords = readFaultlog.read(keys);
    sortFault.Sort(sortrecords, "getType", null);
    return sortrecords;
  }

  /**
   * get the list of Fault ordered by record occur time.
   * @param faultOccurTime
   *        fault record occur time.
   * @return sorted records.
   */
  public List<Fault> retrieveByTime(String faultOccurTime) {
    recordDefaultflag = false;
    String[] keys = {faultOccurTime};
    sortrecords = readFaultlog.read(keys);
    sortFault.Sort(sortrecords, "getTimeOfFailure", "desc");
    return sortrecords;
  }

  /**
   * retrieve by one key level and sort by Level,in monthnum month;
   * @param faultLevel
   *        fault record level.
   * @param monthnum month num.
   * @return sorted records.
   */
  public List<Fault> retrieveByLevel(String faultLevel, int monthnum) {
    recordDefaultflag = false;
    String[] keys = {faultLevel};
    sortrecords = readFaultlog.read(keys, monthnum);
    sortFault.Sort(sortrecords, "getLevel", null);
    return sortrecords;
  }

  /**
   * retrieve by one key level and sort by fault position,in monthnum month;
   * @param faultPostion
   *        fault occurs workername.
   * @param monthnum
   *        month num.
   * @return sorted records.
   */
  public List<Fault> retrieveByPosition(String faultPostion, int monthnum) {
    recordDefaultflag = false;
    String[] keys = {faultPostion};
    sortrecords = readFaultlog.read(keys, monthnum);
    sortFault.Sort(sortrecords, "getWorkerNodeName", null);
    return sortrecords;
  }

  /**
   * retrieve by one key level and sort by fault type,in monthnum month;
   * @param faultType
   *        fault record type.
   * @param monthnum
   *         month num.
   * @return sorted records.
   */
  public List<Fault> retrieveByType(String faultType, int monthnum) {
    recordDefaultflag = false;
    String[] keys = {faultType};
    sortrecords = readFaultlog.read(keys, monthnum);
    sortFault.Sort(sortrecords, "getType", null);
    return sortrecords;
  }

  /**
   * retrieve by one key "desc?" and sort by fault occurs time,
   * in monthnum month.
   * @param faultOccurTime
   *        fault records occured time.
   * @param monthnum
   *        month num.
   * @return sorted records.
   */
  public List<Fault> retrieveByTime(String faultOccurTime, int monthnum) {
    recordDefaultflag = false;
    String[] keys = {faultOccurTime};
    sortrecords = readFaultlog.read(keys, monthnum);
    sortFault.Sort(sortrecords, "getTimeOfFailure", "desc");
    return sortrecords;
  }

  /**
   * retrieve by some keys and sort by time default, in monthnum month;
   * @param keys
   *        retrive keys for user define.
   * @param monthnum
   *        month num.
   * @return sorted records.
   */
  public List<Fault> retrieveWithMoreKeys(String[] keys, int monthnum) {
    recordDefaultflag = false;
    sortrecords = readFaultlog.read(keys, monthnum);
    sortFault.Sort(sortrecords, "getTimeOfFailure", null);
    return sortrecords;
  }

  /**
   * retrieve by some keys and sort by time default
   * @param keys
   *        retrive keys for user define.
   * @return sorted records.
   */
  public List<Fault> retrieveWithMoreKeys(String[] keys) {
    recordDefaultflag = false;
    sortrecords = readFaultlog.read(keys);
    sortFault.Sort(sortrecords, "getTimeOfFailure", null);
    return sortrecords;
  }

  /**
   * get buffered time.
   * @return bufferedTime
   */
  public long getBufferedTime() {
    return bufferedTime;
  }

  /**
   * set the buffered time
   * @param bufferedTime
   *        buffered time to be set.
   */
  public void setBufferedTime(long bufferedTime) {
    this.bufferedTime = bufferedTime;
  }
}
