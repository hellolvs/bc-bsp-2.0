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

package com.chinamobile.bcbsp.partition;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.pipes.Application;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;

/**
 * RangeWritePartition Implements hash-based partition method.The user must
 * provide a no-argument constructor.
 * @author
 * @version
 */
public class RangeWritePartition extends WritePartition {
  /** The log of the class.*/
  public static final Log LOG = LogFactory.getLog(RangeWritePartition.class);
  /** The counter contains the Maxnumbervertex of each partition.*/
  private HashMap<Integer, Integer> counter = new HashMap<Integer, Integer>();
  /**
   * The constructor of the class.
   */
  public RangeWritePartition() {
  }
  /**
   * The constructor of the class.
   * @param aWorkerAgent The workerAgent of the staff.
   * @param bspstaff The staff owns the writepartition.
   * @param aPartitioner The partitioner of the WritePartition.
   */
  public RangeWritePartition(
      WorkerAgentForStaffInterface aWorkerAgent, BSPStaff bspstaff,
      Partitioner<Text> aPartitioner) {
    this.workerAgent = aWorkerAgent;
    this.staff = bspstaff;
    this.partitioner = aPartitioner;
  }
  /**
   * This method is used to partition graph vertexes. Every vertex in the
   * split is partitioned to the local staff.
   * @param recordReader The recordreader of the split.
   * @throws IOException The io exception
   * @throws InterruptedException The Interrupted Exception
   */
  public void write(RecordReader recordReader) throws IOException,
      InterruptedException {
    int headNodeNum = 0;
    int local = 0;
    int lost = 0;
    int partitionid = this.staff.getPartition();
    int maxid = Integer.MIN_VALUE;
    try {
      while (recordReader != null && recordReader.nextKeyValue()) {
        headNodeNum++;
        Text key = new Text(recordReader.getCurrentKey().toString());
        Text value = new Text(recordReader.getCurrentValue().toString());
        Text vertexID = this.recordParse.getVertexID(key);
        if (vertexID != null) {
          local++;
          int vertexid = Integer.parseInt(vertexID.toString());
          if (vertexid > maxid) {
            maxid = vertexid;
          }
          Vertex vertex = this.recordParse.recordParse(key.toString(),
              value.toString());
          this.staff.getGraphData().addForAll(vertex);
        } else {
          lost++;
          continue;
        }
      }
      if (lost == 0) {
        counter.put(maxid, partitionid);
        this.ssrc.setDirFlag(new String[] {"3"});
        this.ssrc.setCounter(counter);
        HashMap<Integer, Integer> rangerouter = this.sssc.rangerouter(ssrc);
        this.staff.setRangeRouter(rangerouter);
      }
      LOG.info("The number of vertices that were read from the input file: "
          + headNodeNum);
      LOG.info("The number of vertices that were put into the partition: "
          + local);
      LOG.info("The number of verteices in the partition that cound not be"
          + " parsed:" + lost);
    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      throw e;
    }
  }
  /**
   * @param recordReader
   *        The recordReader of the staff write.
   * @param application
   *        The application of the writepartition.
   * @param staffnum
   *        The staff number of the job.
   * @throws IOException
   *         the io exception
   * @throws InterruptedException
   *         the Interrupted Exception
   */
  public void write(RecordReader recordReader, Application application,
      int staffnum) throws IOException, InterruptedException {
  }
}
