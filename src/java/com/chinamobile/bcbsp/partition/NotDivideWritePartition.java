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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.pipes.Application;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;

/**
 * NotDivideWritePartition Implements the partition method which don't need to
 * divide.The user must provide a no-argument constructor.
 * @author
 * @version
 */
public class NotDivideWritePartition extends WritePartition {
  /**The log of the WritePartition class.*/
  public static final Log LOG = LogFactory
      .getLog(NotDivideWritePartition.class);
  /**
   * The constructor of the HashWritePartition class.
   */
  public NotDivideWritePartition() {
  }
  /**
   * The constructor of the WritePartition class.
   * @param aWorkerAgent The workerAgent of the staff.
   * @param bspstaff The staff owns the writepartition.
   * @param aPartitioner The partitioner of the WritePartition.
   */
  public NotDivideWritePartition(WorkerAgentForStaffInterface aWorkerAgent,
      BSPStaff  bspstaff, Partitioner<Text> aPartitioner) {
    this.workerAgent = aWorkerAgent;
    this.staff = bspstaff;
    this.partitioner = aPartitioner;
  }
  /**
   * This method is used to partition graph vertexes.
   * @param recordReader The recordreader of the split.
   * @throws IOException The io exception
   * @throws InterruptedException The Interrupted Exception
   */
  public void write(RecordReader recordReader) throws IOException,
      InterruptedException {
    int headNodeNum = 0;
    int local = 0;
    int lost = 0;
    try {
      while (recordReader != null && recordReader.nextKeyValue()) {
        headNodeNum++;
        Text key = new Text(recordReader.getCurrentKey().toString());
        Text value = new Text(recordReader.getCurrentValue().toString());
        Vertex vertex = this.recordParse.recordParse(key.toString(),
            value.toString());
        if (vertex == null) {
          lost++;
          continue;
        }
        staff.getGraphData().addForAll(vertex);
        local++;

      }
      LOG.info("The number of vertices that were read from the input file: "
          + headNodeNum);
      LOG.info("The number of vertices that were put into the partition: "
          + local);
      LOG.info("The number of vertices that were sent to other partitions: "
          + 0);
      LOG.info("The number of verteices in the partition that cound not be "
          + "parsed:" + lost);
    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      throw e;
    }
  }
  /**
   * @param recordReader The recordReader of the staff write.
   * @param application The application of the writepartition.
   * @param staffnum The staff number of the job.
   * @throws IOException the io exception
   * @throws InterruptedException the Interrupted Exception
   */
  public void write(RecordReader recordReader, Application application,
      int staffnum) throws IOException, InterruptedException {
  }
}
