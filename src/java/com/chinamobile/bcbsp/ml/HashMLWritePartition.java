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

package com.chinamobile.bcbsp.ml;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.partition.WritePartition;
import com.chinamobile.bcbsp.pipes.Application;
import com.chinamobile.bcbsp.util.ThreadPool;
import com.chinamobile.bcbsp.util.ThreadSignle;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;

/**
 * HashWritePartition Implements hash-based partition method.The user must
 * provide a no-argument constructor.
 * @author
 * @version
 */
public class HashMLWritePartition extends WritePartition {
  /**The log of the HashWritePartition class.*/
  public static final Log LOG = LogFactory.getLog(HashMLWritePartition.class);
  /**The number of the byte owns the bits.*/
  private static final int CONTAINERNUMBER = 1024;
  /**
   * The constructor of the HashWritePartition class.
   */
  public HashMLWritePartition() {
  }
  /**
   * The constructor of the HashWritePartition class.
   * @param aWorkerAgent The workerAgent of the staff.
   * @param bspstaff The staff owns the writepartition.
   * @param aPartitioner The partitioner of the WritePartition.
   */
  public HashMLWritePartition(WorkerAgentForStaffInterface aWorkerAgent,
      BSPStaff bspstaff, Partitioner<Text> aPartitioner) {
    this.workerAgent = aWorkerAgent;
    this.staff = bspstaff;
    this.partitioner = aPartitioner;
  }
  /**
   * This method is used to partition graph vertexes. Writing Each vertex to the
   * corresponding partition. In this method calls recordParse method to create
   * an HeadNode object. The last call partitioner's getPartitionId method to
   * calculate the HeadNode belongs to partition's id. If the HeadNode belongs
   * local partition then written to the local partition or send it to the
   * appropriate partition.
   * @param recordReader The recordreader of the split.
   * @throws IOException The io exception
   * @throws InterruptedException The Interrupted Exception
   */
  public void write(RecordReader recordReader) throws IOException,
      InterruptedException {
    int headNodeNum = 0;
    int local = 0;
    int send = 0;
    int lost = 0;
    ThreadPool tpool = new ThreadPool(this.sendThreadNum);
    int bufferSize = (int) ((this.TotalCacheSize * CONTAINERNUMBER
        * CONTAINERNUMBER)
        / (this.staff.getStaffNum() + this.sendThreadNum));
    byte[][] buffer = new byte[this.staff.getStaffNum()][bufferSize];
    int[] bufindex = new int[this.staff.getStaffNum()];
    BytesWritable kbytes = new BytesWritable();
    int ksize = 0;
    BytesWritable vbytes = new BytesWritable();
    int vsize = 0;
    DataOutputBuffer bb = new DataOutputBuffer();
    try {
      this.keyserializer.open(bb);
      this.valueserializer.open(bb);
    } catch (IOException e) {
      throw e;
    }
    try {
      while (recordReader != null && recordReader.nextKeyValue()) {
        headNodeNum++;
        Text key = new Text(recordReader.getCurrentKey().toString());
        Text value = new Text(recordReader.getCurrentValue().toString());
        int pid = -1;
        if (key != null) {
          pid = this.partitioner.getPartitionID(key);
        } else {
          lost++;
          continue;
        }
        if (pid == this.staff.getPartition()) {
          local++;
          
          KeyValuePair pair = (KeyValuePair) this.recordParse.recordParse(key.toString(),
              value.toString());
          
          if (pair == null) {
            lost++;
            continue;
          }
          staff.getGraphData().addForAll(pair);
        } else {
          send++;
          bb.reset();
          this.keyserializer.serialize(key);
          kbytes.set(bb.getData(), 0, bb.getLength());
          ksize = kbytes.getLength();
          bb.reset();
          this.valueserializer.serialize(value);
          vbytes.set(bb.getData(), 0, bb.getLength());
          vsize = vbytes.getLength();
          if ((buffer[pid].length - bufindex[pid]) > (ksize + vsize)) {
            System.arraycopy(kbytes.getBytes(), 0, buffer[pid], bufindex[pid],
                ksize);
            bufindex[pid] += ksize;
            System.arraycopy(vbytes.getBytes(), 0, buffer[pid], bufindex[pid],
                vsize);
            bufindex[pid] += vsize;
          } else if (buffer[pid].length < (ksize + vsize)) {
            ThreadSignle t = tpool.getThread();
            while (t == null) {
              t = tpool.getThread();
            }
            t.setWorker(this.workerAgent.getWorker(staff.getJobID(),
                staff.getStaffID(), pid));
            t.setJobId(staff.getJobID());
            t.setTaskId(staff.getStaffID());
            t.setBelongPartition(pid);
            BytesWritable data = new BytesWritable();
            byte[] tmp = new byte[vsize + ksize];
            System.arraycopy(kbytes.getBytes(), 0, tmp, 0, ksize);
            System.arraycopy(vbytes.getBytes(), 0, tmp, ksize, vsize);
            data.set(tmp, 0, (ksize + vsize));
            t.setData(data);
            tmp = null;
            LOG.info("Using Thread is: " + t.getThreadNumber());
            LOG.info("this is a super record");
            t.setStatus(true);
          } else {
            ThreadSignle t = tpool.getThread();
            while (t == null) {
              t = tpool.getThread();
            }
            t.setWorker(this.workerAgent.getWorker(staff.getJobID(),
                staff.getStaffID(), pid));
            t.setJobId(staff.getJobID());
            t.setTaskId(staff.getStaffID());
            t.setBelongPartition(pid);
            BytesWritable data = new BytesWritable();
            data.set(buffer[pid], 0, bufindex[pid]);
            t.setData(data);
            LOG.info("Using Thread is: " + t.getThreadNumber());
            t.setStatus(true);
            bufindex[pid] = 0;
            // store data
            System.arraycopy(kbytes.getBytes(), 0, buffer[pid], bufindex[pid],
                ksize);
            bufindex[pid] += ksize;
            System.arraycopy(vbytes.getBytes(), 0, buffer[pid], bufindex[pid],
                vsize);
            bufindex[pid] += vsize;
          }
        }
      }
      for (int i = 0; i < this.staff.getStaffNum(); i++) {
        if (bufindex[i] != 0) {
          ThreadSignle t = tpool.getThread();
          while (t == null) {
            t = tpool.getThread();
          }
          t.setWorker(this.workerAgent.getWorker(staff.getJobID(),
              staff.getStaffID(), i));
          t.setJobId(staff.getJobID());
          t.setTaskId(staff.getStaffID());
          t.setBelongPartition(i);
          BytesWritable data = new BytesWritable();
          data.set(buffer[i], 0, bufindex[i]);
          t.setData(data);
          LOG.info("Using Thread is: " + t.getThreadNumber());
          t.setStatus(true);
        }
      }
      tpool.cleanup();
      tpool = null;
      buffer = null;
      bufindex = null;
      LOG.info("The number of vertices that were read from the input file: "
          + headNodeNum);
      LOG.info("The number of vertices that were put into the partition: "
          + local);
      LOG.info("The number of vertices that were sent to other partitions: "
          + send);
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
   * @param staffNum The staff number of the job.
   * @throws IOException the io exception
   * @throws InterruptedException the Interrupted Exception
   */
  public void write(RecordReader recordReader, Application application,
      int staffNum) throws IOException, InterruptedException {
    LOG.info("in hashWritePartion write method");
    int headNodeNum = 0;
    int local = 0;
    int send = 0;
    int lost = 0;
    ThreadPool tpool = new ThreadPool(this.sendThreadNum);
    int bufferSize = (int) ((this.TotalCacheSize * CONTAINERNUMBER
        * CONTAINERNUMBER)
        / (this.staff.getStaffNum() + this.sendThreadNum));
    byte[][] buffer = new byte[this.staff.getStaffNum()][bufferSize];
    int[] bufindex = new int[this.staff.getStaffNum()];
    BytesWritable kbytes = new BytesWritable();
    int ksize = 0;
    BytesWritable vbytes = new BytesWritable();
    int vsize = 0;
    DataOutputBuffer bb = new DataOutputBuffer();
    try {
      this.keyserializer.open(bb);
      this.valueserializer.open(bb);
    } catch (IOException e) {
      throw e;
    }
    try {
      while (recordReader != null && recordReader.nextKeyValue()) {
        headNodeNum++;
        Text key = new Text(recordReader.getCurrentKey().toString());
        Text value = new Text(recordReader.getCurrentValue().toString());
        int pid = -1;
        Text vertexID = this.recordParse.getVertexID(key);
        pid = this.partitioner.getPartitionID(vertexID);
        if (pid == -1) {
          lost++;
          continue;
        }
        if (pid == this.staff.getPartition()) {
          local++;
          LOG.info("send to local");
          application.getDownlink().sendKeyValue(key.toString(),
              value.toString());
        } else {
          send++;
          LOG.info("send to remote");
          bb.reset();
          this.keyserializer.serialize(key);
          kbytes.set(bb.getData(), 0, bb.getLength());
          ksize = kbytes.getLength();
          bb.reset();
          this.valueserializer.serialize(value);
          vbytes.set(bb.getData(), 0, bb.getLength());
          vsize = vbytes.getLength();
          if ((buffer[pid].length - bufindex[pid]) > (ksize + vsize)) {
            System.arraycopy(kbytes.getBytes(), 0, buffer[pid], bufindex[pid],
                ksize);
            bufindex[pid] += ksize;
            System.arraycopy(vbytes.getBytes(), 0, buffer[pid], bufindex[pid],
                vsize);
            bufindex[pid] += vsize;
          } else if (buffer[pid].length < (ksize + vsize)) {
            ThreadSignle t = tpool.getThread();
            while (t == null) {
              t = tpool.getThread();
            }
            t.setDataType(1);
            t.setWorker(this.workerAgent.getWorker(staff.getJobID(),
                staff.getStaffID(), pid));
            t.setJobId(staff.getJobID());
            t.setTaskId(staff.getStaffID());
            t.setBelongPartition(pid);
            BytesWritable data = new BytesWritable();
            byte[] tmp = new byte[vsize + ksize];
            System.arraycopy(kbytes.getBytes(), 0, tmp, 0, ksize);
            System.arraycopy(vbytes.getBytes(), 0, tmp, ksize, vsize);
            data.set(tmp, 0, (ksize + vsize));
            t.setData(data);
            tmp = null;
            LOG.info("Using Thread is: " + t.getThreadNumber());
            LOG.info("this is a super record");
            t.setStatus(true);
          } else {
            ThreadSignle t = tpool.getThread();
            while (t == null) {
              t = tpool.getThread();
            }
            t.setDataType(1);
            t.setWorker(this.workerAgent.getWorker(staff.getJobID(),
                staff.getStaffID(), pid));
            t.setJobId(staff.getJobID());
            t.setTaskId(staff.getStaffID());
            t.setBelongPartition(pid);
            BytesWritable data = new BytesWritable();
            data.set(buffer[pid], 0, bufindex[pid]);
            t.setData(data);
            t.setStatus(true);
            bufindex[pid] = 0;
            System.arraycopy(kbytes.getBytes(), 0, buffer[pid], bufindex[pid],
                ksize);
            bufindex[pid] += ksize;
            System.arraycopy(vbytes.getBytes(), 0, buffer[pid], bufindex[pid],
                vsize);
            bufindex[pid] += vsize;
          }
        }
      }
      for (int i = 0; i < this.staff.getStaffNum(); i++) {
        if (bufindex[i] != 0) {
          ThreadSignle t = tpool.getThread();
          while (t == null) {
            t = tpool.getThread();
          }
          t.setWorker(this.workerAgent.getWorker(staff.getJobID(),
              staff.getStaffID(), i));
          t.setJobId(staff.getJobID());
          t.setTaskId(staff.getStaffID());
          t.setDataType(1);
          t.setBelongPartition(i);
          BytesWritable data = new BytesWritable();
          data.set(buffer[i], 0, bufindex[i]);
          t.setData(data);
          t.setStatus(true);
        }
      }
      tpool.cleanup();
      tpool = null;
      buffer = null;
      bufindex = null;
      LOG.info("The number of vertices that were read from the input file: "
          + headNodeNum);
      LOG.info("The number of vertices that were put into the partition: "
          + local);
      LOG.info("The number of vertices that were sent to other partitions: "
          + send);
      LOG.info("The number of verteices in the partition that cound not be "
          + "parsed:" + lost);
    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      throw e;
    }
  }
}
