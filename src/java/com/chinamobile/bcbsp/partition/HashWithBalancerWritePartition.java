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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.pipes.Application;
import com.chinamobile.bcbsp.util.ThreadPool;
import com.chinamobile.bcbsp.util.ThreadSignle;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;

/**
 * HashWithBalancerWritePartition Implements partition method based on hash with
 * balancer.The user must provide a no-argument constructor.
 * @author
 * @version 2.0
 */
public class HashWithBalancerWritePartition extends WritePartition {
  /** The log of the class.*/
  private static final Log LOG = LogFactory
      .getLog(HashWithBalancerWritePartition.class);
  /**The number of the byte owns the bits.*/
  private static final int CONTAINERNUMBER = 1024;
  /**The part of the buffer.*/
  private static final double PART = 0.5;
  /**The times of the buffer.*/
  private static final int TIME = 10;
  /** The counter contains the number of each partition.*/
  private HashMap<Integer, Integer> counter = new HashMap<Integer, Integer>();
  /**
   * The constructor of the class.
   */
  public HashWithBalancerWritePartition() {
  }
  /**
   * The constructor of the class.
   * @param aWorkerAgent The workerAgent of the staff.
   * @param bspstaff The staff owns the writepartition.
   * @param aPartitioner The partitioner of the WritePartition.
   */
  public HashWithBalancerWritePartition(
      WorkerAgentForStaffInterface aWorkerAgent, BSPStaff bspstaff,
      Partitioner<Text> aPartitioner) {
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
    int staffNum = this.staff.getStaffNum();
    BytesWritable kbytes = new BytesWritable();
    int ksize = 0;
    BytesWritable vbytes = new BytesWritable();
    int vsize = 0;
    DataOutputBuffer bb = new DataOutputBuffer();
    int bufferSize = (int) ((this.TotalCacheSize * CONTAINERNUMBER
        * CONTAINERNUMBER) * PART);
    int dataBufferSize = (int) ((this.TotalCacheSize * CONTAINERNUMBER
        * CONTAINERNUMBER)
        / (this.staff.getStaffNum() + this.sendThreadNum));
    byte[] buffer = new byte[bufferSize];
    int bufindex = 0;
    SerializationFactory sFactory = new SerializationFactory(
        new Configuration());
    Serializer<IntWritable> psserializer = sFactory
        .getSerializer(IntWritable.class);
    byte[] pidandsize = new byte[TIME * CONTAINERNUMBER * CONTAINERNUMBER];
    int psindex = 0;
    BytesWritable pidbytes = new BytesWritable();
    int psize = 0;
    BytesWritable sizebytes = new BytesWritable();
    int ssize = 0;
    try {
      this.keyserializer.open(bb);
      this.valueserializer.open(bb);
      psserializer.open(bb);
    } catch (IOException e) {
      throw e;
    }
    String path = "/tmp/bcbsp/" + this.staff.getJobID() + "/"
        + this.staff.getStaffID();
    File dir = new File("/tmp/bcbsp/" + this.staff.getJobID());
    dir.mkdir();
    dir = new File("/tmp/bcbsp/" + this.staff.getJobID() + "/"
        + this.staff.getStaffID());
    dir.mkdir();
    ArrayList<File> files = new ArrayList<File>();
    try {
      File file = new File(path + "/" + "data" + ".txt");
      files.add(file);
      DataOutputStream dataWriter = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(path + "/" + "data"
              + ".txt", true)));
      DataInputStream dataReader = new DataInputStream(new BufferedInputStream(
          new FileInputStream(path + "/" + "data" + ".txt")));
      File filet = new File(path + "/" + "pidandsize" + ".txt");
      files.add(filet);
      DataOutputStream psWriter = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(path + "/"
              + "pidandsize" + ".txt", true)));
      DataInputStream psReader = new DataInputStream(new BufferedInputStream(
          new FileInputStream(path + "/" + "pidandsize" + ".txt")));
      while (recordReader != null && recordReader.nextKeyValue()) {
        headNodeNum++;
        Text key = new Text(recordReader.getCurrentKey().toString());
        Text value = new Text(recordReader.getCurrentValue().toString());
        int pid = -1;
        Text vertexID = this.recordParse.getVertexID(key);
        if (vertexID != null) {
          pid = this.partitioner.getPartitionID(vertexID);
        } else {
          lost++;
          continue;
        }
        if (this.counter.containsKey(pid)) {
          this.counter.put(pid, (this.counter.get(pid) + 1));
        } else {
          this.counter.put(pid, 1);
        }
        bb.reset();
        this.keyserializer.serialize(key);
        kbytes.set(bb.getData(), 0, bb.getLength());
        ksize = kbytes.getLength();
        bb.reset();
        this.valueserializer.serialize(value);
        vbytes.set(bb.getData(), 0, bb.getLength());
        vsize = vbytes.getLength();
        bb.reset();
        psserializer.serialize(new IntWritable(ksize + vsize));
        sizebytes.set(bb.getData(), 0, bb.getLength());
        ssize = sizebytes.getLength();
        bb.reset();
        psserializer.serialize(new IntWritable(pid));
        pidbytes.set(bb.getData(), 0, bb.getLength());
        psize = pidbytes.getLength();
        if ((pidandsize.length - psindex) > (ssize + psize)) {
          System.arraycopy(sizebytes.getBytes(), 0, pidandsize, psindex, ssize);
          psindex += ssize;
          System.arraycopy(pidbytes.getBytes(), 0, pidandsize, psindex, psize);
          psindex += psize;
        } else {
          psWriter.write(pidandsize, 0, psindex);
          psindex = 0;
          System.arraycopy(sizebytes.getBytes(), 0, pidandsize, psindex, ssize);
          psindex += ssize;
          System.arraycopy(pidbytes.getBytes(), 0, pidandsize, psindex, psize);
          psindex += psize;
        }
        if ((buffer.length - bufindex) > (ksize + vsize)) {
          System.arraycopy(kbytes.getBytes(), 0, buffer, bufindex, ksize);
          bufindex += ksize;
          System.arraycopy(vbytes.getBytes(), 0, buffer, bufindex, vsize);
          bufindex += vsize;
        } else if (buffer.length < (ksize + vsize)) {
          dataWriter.write(buffer, 0, bufindex);
          bufindex = 0;
          LOG.info("This is a super record");
          dataWriter.write(kbytes.getBytes(), 0, ksize);
          dataWriter.write(vbytes.getBytes(), 0, vsize);
        } else {
          dataWriter.write(buffer, 0, bufindex);
          bufindex = 0;
          System.arraycopy(kbytes.getBytes(), 0, buffer, bufindex, ksize);
          bufindex += ksize;
          System.arraycopy(vbytes.getBytes(), 0, buffer, bufindex, vsize);
          bufindex += vsize;
        }
      }
      if (psindex != 0) {
        psWriter.write(pidandsize, 0, psindex);
      }
      if (bufindex != 0) {
        dataWriter.write(buffer, 0, bufindex);
        bufindex = 0;
      }
      dataWriter.close();
      dataWriter = null;
      psWriter.close();
      psWriter = null;
      buffer = null;
      pidandsize = null;
      this.ssrc.setDirFlag(new String[] {"3"});
      this.ssrc.setCounter(this.counter);
      HashMap<Integer, Integer> hashBucketToPartition = this.sssc
          .loadDataInBalancerBarrier(ssrc, Constants.PARTITION_TYPE.HASH);
      this.staff.setHashBucketToPartition(hashBucketToPartition);
      byte[][] databuf = new byte[staffNum][dataBufferSize];
      int[] databufindex = new int[staffNum];
      try {
        IntWritable pid = new IntWritable();
        IntWritable size = new IntWritable();
        int belongPid = 0;
        while (true) {
          size.readFields(psReader);
          pid.readFields(psReader);
          belongPid = hashBucketToPartition.get(pid.get());
          if (belongPid != this.staff.getPartition()) {
            send++;
          } else {
            local++;
            }
          if ((databuf[belongPid].length - databufindex[belongPid]) > size
              .get()) {
            dataReader.read(databuf[belongPid], databufindex[belongPid],
                size.get());
            databufindex[belongPid] += size.get();
          } else if (databuf[belongPid].length < size.get()) {
            LOG.info("This is a super record");
            byte[] tmp = new byte[size.get()];
            dataReader.read(tmp, 0, size.get());
            if (belongPid == this.staff.getPartition()) {
              DataInputStream reader = new DataInputStream(
                  new BufferedInputStream(new ByteArrayInputStream(tmp)));
              try {
                boolean stop = true;
                while (stop) {
                  Text key = new Text();
                  key.readFields(reader);
                  Text value = new Text();
                  value.readFields(reader);
                  if (key.getLength() > 0 && value.getLength() > 0) {
                    Vertex vertex = this.recordParse.recordParse(
                        key.toString(), value.toString());
                    if (vertex == null) {
                      lost++;
                      continue;
                    }
                    this.staff.getGraphData().addForAll(vertex);
                  } else {
                    stop = false;
                  }
                }
              } catch (IOException e) {
                LOG.info("IO exception: " + e.getStackTrace());
              }
            } else {
              ThreadSignle t = tpool.getThread();
              while (t == null) {
                t = tpool.getThread();
              }
              t.setWorker(this.workerAgent.getWorker(staff.getJobID(),
                  staff.getStaffID(), belongPid));
              t.setJobId(staff.getJobID());
              t.setTaskId(staff.getStaffID());
              t.setBelongPartition(belongPid);
              BytesWritable data = new BytesWritable();
              data.set(tmp, 0, size.get());
              t.setData(data);
              LOG.info("Using Thread is: " + t.getThreadNumber());
              t.setStatus(true);
            }
            tmp = null;
          } else {
            if (belongPid == this.staff.getPartition()) {
              DataInputStream reader = new DataInputStream(
                  new BufferedInputStream(new ByteArrayInputStream(
                      databuf[belongPid], 0, databufindex[belongPid])));
              try {
                boolean stop = true;
                while (stop) {
                  Text key = new Text();
                  key.readFields(reader);
                  Text value = new Text();
                  value.readFields(reader);
                  if (key.getLength() > 0 && value.getLength() > 0) {
                    Vertex vertex = this.recordParse.recordParse(
                        key.toString(), value.toString());
                    if (vertex == null) {
                      lost++;
                      continue;
                    }
                    this.staff.getGraphData().addForAll(vertex);
                  } else {
                    stop = false;
                  }
                }
              } catch (IOException e) {
                LOG.info("IO exception: " + e.getStackTrace());
              }
            } else {
              ThreadSignle t = tpool.getThread();
              while (t == null) {
                t = tpool.getThread();
              }
              t.setWorker(this.workerAgent.getWorker(staff.getJobID(),
                  staff.getStaffID(), belongPid));
              t.setJobId(staff.getJobID());
              t.setTaskId(staff.getStaffID());
              t.setBelongPartition(belongPid);
              BytesWritable data = new BytesWritable();
              data.set(databuf[belongPid], 0, databufindex[belongPid]);
              t.setData(data);
              LOG.info("Using Thread is: " + t.getThreadNumber());
              t.setStatus(true);
            }
            databufindex[belongPid] = 0;
            dataReader.read(databuf[belongPid], databufindex[belongPid],
                size.get());
            databufindex[belongPid] += size.get();
          }
        }
      } catch (EOFException ex) {
        LOG.error("[write]", ex);
      }
      for (int i = 0; i < staffNum; i++) {
        if (databufindex[i] != 0) {
          if (i == this.staff.getPartition()) {
            DataInputStream reader = new DataInputStream(
                new BufferedInputStream(new ByteArrayInputStream(databuf[i], 0,
                    databufindex[i])));
            try {
              boolean stop = true;
              while (stop) {
                Text key = new Text();
                key.readFields(reader);
                Text value = new Text();
                value.readFields(reader);
                if (key.getLength() > 0 && value.getLength() > 0) {
                  Vertex vertex = this.recordParse.recordParse(key.toString(),
                      value.toString());
                  if (vertex == null) {
                    lost++;
                    continue;
                  }
                  this.staff.getGraphData().addForAll(vertex);
                } else {
                  stop = false;
                }
              }
            } catch (IOException e) {
              LOG.info("IO exception: " + e.getStackTrace());
            }
          } else {
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
            data.set(databuf[i], 0, databufindex[i]);
            t.setData(data);
            LOG.info("Using Thread is: " + t.getThreadNumber());
            t.setStatus(true);
          }
        }
      }
      dataReader.close();
      dataReader = null;
      psReader.close();
      psReader = null;
      for (File f : files) {
        f.delete();
      }
      dir.delete();
      dir = new File(path.substring(0, path.lastIndexOf('/')));
      dir.delete();
      tpool.cleanup();
      tpool = null;
      databuf = null;
      databufindex = null;
      this.counter = null;
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
    } finally {
      for (File f : files) {
        f.delete();
      }
      dir.delete();
      dir = new File(path.substring(0, path.lastIndexOf('/')));
      dir.delete();
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
