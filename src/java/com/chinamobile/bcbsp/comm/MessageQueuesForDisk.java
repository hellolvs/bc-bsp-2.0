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

package com.chinamobile.bcbsp.comm;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.ObjectSizer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * MessageQueuesForDisk Message queues manager for disk supported.
 */
public class MessageQueuesForDisk implements MessageQueuesInterface {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(MessageQueuesForDisk.class);
  /** default size of message. */
  private static final int MESSAGE_SIZE = 150;
  /** Converter byte to MByte. */
  private static final long MB_SIZE = 1048576;
  /** Converter byte to KByte. */
  private static final long KB_SIZE = 1000;
  /** time change. */
  private static final float TIME_CHANGE = 1000f;
  /** For time accumulation. */
  private long writeDiskTime = 0;
  /** Clock time. */
  private long readDiskTime = 0;
  
  /** The meta data for a bucket. */
  class BucketMeta {
    /** Is on disk flag. */
    public boolean onDiskFlag;
    /** The length of the bucket by Bytes. */
    public long length;
    /** The length of the part of the bucket still in memory by Bytes. */
    public long lengthInMemory;
    /** Number of messages in the bucket. */
    public int count;
    /** Number of messages of the part of the bucket still in memory. */
    public int countInMemory;
    /** The hash map of queues indexed by dstVertexID. */
    public ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> queueMap;
  }
  
  /**
   * The beta parameter for the proportion of data memory for the graph data,
   * 1-beta for messages data
   */
  private float beta;
  /**
   * The parameter for the percentage of the heap memory for the data memory
   * (graph & messages)
   */
  private float dataPercent;
  /** Hash bucket number. */
  private int hashBucketNumber;
  /** The total space for messages data(Bytes). */
  private long sizeOfMessagesSpace;
  /** The current size of messages data in memory(Bytes). */
  private long sizeOfMessagesDataInMem;
  /** The current count of messages data in memory. */
  private long countOfMessagesDataInMem;
  /** The size of hash maps structures in memory(Bytes). */
  private long sizeOfHashMapsInMem;
  /** The threshold size for messages data(Bytes). */
  private long sizeThreshold;
  /** The threshold count number for messages data. */
  private long countThreshold;
  /** The threshold count number for messages in an incoming bucket. */
  private long countThresholdForBucket;
  /** Accumulate the size fo messages totally.(Bytes). */
  private long totalSizeOfMessages;
  /** The total count number of messages. */
  private long totalCount;
  /** Size of IMessage type instance.(Bytes). */
  private int sizeOfMessage;
  /** Size of empty ConcurrentLinkedQueue<IMessage> object. */
  private int sizeOfEmptyMessageQueue;
  /** Size of a reference. */
  private final int sizeOfRef;
  /** Size of an Integer. */
  private final int sizeOfInteger;
  /** Size of a char. */
  private final int sizeOfChar;
  /** Object sizer. */
  private final ObjectSizer sizer;
  /** BSP Job ID. */
  private BSPJobID jobID;
  /** the partition ID. */
  private int partitionID;
  /** root file of message on disk. */
  private File fileRoot;
  /** message file on disk. */
  private File messagesDataFile;
  /**
   * Outgoing Message Queues hash indexed by <WorkerManagerName:Port>.
   */
  @SuppressWarnings("rawtypes")
  private ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues;
  /**
   * Hash buckets of hash map of Incoming Message Queues. Hash indexed by
   * vertexID(int).
   */
  private ArrayList<BucketMeta> incomingQueues;
  /**
   * Hash buckets of hash map of Incomed Message Queues. Hash indexed by
   * vertexID(int).
   */
  private ArrayList<BucketMeta> incomedQueues;
  /** Index of the current accessed bucket of incomedQueues. */
  private volatile int currentBucket;
  /** incomed message file lock. */
  private Lock[] incomedFileLocks = null;
  /** incoming message file lock. */
  private Lock[] incomingFileLocks = null;
  /** counter of next outgoing queue. */
  private int nextOutgoingQueueCount = 1;
  
  /**
   * Constructor.
   * @param job
   * @param partitionID
   */
  public MessageQueuesForDisk(BSPJob job, int partitionID) {
    LOG.info("========== Initializing Message Queues Data For Disk ==========");
    this.dataPercent = job.getMemoryDataPercent(); // Default 0.8
    this.jobID = job.getJobID();
    this.partitionID = partitionID;
    // ratio of graph data
    this.beta = job.getBeta();
    this.hashBucketNumber = job.getHashBucketNumber();
    LOG.info("[beta] = " + this.beta);
    LOG.info("[hashBucketNumber] = " + this.hashBucketNumber);
    // the structure of three kinds message queues
    this.outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
    this.incomingQueues = new ArrayList<BucketMeta>(this.hashBucketNumber);
    this.incomedQueues = new ArrayList<BucketMeta>(this.hashBucketNumber);
    for (int i = 0; i < this.hashBucketNumber; i++) {
      BucketMeta meta = new BucketMeta();
      meta.onDiskFlag = false;
      meta.length = 0;
      meta.lengthInMemory = 0;
      meta.count = 0;
      meta.countInMemory = 0;
      meta.queueMap = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
      this.incomingQueues.add(meta);
      meta = new BucketMeta();
      meta.onDiskFlag = false;
      meta.length = 0;
      meta.lengthInMemory = 0;
      meta.count = 0;
      meta.countInMemory = 0;
      meta.queueMap = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
      this.incomedQueues.add(meta);
    }
    // Initialize the bucket file locks
    this.incomedFileLocks = new ReentrantLock[this.hashBucketNumber];
    this.incomingFileLocks = new ReentrantLock[this.hashBucketNumber];
    for (int i = 0; i < this.hashBucketNumber; i++) {
      this.incomedFileLocks[i] = new ReentrantLock();
      this.incomingFileLocks[i] = new ReentrantLock();
    }
    // Initialize the size of objects.
    BSPConfiguration conf = new BSPConfiguration();
    if (conf.getInt(Constants.BC_BSP_JVM_VERSION, 32) == 64) {
      sizer = ObjectSizer.forSun64BitsVM();
    } else {
      sizer = ObjectSizer.forSun32BitsVM();
    }
    this.sizeOfRef = sizer.sizeOfRef();
    this.sizeOfInteger = sizer.sizeOf(new Integer(0));
    this.sizeOfChar = sizer.sizeOfChar();
    this.sizeOfEmptyMessageQueue = sizer
        .sizeOf(new ConcurrentLinkedQueue<IMessage>());
    // Size will be evaluted later based on first m received messages.
    this.sizeOfMessage = MESSAGE_SIZE; // Default
    // Get the memory mxBean.
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    // Get the heap memory usage.
    MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
    long maxHeapSize = memoryUsage.getMax();
    LOG.info("[JVM max Heap size] = " + maxHeapSize / MB_SIZE + "MB");
    this.sizeOfMessagesSpace = (long) (maxHeapSize * dataPercent * (1.0f - beta));
    this.sizeThreshold = (long) (sizeOfMessagesSpace);
    this.countThreshold = (long) (sizeThreshold / sizeOfMessage);
    this.countThresholdForBucket = this.countThreshold
        / (3 * this.hashBucketNumber);
    LOG.info("[size of Messages Space Threshold] = " + this.sizeThreshold
        / MB_SIZE + "MB");
    LOG.info("[count of Messages In Memory Threshold] = " + this.countThreshold
        / KB_SIZE + "K");
    this.sizeOfMessagesDataInMem = 0;
    this.countOfMessagesDataInMem = 0;
    this.totalSizeOfMessages = 0;
    this.totalCount = 0;
    this.sizeOfHashMapsInMem = 0;
    this.fileRoot = new File("/tmp/bcbsp/" + this.jobID.toString() + "/"
        + "partition-" + this.partitionID);
    this.messagesDataFile = new File(this.fileRoot + "/" + "MessagesData");
    // If the root dir does not exit, create it.
    if (!this.fileRoot.exists()) {
      this.fileRoot.mkdirs();
    }
    // If the messages data dir does not exit, create it.
    if (!this.messagesDataFile.exists()) {
      this.messagesDataFile.mkdir();
    }
    // Initialize the current accessed bucket index.
    this.currentBucket = -1;
    LOG.info("===============================================================");
  }
  
  @Override
  public void clearAllQueues() {
    clearIncomedQueues();
    clearIncomingQueues();
    clearOutgoingQueues();
    this.sizeOfMessagesDataInMem = 0;
    this.countOfMessagesDataInMem = 0;
    this.totalSizeOfMessages = 0;
    this.totalCount = 0;
    this.sizeOfHashMapsInMem = 0;
    this.currentBucket = -1;
    try {
      deleteFile(this.messagesDataFile.toString());
    } catch (IOException e) {
      throw new RuntimeException("[File] Delete file:" + this.messagesDataFile
          + " failed!", e);
    }
  }
  
  @Override
  public void clearIncomedQueues() {
    File messagesDataFilebucket;
    for (int i = 0; i < this.hashBucketNumber; i++) {
      messagesDataFilebucket = new File(this.messagesDataFile + "/" + "incomed"
          + "/" + "bucket-" + i);
      if (messagesDataFilebucket.exists()) {
        if (!messagesDataFilebucket.delete()) {
          LOG.warn("[File] Delete file:" + messagesDataFilebucket + " fail!");
        }
      }
      BucketMeta meta = this.incomedQueues.get(i);
      this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem
          - meta.lengthInMemory;
      this.countOfMessagesDataInMem = this.countOfMessagesDataInMem
          - meta.countInMemory;
      this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
          - (meta.queueMap.size() * (sizeOfRef + sizeOfInteger + sizeOfEmptyMessageQueue));
      meta.onDiskFlag = false;
      meta.length = 0;
      meta.lengthInMemory = 0;
      meta.count = 0;
      meta.countInMemory = 0;
      meta.queueMap.clear();
    }
    messagesDataFilebucket = new File(this.messagesDataFile + "/" + "incomed");
    if (messagesDataFilebucket.exists()) {
      if (!messagesDataFilebucket.delete()) {
        LOG.warn("[File] Delete directory:" + messagesDataFilebucket
            + " failed!");
      }
    }
  }
  
  @Override
  public void clearIncomingQueues() {
    File messagesDataFile_bucket;
    for (int i = 0; i < this.hashBucketNumber; i++) {
      messagesDataFile_bucket = new File(this.messagesDataFile + "/"
          + "incoming" + "/" + "bucket-" + i);
      if (messagesDataFile_bucket.exists()) {
        if (!messagesDataFile_bucket.delete()) {
          LOG.warn("[File] Delete file:" + messagesDataFile_bucket + " fail!");
        }
      }
      BucketMeta meta = this.incomingQueues.get(i);
      this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem
          - meta.lengthInMemory;
      this.countOfMessagesDataInMem = this.countOfMessagesDataInMem
          - meta.countInMemory;
      this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
          - (meta.queueMap.size() * (sizeOfRef + sizeOfInteger + sizeOfEmptyMessageQueue));
      meta.onDiskFlag = false;
      meta.length = 0;
      meta.lengthInMemory = 0;
      meta.count = 0;
      meta.countInMemory = 0;
      meta.queueMap.clear();
    }
    messagesDataFile_bucket = new File(this.messagesDataFile + "/" + "incoming");
    if (messagesDataFile_bucket.exists()) {
      if (!messagesDataFile_bucket.delete()) {
        LOG.warn("[File] Delete directory:" + messagesDataFile_bucket
            + " failed!");
      }
    }
  }
  
  @Override
  public void clearOutgoingQueues() {
    Entry<String, ConcurrentLinkedQueue<IMessage>> entry = null;
    Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it = this.outgoingQueues
        .entrySet().iterator();
    ConcurrentLinkedQueue<IMessage> tmpQueue = null;
    int tmpCount = 0;
    while (it.hasNext()) {
      entry = it.next();
      tmpQueue = entry.getValue();
      tmpCount = tmpQueue.size();
      this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem
          - (tmpCount * this.sizeOfMessage);
      this.countOfMessagesDataInMem = this.countOfMessagesDataInMem - tmpCount;
      this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
          - (sizeOfRef * 2 + (entry.getKey().length() * sizeOfChar) + sizeOfEmptyMessageQueue);
    }
    this.outgoingQueues.clear();
  }
  
  /**
   * Delete all files in the filepath and the filepath.
   * @param filepath
   * @throws IOException
   */
  private void deleteFile(String filepath) throws IOException {
    File f = new File(filepath);
    if (f.exists() && f.isDirectory()) {
      if (f.listFiles().length == 0) {
        f.delete();
      } else {
        File[] delFile = f.listFiles();
        int i = f.listFiles().length;
        for (int j = 0; j < i; j++) {
          if (delFile[j].isDirectory()) {
            deleteFile(delFile[j].getAbsolutePath());
          }
          delFile[j].delete();
        }
      }
    }
  }
  
  @Override
  public void exchangeIncomeQueues() {
    LOG.info("[==>Clock<==] <MessageQueues: save bucket> totally used "
        + this.writeDiskTime / TIME_CHANGE + " seconds");
    /* Clock */
    LOG.info("[==>Clock<==] <MessageQueues: load bucket> totally used "
        + this.readDiskTime / TIME_CHANGE + " seconds");
    /* Clock */
    LOG.info("[==>Clock<==] <MessageQueues: Disk I/O> totally used "
        + (this.writeDiskTime + this.readDiskTime) / TIME_CHANGE + " seconds");
    /* Clock */
    this.writeDiskTime = 0;
    /* Clock */
    this.readDiskTime = 0;
    
    /* Clock */
    this.tidyIncomingQueues();
    clearIncomedQueues();
    // Rename the incoming path to incomed path.
    File incomingPath = new File(this.messagesDataFile + "/" + "incoming");
    if (incomingPath.exists()) {
      if (!incomingPath.renameTo(new File(this.messagesDataFile + "/"
          + "incomed"))) {
        LOG.warn("[MessageQueuesForDisk]:<exchangeIncomeQueues> !"
            + " Rename the incoming path to incomed path failed!");
      }
    }
    ArrayList<BucketMeta> temp = this.incomedQueues;
    this.incomedQueues = this.incomingQueues;
    this.incomingQueues = temp;
//    this.showHashBucketsInfo();
    // Reset the accumulative count and size of messages.
    this.totalCount = 0;
    this.totalSizeOfMessages = 0;
    LOG.info("[MessageQueuesForDisk: exchangeIncomeQueues] "
        + "After exchange, incomed size = " + getIncomedQueuesSize()
        + ", incoming size = " + getIncomingQueuesSize() + ".");
  }
  
  /**
   * Tidy the incoming queues To save the whole bucket onto disk if it has any
   * part on disk.
   */
  private void tidyIncomingQueues() {
    for (int i = 0; i < this.hashBucketNumber; i++) {
      BucketMeta meta = this.incomingQueues.get(i);
      if (meta.countInMemory < meta.count) {
        try {
          saveBucket(this.incomingQueues, i, "incoming");
        } catch (IOException e) {
          throw new RuntimeException(
              "[MessageQueuesForDisk:tidyIncomingQueues]", e);
        }
      }
    }
  }
  
  @Override
  public int getIncomedQueuesSize() {
    int size = 0;
    for (int i = 0; i < this.hashBucketNumber; i++) {
      size = size + this.incomedQueues.get(i).count;
    }
    return size;
  }
  
  private int getIncomedQueuesSizeInMem() {
    int size = 0;
    for (int i = 0; i < this.hashBucketNumber; i++) {
      size = size + this.incomedQueues.get(i).countInMemory;
    }
    return size;
  }
  
  @Override
  public int getIncomingQueuesSize() {
    int size = 0;
    for (int i = 0; i < this.hashBucketNumber; i++) {
      size = size + this.incomingQueues.get(i).count;
    }
    return size;
  }
  
  private int getIncomingQueuesSizeInMem() {
    int size = 0;
    for (int i = 0; i < this.hashBucketNumber; i++) {
      size = size + this.incomingQueues.get(i).countInMemory;
    }
    return size;
  }
  
  /**
   * Current strategy is first find the max bucket in memory, and second get the
   * max queue in the bucket.
   */
  @Override
  public String getMaxIncomingQueueIndex() {
    int maxSize = 0;
    int maxBucket = 0;
    // Find the max buckets in memory.
    for (int i = 0; i < this.hashBucketNumber; i++) {
      BucketMeta meta = this.incomingQueues.get(i);
      if (meta.count > maxSize && !meta.onDiskFlag) {
        maxSize = meta.count;
        maxBucket = i;
      }
    }
    // Find the max queue in the maxBucket.
    String maxQueueIndex = null;
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> queueMap = this.incomingQueues
        .get(maxBucket).queueMap;
    Entry<String, ConcurrentLinkedQueue<IMessage>> entry = null;
    Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it = queueMap
        .entrySet().iterator();
    maxSize = 0;
    while (it.hasNext()) {
      entry = it.next();
      if (entry.getValue().size() > maxSize) {
        maxSize = entry.getValue().size();
        maxQueueIndex = entry.getKey();
      }
    }
    return maxQueueIndex;
  }
  
  /**
   * Current strategy is get the max outgoing queue in memory.
   */
  @Override
  public String getMaxOutgoingQueueIndex() {
    String maxIndex = null;
    long maxSize = 0;
    synchronized (this.outgoingQueues) {
      Entry<String, ConcurrentLinkedQueue<IMessage>> entry = null;
      Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it = this.outgoingQueues
          .entrySet().iterator();
      ConcurrentLinkedQueue<IMessage> tmpQueue = null;
      while (it.hasNext()) {
        entry = it.next();
        tmpQueue = entry.getValue();
        if (tmpQueue.size() > maxSize) {
          maxSize = tmpQueue.size();
          maxIndex = entry.getKey();
        }
      }
    }
    return maxIndex;
  }
  
  @Override
  public int getOutgoingQueuesSize() {
    int size = 0;
    Entry<String, ConcurrentLinkedQueue<IMessage>> entry = null;
    Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it = this.outgoingQueues
        .entrySet().iterator();
    ConcurrentLinkedQueue<IMessage> tmpQueue = null;
    while (it.hasNext()) {
      entry = it.next();
      tmpQueue = entry.getValue();
      size = size + tmpQueue.size();
    }
    return size;
  }
  
  @Override
  public void incomeAMessage(String dstVertexID, IMessage msg) {
    // Evaluate the length of the msg, 16 means 1 long and 2 int.
    int length = sizer.sizeOf(msg) + this.sizeOfRef;
    // Accumulate the total size of messages.
    this.totalSizeOfMessages = this.totalSizeOfMessages + length;
    this.totalCount = this.totalCount + 1;
    this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem + length;
    // Message number in memory
    this.countOfMessagesDataInMem = this.countOfMessagesDataInMem + 1;
    // Get the hash bucket index.
    int hashCode = dstVertexID.hashCode();
    int hashIndex = hashCode % this.hashBucketNumber; // bucket index
    hashIndex = (hashIndex < 0 ? hashIndex + this.hashBucketNumber : hashIndex);
    // Update the bucket meta data.
    BucketMeta meta = this.incomingQueues.get(hashIndex);
    meta.count = meta.count + 1;
    meta.countInMemory = meta.countInMemory + 1;
    meta.length = meta.length + length;
    meta.lengthInMemory = meta.lengthInMemory + length;
    // Add the msg into the incoming queue for the dstVertexID.
    ConcurrentLinkedQueue<IMessage> incomingQueue = meta.queueMap
        .get(dstVertexID);
    if (incomingQueue == null) {
      incomingQueue = new ConcurrentLinkedQueue<IMessage>();
      this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
          + (sizeOfRef * 2 + (dstVertexID.length() * sizeOfChar) + sizeOfEmptyMessageQueue);
    }
    incomingQueue.add(msg);
    meta.queueMap.put(dstVertexID, incomingQueue);
    // On a new message added.
    onMessageIncomed();
  }
  
  @Override
  public void outgoAMessage(String outgoingIndex, IMessage msg) {
    // Evaluate the length of the msg, 16 means 1 long and 2 int.
    int length = sizer.sizeOf(msg) + this.sizeOfRef;
    // Accumulate the total size of messages.
    this.totalSizeOfMessages = this.totalSizeOfMessages + length;
    this.totalCount = this.totalCount + 1;
    this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem + length;
    // Message number in memory
    this.countOfMessagesDataInMem = this.countOfMessagesDataInMem + 1;
    ConcurrentLinkedQueue<IMessage> queue = this.outgoingQueues
        .get(outgoingIndex);
    if (queue == null) {
      queue = new ConcurrentLinkedQueue<IMessage>();
      this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
          + (sizeOfRef * 2 + (outgoingIndex.length() * sizeOfChar) + sizeOfEmptyMessageQueue);
    }
    queue.add(msg);
    this.outgoingQueues.put(outgoingIndex, queue);
    // On a new message outgoed.
    onMessageOutgoed();
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public ConcurrentLinkedQueue<IMessage> removeIncomedQueue(String dstVertID) {
    ConcurrentLinkedQueue<IMessage> incomedQueue = null;
    // Get the hash bucket index.
    int hashCode = dstVertID.hashCode();
    int hashIndex = hashCode % this.hashBucketNumber; // bucket index
    hashIndex = (hashIndex < 0 ? hashIndex + this.hashBucketNumber : hashIndex);
    BucketMeta meta = this.incomedQueues.get(hashIndex);
    // The bucket is on disk.
    if (meta.onDiskFlag) {
      this.incomedFileLocks[hashIndex].lock();
      /** Lock */
      try {
        loadBucket(this.incomedQueues, hashIndex, "incomed");
      } catch (IOException e) {
        LOG.info("==> bucket-" + hashIndex + ", VertexID = " + dstVertID);
        LOG.info("size = " + meta.queueMap.get(dstVertID).size());
        throw new RuntimeException("==> bucket-" + hashIndex + ", VertexID = "
            + dstVertID, e);
      } finally {
        this.incomedFileLocks[hashIndex].unlock();
        /** Unlock */
      }
    }
    meta = this.incomedQueues.get(hashIndex);
    this.currentBucket = hashIndex;
    incomedQueue = meta.queueMap.remove(dstVertID);
    if (incomedQueue == null) {
      incomedQueue = new ConcurrentLinkedQueue<IMessage>();
    }
    int removedCount = incomedQueue.size();
    long removedLength = removedCount * this.sizeOfMessage;
    // Update the meta data.
    meta.count = meta.count - removedCount;
    meta.countInMemory = meta.countInMemory - removedCount;
    meta.length = meta.length - removedLength;
    meta.lengthInMemory = meta.lengthInMemory - removedLength;
    this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem - removedLength;
    this.countOfMessagesDataInMem = this.countOfMessagesDataInMem
        - removedCount;
    this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
        - (sizeOfRef * 2 + (dstVertID.length() * sizeOfChar) + sizeOfEmptyMessageQueue);
    return incomedQueue;
  }
  
  @Override
  public ConcurrentLinkedQueue<IMessage> removeIncomingQueue(String dstVerID) {
    ConcurrentLinkedQueue<IMessage> incomingQueue = null;
    // Get the hash bucket index.
    int hashCode = dstVerID.hashCode();
    int hashIndex = hashCode % this.hashBucketNumber; // bucket index
    hashIndex = (hashIndex < 0 ? hashIndex + this.hashBucketNumber : hashIndex);
    BucketMeta meta = this.incomingQueues.get(hashIndex);
    // The bucket is on disk.
    if (meta.onDiskFlag) {
      this.incomingFileLocks[hashIndex].lock();
      /** Lock */
      try {
        loadBucket(this.incomingQueues, hashIndex, "incoming");
      } catch (IOException e) {
        throw new RuntimeException("[MessageQueuesForDisk:removeIncomingQueue]", e);
      } finally {
        this.incomingFileLocks[hashIndex].unlock();
        /** Unlock */
      }
    }
    meta = this.incomingQueues.get(hashIndex);
    incomingQueue = meta.queueMap.remove(dstVerID);
    if (incomingQueue == null) {
      incomingQueue = new ConcurrentLinkedQueue<IMessage>();
    }
    int removedCount = incomingQueue.size();
    long removedLength = removedCount * this.sizeOfMessage;
    // Update the meta data.
    meta.count = meta.count - removedCount;
    meta.countInMemory = meta.countInMemory - removedCount;
    meta.length = meta.length - removedLength;
    meta.lengthInMemory = meta.lengthInMemory - removedLength;
    this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem - removedLength;
    this.countOfMessagesDataInMem = this.countOfMessagesDataInMem
        - removedCount;
    this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
        - (sizeOfRef * 2 + (dstVerID.length() * sizeOfChar) + sizeOfEmptyMessageQueue);
    return incomingQueue;
  }
  
  @Override
  public ConcurrentLinkedQueue<IMessage> removeOutgoingQueue(String index) {
    ConcurrentLinkedQueue<IMessage> outgoingQueue = null;
    synchronized (this.outgoingQueues) {
      outgoingQueue = this.outgoingQueues.remove(index);
    }
    if (outgoingQueue == null) {
      return null;
    }
    int removedCount = outgoingQueue.size();
    long removeLength = this.sizeOfMessage * removedCount;
    this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem - removeLength;
    this.countOfMessagesDataInMem = this.countOfMessagesDataInMem
        - removedCount;
    this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
        - (sizeOfRef * 2 + (index.length() * sizeOfChar) + sizeOfEmptyMessageQueue);
    return outgoingQueue;
  }
  
  /**
   * On a new message incomed.
   */
  private void onMessageIncomed() {
    if (this.totalCount % 10000 == 1) {
      // Evaluate the size of a single message.
      this.sizeOfMessage = (int) (this.totalSizeOfMessages / this.totalCount);
      // Update the count threshold.
      this.countThreshold = this.sizeThreshold / this.sizeOfMessage;
    }
    // To check the memory occupied, if over the threshold, cache some buckets
    // or queues onto disk.
    if (this.countOfMessagesDataInMem >= this.countThreshold) {
      // First get the longest bucket of incoming queues.
      // Find the longest hash buck index.
      int incomingIndex = findLongestBucket(this.incomingQueues);
      // Message number of longest hash buck.
      long incomingCountInMem = this.incomingQueues.get(incomingIndex).countInMemory;
      int incomedIndex = findLongestBucketWithOut(this.incomedQueues,
          this.currentBucket);
      long incomedCountInMem = this.incomedQueues.get(incomedIndex).countInMemory;
      // If there are still in memory buckets of incomingQueues.
      if (incomingCountInMem >= this.countThresholdForBucket) {
        this.incomingFileLocks[incomingIndex].lock();
        /** Lock */
        try {
          saveBucket(this.incomingQueues, incomingIndex, "incoming");
        } catch (IOException e) {
          throw new RuntimeException("[MessageQueuesForDisk:OnMessageAdded]", e);
        } finally {
          this.incomingFileLocks[incomingIndex].unlock();
          /** Unlock */
        }
      } else if (incomedCountInMem >= this.countThresholdForBucket) {
        // Else, begin to save incomed queues.
        // Find the longest incomed queue in memory wihtout the current accessed
        // bucket.
        this.incomedFileLocks[incomedIndex].lock();
        /** Lock */
        try {
          saveBucket(this.incomedQueues, incomedIndex, "incomed");
        } catch (IOException e) {
          throw new RuntimeException("[MessageQueuesForDisk:OnMessageAdded]", e);
        } finally {
          this.incomedFileLocks[incomedIndex].unlock();
          /** Unlock */
        }
      }
    }
  }
  
  /**
   * On a new message outgoed.
   */
  private void onMessageOutgoed() {
    if (this.totalCount % 10000 == 1) {
      // Evaluate the size of a single message.
      this.sizeOfMessage = (int) (this.totalSizeOfMessages / this.totalCount);
      // Update the count threshold.
      this.countThreshold = this.sizeThreshold / this.sizeOfMessage;
    }
    // To check the memory occupied, if over the threshold, cache some buckets
    // or queues onto disk.
    if (this.countOfMessagesDataInMem >= this.countThreshold) {
      // First get the longest bucket of incoming queues.
      int incomingIndex = findLongestBucket(this.incomingQueues);
      long incomingCountInMem = this.incomingQueues.get(incomingIndex).countInMemory;
      int incomedIndex = findLongestBucketWithOut(this.incomedQueues,
          this.currentBucket);
      long incomedCountInMem = this.incomedQueues.get(incomedIndex).countInMemory;
      // If there are still in memory buckets of incomingQueues.
      if (incomingCountInMem >= this.countThresholdForBucket) {
        this.incomingFileLocks[incomingIndex].lock();
        /* Lock */
        try {
          saveBucket(this.incomingQueues, incomingIndex, "incoming");
        } catch (IOException e) {
          LOG.error("[MessageQueuesForDisk:OnMessageAdded]", e);
        } finally {
          this.incomingFileLocks[incomingIndex].unlock();
          /* Unlock */
        }
      } else if (incomedCountInMem >= this.countThresholdForBucket) {
        // Else, begin to save incomed queues.
        // Find the longest incomed queue in memory wihtout the current accessed
        // bucket.
        this.incomedFileLocks[incomedIndex].lock();
        /** Lock */
        try {
          saveBucket(this.incomedQueues, incomedIndex, "incomed");
        } catch (IOException e) {
          LOG.error("[MessageQueuesForDisk:OnMessageAdded]", e);
          
        } finally {
          this.incomedFileLocks[incomedIndex].unlock();
          /** Unlock */
        }
      } else {
        try {
          Thread.sleep(500);
        } catch (Exception e) {
          LOG.error("[Sender] caught: ", e);
        }
      }
    }
  }
  
  /**
   * Find the longest bucket now in memory of the queuesBuckets.
   * @param queuesBuckets
   * @return int bucketIndex of the longest bucket in memory.
   */
  private synchronized int findLongestBucket(ArrayList<BucketMeta> queueBucks) {
    int bucketIndex = 0;
    long longestLength = 0;
    for (int i = 0; i < this.hashBucketNumber; i++) {
      BucketMeta meta = queueBucks.get(i);
      if (meta.lengthInMemory > longestLength) {
        longestLength = meta.lengthInMemory;
        bucketIndex = i;
      }
    }
    return bucketIndex;
  }
  
  /**
   * Find the longest bucket now in memory of the queuesBuckets without the
   * bucket indexed by withoutIndex.
   * @param queuesBuckets
   * @param withoutIndex
   * @return int bucketIndex of the longest bucket in memory.
   */
  private synchronized int findLongestBucketWithOut(
      ArrayList<BucketMeta> queuesBuckets, int withoutIndex) {
    int bucketIndex = 0;
    long longestLength = 0;
    for (int i = 0; i < this.hashBucketNumber; i++) {
      // Skip the bucket that is being accessed.
      if (i == withoutIndex) {
        continue;
      }
      BucketMeta meta = queuesBuckets.get(i);
      // Find the longest but lengthInMemory is not zero.
      if (meta.lengthInMemory > longestLength) {
        longestLength = meta.lengthInMemory;
        bucketIndex = i;
      }
    }
    return bucketIndex;
  }
  
  /**
   * Cache the bucket of messages indexed by bucketIndex onto disk file.
   * @param queuesBuckets
   * @param bucketIndex
   * @param queuePath
   * @throws IOException
   */
  private void saveBucket(ArrayList<BucketMeta> queuesBuckets, int bucketIndex,
      String queuePath) throws IOException {
    if (queuesBuckets.get(bucketIndex).countInMemory < this.countThresholdForBucket) {
      return;
    }
    LOG.info("[MessageQueuesForDisk] is saving the [" + queuePath + " Bucket-"
        + bucketIndex + "] >>> size = "
        + queuesBuckets.get(bucketIndex).countInMemory + ".");
    long start = System.currentTimeMillis();
    /* Clock */
    File messagesDataFileBucket;
    FileWriter fwMessagesData;
    BufferedWriter bwMessagesData;
    File messagesDataFileQueue = new File(this.messagesDataFile + "/"
        + queuePath);
    if (!messagesDataFileQueue.exists()) {
      if (!messagesDataFileQueue.mkdir()) {
        throw new IOException("Make dir " + messagesDataFileQueue + " fail!");
      }
    }
    messagesDataFileBucket = new File(messagesDataFileQueue + "/" + "bucket-"
        + bucketIndex);
    boolean isNewFile = false;
    // The bucket file does not exit, create it.
    if (!messagesDataFileBucket.exists()) {
      if (!messagesDataFileBucket.createNewFile()) {
        throw new IOException("Create bucket file" + messagesDataFileBucket
            + " failed!");
      }
      isNewFile = true;
    }
    // Append to the bucket file by line.
    fwMessagesData = new FileWriter(messagesDataFileBucket, true);
    bwMessagesData = new BufferedWriter(fwMessagesData, 65536);
    if (isNewFile) {
      // Write the file header.
      bwMessagesData.write(Constants.MSG_BUCKET_FILE_HEADER + "-" + queuePath
          + "-" + bucketIndex);
    }
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> queueMap = queuesBuckets
        .get(bucketIndex).queueMap;
    ConcurrentLinkedQueue<IMessage> tempQueue = null;
    Entry<String, ConcurrentLinkedQueue<IMessage>> entry = null;
    Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it = queueMap
        .entrySet().iterator();
    // Traverse the map of queues and cache them to disk file.
    while (it.hasNext()) {
      entry = it.next();
      String key = entry.getKey();
      tempQueue = entry.getValue();
      if (tempQueue.size() <= 0) {
        continue;
      }
      bwMessagesData.newLine();
      bwMessagesData.write(key + Constants.KV_SPLIT_FLAG
          + queueToString(tempQueue));
      this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
          - (sizeOfRef + sizeOfInteger + sizeOfEmptyMessageQueue);
    } // while
    bwMessagesData.close();
    fwMessagesData.close();
    // Update the meta data of the bucket.
    BucketMeta meta = queuesBuckets.get(bucketIndex);
    // Update the size of messages data in memory.
    this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem
        - meta.lengthInMemory;
    this.countOfMessagesDataInMem = this.countOfMessagesDataInMem
        - meta.countInMemory;
    meta.onDiskFlag = true;
    meta.lengthInMemory = 0;
    meta.countInMemory = 0;
    meta.queueMap.clear();
    this.writeDiskTime = this.writeDiskTime
        + (System.currentTimeMillis() - start);
  }
  
  /**
   * Load bucket from disk.
   * @param queuesBuckets
   * @param bucketIndex
   * @param queuePath
   * @throws IOException
   */
  private void loadBucket(ArrayList<BucketMeta> queuesBuckets, int bucketIndex,
      String queuePath) throws IOException {
    LOG.info("[MessageQueuesForDisk] is loading the [" + queuePath + " Bucket-"
        + bucketIndex + "] <<< size = " + queuesBuckets.get(bucketIndex).count
        + ".");
    long start = System.currentTimeMillis();
    /** Clock */
    File messagesDataFileBucket;
    FileReader frMessagesData;
    BufferedReader brMessagesData;
    messagesDataFileBucket = new File(this.messagesDataFile + "/" + queuePath
        + "/" + "bucket-" + bucketIndex);
    if (!messagesDataFileBucket.exists()) {
      throw new IOException("Bucket file" + messagesDataFileBucket
          + " does not exit!");
    }
    // Open file readers.
    frMessagesData = new FileReader(messagesDataFileBucket);
    brMessagesData = new BufferedReader(frMessagesData);
    // Read the file header.
    @SuppressWarnings("unused")
    String bucketHeader = brMessagesData.readLine();
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> queueMap = queuesBuckets
        .get(bucketIndex).queueMap;
    if (queueMap == null) {
      queueMap = new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
    }
    String buffer;
    while ((buffer = brMessagesData.readLine()) != null) {
      String[] queueBuffer = buffer.split(Constants.KV_SPLIT_FLAG);
      if (queueBuffer[0] == "") {
        LOG.warn("[MessageQueuesForDisk] readLine = " + buffer);
      }
      String key = queueBuffer[0];
      ConcurrentLinkedQueue<IMessage> queue = queueMap.get(key);
      if (queue == null) {
        queue = stringToQueue(queueBuffer[1]);
        this.sizeOfHashMapsInMem = this.sizeOfHashMapsInMem
            + (sizeOfRef + sizeOfInteger + sizeOfEmptyMessageQueue);
      } else {
        queue.addAll(stringToQueue(queueBuffer[1]));
      }
      queueMap.put(key, queue);
    }
    queuesBuckets.get(bucketIndex).queueMap = queueMap;
    brMessagesData.close();
    frMessagesData.close();
    // Update the meta data of the bucket.
    BucketMeta meta = queuesBuckets.get(bucketIndex);
    // Update the size of messages data in memory.
    this.sizeOfMessagesDataInMem = this.sizeOfMessagesDataInMem
        + (meta.length - meta.lengthInMemory);
    this.countOfMessagesDataInMem = this.countOfMessagesDataInMem
        + (meta.count - meta.countInMemory);
    meta.onDiskFlag = false;
    meta.lengthInMemory = meta.length;
    meta.countInMemory = meta.count;
    queuesBuckets.set(bucketIndex, meta);
    if (!messagesDataFileBucket.delete()) {
      throw new IOException("Bucket file delete failed!");
    }
    this.readDiskTime = this.readDiskTime
        + (System.currentTimeMillis() - start);
    /** Clock */
  }
  
  /**
   * ToString method for message queue.
   * @return String
   */
  private String queueToString(ConcurrentLinkedQueue<IMessage> queue) {
    String buffer;
    buffer = queue.poll().intoString();
    IMessage msg;
    while ((msg = queue.poll()) != null) {
      buffer = buffer + Constants.SPACE_SPLIT_FLAG + msg.intoString();
    }
    return buffer;
  }
  
  /**
   * Change String to queue.
   * @return ConcurrentLinkedQueue<IMessage>
   */
  @SuppressWarnings("unchecked")
  private ConcurrentLinkedQueue<IMessage> stringToQueue(String queueBuffer) {
    ConcurrentLinkedQueue<IMessage> queue = new ConcurrentLinkedQueue<IMessage>();
    if (queueBuffer != null) {
      String[] msgs = queueBuffer.split(Constants.SPACE_SPLIT_FLAG);
      for (int i = 0; i < msgs.length; i++) {
        // Note BSPMessage Is Temporayly Left.Should Be class.newInstance().
        // class Should Be Transferred In.
        IMessage msg = new BSPMessage();
        msg.fromString(msgs[i]);
        queue.add(msg);
      }
    }
    return queue;
  }
  
  @Override
  public int getIncomingQueueSize(String dstVertexID) {
    ConcurrentLinkedQueue<IMessage> incomingQueue = null;
    // Get the hash bucket index.
    int hashCode = dstVertexID.hashCode();
    int hashIndex = hashCode % this.hashBucketNumber; // bucket index
    hashIndex = (hashIndex < 0 ? hashIndex + this.hashBucketNumber : hashIndex);
    BucketMeta meta = this.incomingQueues.get(hashIndex);
    incomingQueue = meta.queueMap.get(dstVertexID);
    if (incomingQueue != null) {
      return incomingQueue.size();
    } else {
      return 0;
    }
  }
  
  @Override
  public int getOutgoingQueueSize(String index) {
    ConcurrentLinkedQueue<IMessage> queue = null;
    synchronized (this.outgoingQueues) {
      queue = this.outgoingQueues.get(index);
    }
    if (queue != null) {
      return queue.size();
    } else {
      return 0;
    }
  }
  
  /** Show the information of Memory. */
  public void showMemoryInfo() {
    LOG.info("---------------- Memory Info for Messages ------------------");
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
    long used = memoryUsage.getUsed();
    long committed = memoryUsage.getCommitted();
    LOG.info("<Real> [Memory used] = " + used / MB_SIZE + "MB");
    LOG.info("<Real> [Memory committed] = " + committed / MB_SIZE + "MB");
    LOG.info("<Evaluate> [size of Message] = " + this.sizeOfMessage + "B");
    LOG.info("<Evaluate> [size of Messages Data In Memory] = "
        + this.sizeOfMessagesDataInMem / MB_SIZE + "MB");
    LOG.info("<Evaluate> [size of HashMaps In Memory] = "
        + this.sizeOfHashMapsInMem / MB_SIZE + "MB");
    LOG.info("<Evaluate> [size of Messages Data Threshold] = "
        + this.sizeThreshold / MB_SIZE + "MB");
    LOG.info("<Evaluate> [count of Messages Data In Memory] = "
        + this.countOfMessagesDataInMem / KB_SIZE + "K");
    LOG.info("<Evaluate> [count of Messages Data Threshold] = "
        + this.countThreshold / KB_SIZE + "K");
    LOG.info("----------------- ------------------------ -----------------");
   // showHashBucketsInfo();
  }
  
  /** Show the information of Hash Bucket. */
  private void showHashBucketsInfo() {
    LOG.info("------------ Buckets Info of Messages ------------");
    LOG.info("[Incoming Queues]:");
    long maxCount = 0;
    for (int i = 0; i < this.incomingQueues.size(); i++) {
      BucketMeta meta = this.incomingQueues.get(i);
      if (meta.count > maxCount) {
        maxCount = meta.count;
      }
    }
    for (int i = 0; i < this.incomingQueues.size(); i++) {
      BucketMeta meta = this.incomingQueues.get(i);
      String out = "[Incoming-" + i + "] ";
      if (meta.onDiskFlag) {
        out = out + "OnDisk ";
      } else {
        out = out + "       ";
      }
      out = out + meta.lengthInMemory / MB_SIZE + "MB - " + meta.length
          / MB_SIZE + "MB ";
      int nMax = 30;
      int nAll = (int) (nMax * ((float) meta.count / (float) maxCount));
      int nMem = (int) (nAll * ((float) meta.countInMemory / (float) meta.count));
      int nDisk = nAll - nMem;
      for (int j = 0; j < nMem; j++) {
        out = out + "-";
      }
      for (int j = 0; j < nDisk; j++) {
        out = out + "*";
      }
      LOG.info(out);
    }
    LOG.info("[Incomed Queues]:");
    maxCount = 0;
    for (int i = 0; i < this.incomedQueues.size(); i++) {
      BucketMeta meta = this.incomedQueues.get(i);
      if (meta.count > maxCount) {
        maxCount = meta.count;
      }
    }
    for (int i = 0; i < this.incomedQueues.size(); i++) {
      BucketMeta meta = this.incomedQueues.get(i);
      String out = "[Incomed-" + i + "] ";
      if (meta.onDiskFlag) {
        out = out + "OnDisk ";
      } else {
        out = out + "       ";
      }
      out = out + meta.lengthInMemory / MB_SIZE + "MB - " + meta.length
          / MB_SIZE + "MB ";
      int nMax = 30;
      int nAll = (int) (nMax * ((float) meta.count / (float) maxCount));
      int nMem = (int) (nAll * ((float) meta.countInMemory / (float) meta.count));
      int nDisk = nAll - nMem;
      for (int j = 0; j < nMem; j++) {
        out = out + "-";
      }
      for (int j = 0; j < nDisk; j++) {
        out = out + "*";
      }
      LOG.info(out);
    }
    LOG.info("------------ --------------------- ------------");
  }
  
  /** Show the information of Memory Occupation. */
  @SuppressWarnings("unused")
  private void showInMemoryOccupation() {
    LOG.info("------------ Messages In Memory ------------");
    float outgoPercent = (float) this.getOutgoingQueuesSize()
        / (float) this.countOfMessagesDataInMem;
    float incomedPercent = (float) this.getIncomedQueuesSizeInMem()
        / (float) this.countOfMessagesDataInMem;
    float incomingPercent = (float) this.getIncomingQueuesSizeInMem()
        / (float) this.countOfMessagesDataInMem;
    int maxHeight = 50;
    LOG.info("[Memory Threshold] = " + this.countThreshold / KB_SIZE + "K");
    LOG.info("[In memory Now] = " + this.countOfMessagesDataInMem / KB_SIZE + "K");
    LOG.info("[Outgo] = " + (int) (outgoPercent * 100) + "%, [Incomed] = "
        + (int) (incomedPercent * 100) + "%, [Incoming] = "
        + (int) (incomingPercent * 100) + "%");
    int outgoHeight = (int) (maxHeight * outgoPercent);
    int incomedHeight = (int) (maxHeight * incomedPercent);
    int incomingHeight = (int) (maxHeight * incomingPercent);
    for (int i = 0; i < outgoHeight; i++) {
      LOG.info("oooooooooooooooooooo");
    }
    for (int i = 0; i < incomedHeight; i++) {
      LOG.info("********************");
    }
    for (int i = 0; i < incomingHeight; i++) {
      LOG.info("!!!!!!!!!!!!!!!!!!!!");
    }
    LOG.info("--------------------------------------------");
  }
  
  @Override
  public String getNextOutgoingQueueIndex() throws Exception {
    String nextIndex = null;
    synchronized (this.outgoingQueues) {
      int size = this.outgoingQueues.size();
      if (size == 0) {
        return null;
      }
      if (this.nextOutgoingQueueCount > size) {
        this.nextOutgoingQueueCount = 1;
      }
      Entry<String, ConcurrentLinkedQueue<IMessage>> entry = null;
      Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it = this.outgoingQueues
          .entrySet().iterator();
      for (int i = 0; i < this.nextOutgoingQueueCount; i++) {
        entry = it.next();
        nextIndex = entry.getKey();
      }
      this.nextOutgoingQueueCount++;
    }
    return nextIndex;
  }
  
  /* Zhicheng Liu added */
  @Override
  public IMessage getAMessage() {
    ConcurrentLinkedQueue<IMessage> incomingQueue = null;
    // Get the hash bucket index.
    int hashIndex = (int) (Math.random() * 10);
    BucketMeta meta = this.incomingQueues.get(hashIndex);
    // The bucket is on disk.
    if (meta.onDiskFlag) {
      this.incomingFileLocks[hashIndex].lock();
      /* Lock */
      try {
        loadBucket(this.incomingQueues, hashIndex, "incoming");
      } catch (IOException e) {
        LOG.error("[MessageQueuesForDisk:removeIncomingQueue]", e);
      } finally {
        this.incomingFileLocks[hashIndex].unlock();
        /** Unlock */
      }
    }
    meta = this.incomingQueues.get(hashIndex);
    // incomingQueue = meta.queueMap.remove(dstVertexID);
    Iterator<String> it = meta.queueMap.keySet().iterator();
    IMessage mes = null;
    while (it.hasNext()) {
      String vertexID = it.next();
      if (vertexID != null) {
        ConcurrentLinkedQueue<IMessage> messages = meta.queueMap.get(vertexID);
        if (messages == null) {
          continue;
        }
        mes = messages.peek();
      }
      break;
    }
    return mes;
  }

  public int getHashBucketNumber() {
    return hashBucketNumber;
  }

  public void setHashBucketNumber(int hashBucketNumber) {
    this.hashBucketNumber = hashBucketNumber;
  }

  public int getCurrentBucket() {
    return currentBucket;
  }

  public void setCurrentBucket(int currentBucket) {
    this.currentBucket = currentBucket;
  }

  public ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> getOutgoingQueues() {
    return outgoingQueues;
  }

  public void setOutgoingQueues(
      ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues) {
    this.outgoingQueues = outgoingQueues;
  }
}
