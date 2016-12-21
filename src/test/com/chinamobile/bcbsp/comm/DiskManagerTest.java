
package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.comm.io.util.MemoryAllocator;
import com.chinamobile.bcbsp.comm.io.util.MessageBytePoolPerPartition;
import com.chinamobile.bcbsp.examples.bytearray.pagerank.PageRankMessage;
import com.chinamobile.bcbsp.graph.MetaDataOfGraph;
import com.chinamobile.bcbsp.util.BSPJob;

public class DiskManagerTest {
  private static Log LOG = LogFactory.getLog(DiskManagerTest.class);
  private String baseDir;
  private DiskManager diskManager;
  private String jobID;
  private BSPJob job;
  
  @Before
  public void setUp() throws Exception {
    baseDir = "/tmp/bcbsp";
    jobID = "job_201407142009_0001";
    job = mock(BSPJob.class);
  }
  
  @Test
  public void testDiskManagerStringStringIntInt() {
    diskManager = new DiskManager(baseDir, jobID, 0, 1);
    assertEquals(
        "Using baseDir, baseDir, partitionId, partitionBucketNum to construct a DiskManager",
        "/tmp/bcbsp/job_201407142009_0001/Partition-0/Messages",
        diskManager.getWorkerDir());
  }
  
  @Test
  public void testProcessMessagesSaveArrayListOfIMessageIntInt()
      throws IOException {
    diskManager = new DiskManager(baseDir, jobID, 1, 1);
    ArrayList<IMessage> msgList = new ArrayList<IMessage>();
    IMessage msg = new PageRankMessage();
    msg.setMessageId(1);
    msg.setDstPartition(1);
    msgList.add(msg);
    MetaDataOfMessage.HASH_NUMBER = 1;
    MetaDataOfMessage.PARTITIONBUCKET_NUMBER = 2;
    MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = 6;
    MetaDataOfGraph.initStatisticsPerBucket();
    MetaDataOfGraph.BCBSP_MAX_BUCKET_INDEX = 5;
    MemoryAllocator memoryAllocator = new MemoryAllocator(job);
    memoryAllocator.setupOnEachSuperstep(0, LOG);
    diskManager.processMessagesSave(msgList, 3, 0);
    boolean fileExists = new File(
        "/tmp/bcbsp/job_201407142009_0001/Partition-1/Messages/superstep-1/bucket-0")
        .isDirectory();
    assertEquals("Test processMessagesSave in DiskManager. ", true, fileExists);
  }
  
  @Test
  @Ignore(value = "no use in new version 2014-08-05")
  public void testProcessMessageLoad() throws IOException {
    IMessage msg1 = new PageRankMessage();
    IMessage msg2 = new PageRankMessage();
    msg1.setMessageId(1);
    msg1.setMessageId(2);
    ArrayList<IMessage> msgList = new ArrayList<IMessage>();
    msgList.add(msg1);
    msgList.add(msg2);
    MetaDataOfMessage.HASH_NUMBER = 1;
    MetaDataOfMessage.PARTITIONBUCKET_NUMBER = 2;
    MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = 6;
    MetaDataOfGraph.initStatisticsPerBucket();
    MetaDataOfGraph.BCBSP_MAX_BUCKET_INDEX = 5;
    MemoryAllocator memoryAllocator = new MemoryAllocator(job);
    memoryAllocator.setupOnEachSuperstep(0, LOG);
    HashMap<String, ArrayList<IMessage>> msgMap = new HashMap<String, ArrayList<IMessage>>();
    diskManager = new DiskManager(baseDir, jobID, 1, 1);
    diskManager.processMessageLoad(msgMap, 3, 0);
    System.out.println("msgMap " + msgMap.size());
    
  }
  
  @Test
  public void testProcessMessagesSaveMessageBytePoolPerPartitionIntInt()
      throws IOException {
    diskManager = new DiskManager(baseDir, jobID, 1, 1);
    MetaDataOfMessage.HASH_NUMBER = 1;
    MessageBytePoolPerPartition messages = new MessageBytePoolPerPartition(10);
    MetaDataOfMessage.HASH_NUMBER = 1;
    MetaDataOfMessage.PARTITIONBUCKET_NUMBER = 2;
    MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = 6;
    MetaDataOfGraph.initStatisticsPerBucket();
    MetaDataOfGraph.BCBSP_MAX_BUCKET_INDEX = 5;
    MemoryAllocator memoryAllocator = new MemoryAllocator(job);
    memoryAllocator.setupOnEachSuperstep(0, LOG);
    diskManager.processMessagesSave(messages, 4, 0);
    boolean fileExists = new File(
        "/tmp/bcbsp/job_201407142009_0001/Partition-1/Messages/superstep-4/bucket-0")
        .isDirectory();
    assertEquals("Test processMessagesSave in DiskManager. ", true, fileExists);

  }
  
  @Test
  public void testPreLoadFile() {
    diskManager = new DiskManager(baseDir, jobID, 1, 1);
    boolean fileLoad = (diskManager.preLoadFile(3, 0)==null);
    assertEquals("Test PreLoadFile in DiskManager. ", false, fileLoad);
  }
  
  @Test
  public void testProcessMessageLoadFile() throws IOException {
    diskManager = new DiskManager(baseDir, jobID, 1, 1);
    IMessage msg1 = new PageRankMessage();
    IMessage msg2 = new PageRankMessage();
    msg1.setMessageId(1);
    msg1.setMessageId(2);
    MetaDataOfMessage.HASH_NUMBER = 1;
    MessageBytePoolPerPartition messages = new MessageBytePoolPerPartition(10);
    MetaDataOfMessage.HASH_NUMBER = 1;
    MetaDataOfMessage.PARTITIONBUCKET_NUMBER = 2;
    MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = 6;
    MetaDataOfGraph.initStatisticsPerBucket();
    MetaDataOfGraph.BCBSP_MAX_BUCKET_INDEX = 5;
    MemoryAllocator memoryAllocator = new MemoryAllocator(job);
    memoryAllocator.setupOnEachSuperstep(0, LOG);
    messages.add(msg1);
    messages.add(msg2);
    File f = new File(
        "/tmp/bcbsp/job_201407142009_0001/Partition-1/Messages/superstep-4/bucket-0/srcPartition-0_counter-0_count-0");
    diskManager.processMessageLoadFile(messages, f);
    assertEquals("Test processMessagesloadfile in DiskManager. ", 0, messages.getSize() );
  }
  
}
