
package com.chinamobile.bcbsp.bspcontroller;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.util.BSPJobID;

public class QueueManagerTest {
  private QueueManager queue;
  private BSPConfiguration conf;
  private Path jobFile;
  private BSPController controller;
  private BSPJobID jobId;
  
  @Before
  public void setUp() throws Exception {
    conf = new BSPConfiguration();
    jobId = new BSPJobID();
    controller = new BSPController();
    queue = new QueueManager(conf);
    jobFile = new Path("hdfs://Master:9000/user/root/input");
    controller = mock(BSPController.class);
    when(controller.getLocalPath("hdfs://Master:9000/user/root/input")).thenReturn(jobFile).toString();
    when(controller.getSystemDirectoryForJob(jobId)).thenReturn(jobFile)
        .toString();
    
  }
  
  @Test
  public void testAddJob() throws IOException {
    JobInProgress job = null;
    String waitQueue;
    job = new JobInProgress(); 
    waitQueue = "HIGHER_WAIT_QUEUE";
    queue.createHRNQueue(waitQueue);
    queue.addJob(waitQueue, job);
    assertEquals(true, queue.findQueue(waitQueue) != null);
  }
  
  @Test
  public void testCreateLogFile() throws IOException {
	boolean flag;
    queue.createLogFile();
    flag = queue.getQueueLogOperator().isExist(conf.get(Constants.BC_BSP_HA_LOG_DIR) +
        Constants.BC_BSP_HA_QUEUE_OPERATE_LOG);
    assertEquals(true, flag);
  }

  @Test
  public void testMoveJobHrn() {
    JobInProgress job = null;
    String waitQueue;
    job = new JobInProgress();
    queue = new QueueManager(conf);
    waitQueue = "HIGH_WAIT_QUEUE";
    queue.createHRNQueue("HIGHER_WAIT_QUEUE");
    queue.createHRNQueue("HIGH_WAIT_QUEUE");
    queue.createHRNQueue("NORMAL_WAIT_QUEUE");
    queue.createHRNQueue("LOW_WAIT_QUEUE");
    queue.createHRNQueue("LOWER_WAIT_QUEUE");
    queue.addJob(waitQueue, job);
    queue.moveJobHrn("HIGHER_WAIT_QUEUE", job);
    
    assertEquals(true, queue.findQueue("HIGHER_WAIT_QUEUE") != null);
  }
  
  @Test
  public void testGetJobs() {
    JobInProgress job = null;
    String waitQueue;
    job = new JobInProgress();
    JobInProgress job1 = null;
    job1 = new JobInProgress();
    
    queue = new QueueManager(conf);
    waitQueue = "HIGH_WAIT_QUEUE";
    queue.createHRNQueue("HIGHER_WAIT_QUEUE");
    queue.createHRNQueue("HIGH_WAIT_QUEUE");
    queue.createHRNQueue("NORMAL_WAIT_QUEUE");
    queue.createHRNQueue("LOW_WAIT_QUEUE");
    queue.createHRNQueue("LOWER_WAIT_QUEUE");
    queue.addJob(waitQueue, job);
    queue.addJob(waitQueue, job1);
    assertEquals(2, queue.getJobs().size());
  }
}
