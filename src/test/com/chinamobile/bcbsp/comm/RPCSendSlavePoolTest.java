package com.chinamobile.bcbsp.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class RPCSendSlavePoolTest {
  private int maxSlaveNum;
  private String jobID;
  private BSPJobID bspjobID;
  private RPCSendSlavePool sendSlavePool;
  private String hostNameAndPort;
  private RPCSender sender;
  private RPCCommunicator comm;
  private MessageQueuesInterface messageQueues;
  private int superStepCount;
  
  @Before
  public void setUp() throws Exception {
    BSPJob job = mock(BSPJob.class);
    maxSlaveNum = 1;
    jobID = "job_201407142009_0001";
    hostNameAndPort = "Slave1.Hadoop:3030";
    superStepCount = 2;
    comm = mock(RPCCommunicator.class);
    when(comm.getJob()).thenReturn(job);
    when(comm.getBSPJobID()).thenReturn(bspjobID);
    when(comm.getMessageQueues()).thenReturn(messageQueues);
    when(comm.isSendCombineSetFlag()).thenReturn(false);
    sender = mock(RPCSender.class);
  }
  
  @Test
  public void testRPCSendSlavePool() {
    sendSlavePool = new RPCSendSlavePool(maxSlaveNum, jobID);
    assertEquals("Use maxSlaveNum, jobID to construct RPCSendSlavePool", 1, sendSlavePool
        .getMaxSlaveNum());
  }
  
  @Test
  public void testGetSlaveCount() {
    sendSlavePool = new RPCSendSlavePool(maxSlaveNum, jobID);
    sendSlavePool.getSlave(hostNameAndPort, superStepCount, sender);
    assertEquals("Test getSlaveCount method", 1, sendSlavePool.getSlaveCount());
  }
  
  @Test
  public void testGetActiveSlaveCount() {
    sendSlavePool = new RPCSendSlavePool(maxSlaveNum, jobID);
    sendSlavePool.getSlave(hostNameAndPort, superStepCount, sender);
    assertEquals("Test getSlaveCount method", 1, sendSlavePool.getActiveSlaveCount(superStepCount));

  }
  
  @Test
  public void testGetSlave() {
    sendSlavePool = new RPCSendSlavePool(maxSlaveNum, jobID);
    boolean nullFlag = (sendSlavePool.getSlave(hostNameAndPort, 0, sender)==null);
    assertEquals("Test getSlave method", false, nullFlag);
  }
  
  @Test
  public void testCleanFailedSlave() {
    sendSlavePool = new RPCSendSlavePool(maxSlaveNum, jobID);
    RPCSingleSendSlave rpcSlave = mock(RPCSingleSendSlave.class);
    when(rpcSlave.isFailed()).thenReturn(true);
    sendSlavePool.getSlaves().put(hostNameAndPort, rpcSlave);
    sendSlavePool.cleanFailedSlave();
    assertEquals("Test CleanFailedSlave method", 0, sendSlavePool.getSlaves().size());

  }
}
