package com.chinamobile.bcbsp.comm.io.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.comm.MetaDataOfMessage;
import com.chinamobile.bcbsp.graph.MetaDataOfGraph;
import com.chinamobile.bcbsp.util.BSPJob;

public class MemoryAllocatorTest {
  private static final Log LOG = LogFactory.getLog(MemoryAllocatorTest.class);
  private BSPJob job;
  private MemoryAllocator memoryAllocator;
  
  @Before
  public void setUp() throws Exception {
    job = mock(BSPJob.class);
  }
  
  @Test
  public void testMemoryAllocator() {
    memoryAllocator = new MemoryAllocator(job);
    assertEquals("Use job to construct MemoryAllocator", 8, MetaDataOfGraph.BYTESIZE_PER_VERTEX);
  }
  
  @Test
  public void testSetupOnEachSuperstep() {
    MetaDataOfMessage.PARTITIONBUCKET_NUMBER = 2;
    MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = 6;
    MetaDataOfGraph.initStatisticsPerBucket();
    MetaDataOfGraph.BCBSP_MAX_BUCKET_INDEX = 5;
    memoryAllocator = new MemoryAllocator(job);
    memoryAllocator.setupOnEachSuperstep(2, LOG);
    memoryAllocator.setupOnEachSuperstep(0, LOG);
  }
  
}
