package com.chinamobile.bcbsp.util;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.client.BSPJobClient;
import com.chinamobile.bcbsp.util.BSPJob.JobState;

public class BSPJobTest {
  private BSPJob job;
  private BSPConfiguration conf;
  private BSPJobClient client;
  
  @Before
  public void setUp() throws Exception {
    conf = new BSPConfiguration();
    job = new BSPJob(conf, 2);
//    client = new BSPJobClient();
//    client.setConf(conf);
  }
  
  @Test
  public void testBSPJobBSPConfigurationString() throws IOException {
    job = new BSPJob(conf,2);
  }
}
