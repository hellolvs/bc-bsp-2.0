
package com.chinamobile.bcbsp.io;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPHdfs;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.util.BSPJob;

public class BSPFileOutputFormatTest {
  private BSPJob job;
  
  @Test
  public void testSetOutputPath() throws IOException {
    BSPConfiguration conf = new BSPConfiguration();
    conf.set("job.output.dir", new Path("hdfs://Master.Hadoop:9000/user/user/").toString());
    job = new BSPJob(conf, 2);
    job.setOutputFormatClass(TextBSPFileOutputFormat.class);
    BSPHdfs checkout = new BSPHdfsImpl();
    System.out.println(new Path("hdfs://Master.Hadoop:9000/user/user/output1"));
    System.out
        .println(checkout.hdfscheckOutputSpecs(job).getHomeDirectory());
    checkout.hdfscheckOutputSpecs(job).exists(
        new Path("hdfs://Master.Hadoop:9000/user/user/output1"));
    
     TextBSPFileOutputFormat.setOutputPath(job, new
     Path("hdfs://Master.Hadoop:9000/user/user/output1"));
  }
  
}
