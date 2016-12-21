
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.io.BSPFileOutputFormat;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPhdfsRW;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPhdfsRWImpl A concrete class that implements interface BSPhdfsRW.
 *
 */
public class BSPhdfsRWImpl implements BSPhdfsRW {
  /** State a FSDataOutputStream type of variable fileOut */
  private FSDataOutputStream fileOut = null;

  /**
   * constructor
   * @param job
   *        BSPJob
   * @param staffId
   *        StaffAttemptID
   * @param writePath
   *        Path
   * @throws IOException
   */
  public BSPhdfsRWImpl(BSPJob job, StaffAttemptID staffId, Path writePath)
      throws IOException {
    Path file = BSPFileOutputFormat.getOutputPath(staffId, writePath);
    FileSystem fs = file.getFileSystem(job.getConf());
    fileOut = fs.create(file, false);

  }

  @Override
  public FSDataOutputStream getFileOut() {
    return fileOut;
  }


}
