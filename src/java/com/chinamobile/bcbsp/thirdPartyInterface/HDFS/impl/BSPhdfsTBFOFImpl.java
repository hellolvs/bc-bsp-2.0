
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPhdfsTBFOF;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.io.BSPFileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPhdfsTBFOFImpl A concrete class that implements interface BSPhdfsTBFOF.
 *
 */
public class BSPhdfsTBFOFImpl implements BSPhdfsTBFOF {
  /** State a FSDataOutputStream type of variable fileOut */
  private FSDataOutputStream fileOut = null;

  /**
   * constructor
   * @param job
   *        BSPJob
   * @param staffId
   *        StaffAttemptID
   * @throws IOException
   */
  public BSPhdfsTBFOFImpl(BSPJob job, StaffAttemptID staffId)
      throws IOException {
    Path file = BSPFileOutputFormat.getOutputPath(job, staffId);
    FileSystem fs = file.getFileSystem(job.getConf());
    fileOut = fs.create(file, false);
  }

  @Override
  public FSDataOutputStream getFileOut() {
    return fileOut;
  }



}
