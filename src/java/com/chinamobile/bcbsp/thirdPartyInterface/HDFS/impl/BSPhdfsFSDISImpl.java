
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPhdfsFSDIS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * BSPhdfsFSDISImpl A concrete class that implements interface BSPhdfsFSDIS.
 *
 */
public class BSPhdfsFSDISImpl implements BSPhdfsFSDIS {
  /** State a FSDataInputStream type of variable fileIn */
  private FSDataInputStream fileIn = null;

  /**
   * constructor
   * @param split
   *        FileSplit
   * @param conf
   *        Configuration
   * @throws IOException
   */
  public BSPhdfsFSDISImpl(FileSplit split, Configuration conf)
      throws IOException {
    FileSystem fs = split.getPath().getFileSystem(conf);

    fileIn = fs.open(split.getPath());

  }

  @Override
  public FSDataInputStream getFileIn() {
    return fileIn;
  }

  @Override
  public void seek(long s) throws IOException {
    // TODO Auto-generated method stub
    fileIn.seek(s);
  }

}
