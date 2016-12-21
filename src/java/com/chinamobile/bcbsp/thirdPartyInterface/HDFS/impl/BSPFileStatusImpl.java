
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileStatus;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPFileStatusImpl A concrete class that implements interface BSPFileStatus.
 *
 */
public class BSPFileStatusImpl implements BSPFileStatus {
  /** State a FileStatus type of variable file*/
  private FileStatus file;

  @Override
  public FileStatus getFile() {
    return file;
  }

  @Override
  public long getLen() {
    // TODO Auto-generated method stub
    return file.getLen();

  }

  @Override
  public Path getPath() {
    // TODO Auto-generated method stub
    return file.getPath();
  }

  @Override
  public long getBlockSize() {
    // TODO Auto-generated method stub
    return file.getBlockSize();
  }

}
