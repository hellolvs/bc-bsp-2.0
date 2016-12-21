
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPLocalFileSystem;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPLocalFileSystemImpl
 *  A concrete class that implements interface BSPLocalFileSystem.
 *
 */
public class BSPLocalFileSystemImpl implements BSPLocalFileSystem {
  /** State a LocalFileSystem type of variable localFs */
  private LocalFileSystem localFs;

  /**
   * constructor
   * @param conf
   *        Configuration
   * @throws IOException
   */
  public BSPLocalFileSystemImpl(Configuration conf) throws IOException {
    localFs = FileSystem.getLocal(conf);

  }

  @Override
  public boolean delete(Path f, boolean b) throws IOException {
    // TODO Auto-generated method stub
    return localFs.delete(f, b);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    // TODO Auto-generated method stub
    return localFs.exists(f);
  }

}
