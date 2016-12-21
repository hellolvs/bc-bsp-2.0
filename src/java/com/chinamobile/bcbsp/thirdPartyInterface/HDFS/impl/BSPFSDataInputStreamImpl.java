
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.bspcontroller.HDFSOperator;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFSDataInputStream;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileSystem;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPFSDataInputStreamImpl
 * A concrete class that implements interface BSPFSDataInputStream.
 *
 */
public class BSPFSDataInputStreamImpl implements BSPFSDataInputStream {
  /** State a FSDataInputStream type of variable hdfsInStream */
  private FSDataInputStream hdfsInStream = null;
  /** State a FSDataInputStream type of variable in */
  private FSDataInputStream in;

  /**
   * constructor
   * @param bspfs
   *        BSPFileSystem
   * @param path
   *        the given path
   * @throws IOException
   */
  public BSPFSDataInputStreamImpl(BSPFileSystem bspfs, Path path)
      throws IOException {
    hdfsInStream = bspfs.open(path);

  }

  /**
   * constructor
   * @param haLogOperator
   *        HDFSOperator
   * @param file
   *        the given file
   * @throws IOException
   */
  public BSPFSDataInputStreamImpl(HDFSOperator haLogOperator, String file)
      throws IOException {
    in = haLogOperator.readFile(file);

  }

  @Override
  public int read(byte[] b) throws IOException {
    return hdfsInStream.read(b);

  }

  @Override
  public void hdfsInStreamclose() throws IOException {
    // TODO Auto-generated method stub
    hdfsInStream.close();
  }

  @Override
  public String readUTF() throws IOException {
    // TODO Auto-generated method stub
    return in.readUTF();
  }

  @Override
  public FSDataInputStream getIn() {
    return in;
  }


}
