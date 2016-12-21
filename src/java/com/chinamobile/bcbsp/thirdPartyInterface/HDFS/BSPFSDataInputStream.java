
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 *
 * BSPFSDataInputStream An interface that encapsulates FSDataInputStream.
 *
 */
public interface BSPFSDataInputStream {

  /**
   * read byte array
   * @param b
   *        byte array
   * @return
   *       int
   * @throws IOException
   */
  int read(byte[] b) throws IOException;

  /**
   * close a filter input Stream
   * @throws IOException
   */
  void hdfsInStreamclose() throws IOException;

  /**
   * read data input stream
   * @return
   *        string
   * @throws IOException
   */
  String readUTF() throws IOException;

  /**
   * getter method
   * @return
   *        in
   */
  FSDataInputStream getIn();
}
