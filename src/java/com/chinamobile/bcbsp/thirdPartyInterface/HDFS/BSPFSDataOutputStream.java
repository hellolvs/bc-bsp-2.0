
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

/**
 *
 * BSPFSDataOutputStream An interface that encapsulates FSDataOutputStream.
 *
 */
public interface BSPFSDataOutputStream {

  /**
   * A method that encapsulates flush().
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * A method that encapsulates close().
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * A method that encapsulates writeUTF(String contents).
   * @param contents
   *        string
   * @throws IOException
   */
  void writeUTF(String contents) throws IOException;

  /**
   * getter method
   * @return
   *        FSDataOutputStream
   */
  FSDataOutputStream getOut();

  /**
   *  A method that encapsulates write(byte[] b).
   * @param b
   *        byte array
   * @throws IOException
   */
  void write(byte[] b) throws IOException;
}
