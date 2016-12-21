
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 *
 * BSPhdfsFSDIS An interface that encapsulates FSDataInputStream.
 *
 */
public interface BSPhdfsFSDIS {

  /**
   * Seek to the given offset from the start of the file.
   * @param s
   *        the given offset
   * @throws IOException
   */
  void seek(long s) throws IOException;

  /**
   * getter method
   * @return
   *        FSDataInputStream
   */
  FSDataInputStream getFileIn();

}
