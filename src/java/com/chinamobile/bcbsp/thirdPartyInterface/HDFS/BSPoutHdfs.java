
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPoutHdfs An interface that encapsulates FSDataOutputStream.
 *
 */
public interface BSPoutHdfs {

  /**
   * Opens an FSDataOutputStream at the indicated Path.
   * @param writePath
   *        the indicated Path
   * @param conf
   *        Configuration
   * @throws IOException
   */
  void fsDataOutputStream(Path writePath, Configuration conf)
      throws IOException;

  /**
   * getter method
   * @return
   *       FSDataOutputStream
   */
  FSDataOutputStream getOut();

  /**
   * A method that encapsulates writeBytes(String s).
   * @param s
   *        String
   * @throws IOException
   */
  void writeBytes(String s) throws IOException;

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

}
