
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import org.apache.hadoop.fs.FSDataOutputStream;

/**
 *
 * BSPhdfsRW An interface that encapsulates FSDataOutputStream.
 *
 */
public interface BSPhdfsRW {

  /**
   * getter method
   * @return
   *       FSDataOutputStream
   */
  FSDataOutputStream getFileOut();
}
