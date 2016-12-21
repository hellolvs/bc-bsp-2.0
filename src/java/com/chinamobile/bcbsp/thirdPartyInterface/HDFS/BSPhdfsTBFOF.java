
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import org.apache.hadoop.fs.FSDataOutputStream;

/**
 *
 * BSPhdfsTBFOF An interface that encapsulates FSDataOutputStream
 *
 */
public interface BSPhdfsTBFOF {

  /**
   * getter method
   * @return
   *       FSDataOutputStream
   */
  FSDataOutputStream getFileOut();
}
