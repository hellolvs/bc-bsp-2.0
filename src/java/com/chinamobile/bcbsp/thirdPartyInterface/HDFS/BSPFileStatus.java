
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPFileStatus An interface that encapsulates FileStatus.
 *
 */
public interface BSPFileStatus {

  /**
   * get the file length
   * @return
   *        file length
   */
  long getLen();

  /**
   * get the file
   * @return
   *        file
   */
  FileStatus getFile();

  /**
   * get the file path
   * @return
   *        path
   */
  Path getPath();

  /**
   * Get the block size of the file.
   * @return
   *       the number of bytes
   */
  long getBlockSize();
}
