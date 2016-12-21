
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPPath An interface that encapsulates Path.
 *
 */
public interface BSPPath {

  /**
   * getter method
   * @return
   *       SystemDir
   */
  Path getSystemDir();

  /**
   * Returns a qualified path object.
   * @param fs
   *       FileSystem
   * @return
   *       a qualified path object
   */
  Path makeQualified(FileSystem fs);

}
