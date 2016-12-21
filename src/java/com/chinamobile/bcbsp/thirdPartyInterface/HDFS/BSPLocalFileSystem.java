
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 *
 * BSPLocalFileSystem An interface that encapsulates LocalFileSystem.
 *
 */
public interface BSPLocalFileSystem {

  /**
   *  Implement the delete(Path, boolean) in checksum file system.
   * @param f
   *       the path to delete.
   * @param b
   *       if path is a directory and set to true,
   *       the directory is deleted else throws an exception.
   *       In case of a file the recursive can be set to either true or false.
   * @return
   *        true if delete is successful else false.
   * @throws IOException
   */
  boolean delete(Path f, boolean b) throws IOException;

  /**
   * Check if exists.
   * @param f
   *        path
   * @return
   *       true or false
   * @throws IOException
   */
  boolean exists(Path f) throws IOException;

}
