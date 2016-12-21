
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPFileSystem An interface that encapsulates FileSystem.
 *
 */
public interface BSPFileSystem {

  /**
   *  Opens an FSDataOutputStream at the indicated Path.
   * @param f
   *       the file to create
   * @return
   *        Files are overwritten by default.
   * @throws IOException
   */
  FSDataOutputStream create(Path f) throws IOException;

  /**
   * True if the named path is a regular file.
   * @param p
   *        the file path
   * @return
   *        true or false
   * @throws IOException
   */
  boolean isFile(Path p) throws IOException;

  /**
   *  Append to an existing file (optional operation).
   * @param f
   *        the existing file to be appended.
   * @return
   *        FSDataOutputStream
   * @throws IOException
   */
  FSDataOutputStream append(Path f) throws IOException;

  /**
   *  Opens an FSDataInputStream at the indicated Path.
   * @param f
   *        the file to open
   * @return
   *        FSDataInputStream
   * @throws IOException
   */
  FSDataInputStream open(Path f) throws IOException;

  /**
   * Delete a file.
   * @param f
   *        the path to delete.
   * @param b
   *        if path is a directory and set to true,
   *        the directory is deleted else throws an exception.
   * @return
   *        true or false
   * @throws IOException
   */
  boolean delete(Path f, boolean b) throws IOException;

  /**
   * list the Status
   * @param f
   *        path
   * @return
   *        FileStatus[]
   * @throws IOException
   */
  FileStatus[] listStatus(Path f) throws IOException;

  /**
   * Check if exists.
   * @param f
   *        source file
   * @return
   *        true or false
   * @throws IOException
   */
  boolean exists(Path f) throws IOException;

  /**
   * No more filesystem operations are needed. Will release any held locks.
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * The s file is under FS, and the d is on the local disk.
   * Copy it from FS control to the local d name.
   * @param s
   *        source file
   * @param d
   *        destination file
   * @throws IOException
   */
  void copyToLocalFile(Path s, Path d) throws IOException;

  /**
   * Set the current working directory for the given file system.
   * All relative paths will be resolved relative to it.
   * @param f
   *        path
   */
  void setWorkingDirectory(Path f);

  /**
   * getter method
   * @return
   *       fs
   */
  FileSystem getFs();

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   * @return
   *        a URI
   */
  URI getUri();

  /**
   * Make sure that a path specifies a FileSystem.
   * @param path
   *        the given path
   * @return
   *        path
   */
  Path makeQualified(Path path);

}
