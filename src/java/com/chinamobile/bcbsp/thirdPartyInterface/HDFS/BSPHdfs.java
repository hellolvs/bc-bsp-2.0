
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * BSPHdfs An interface that encapsulates fs
 *
 */
public interface BSPHdfs {

  /**
   * Add a configuration resource.
   * @param corexml
   *        string
   */
  void hdfsConf(String corexml);

  /**
   * Get the value of the name property, null if no such property exists.
   * @return
   *        hostname
   */
  String hNhostname();

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param uri
   *        URI
   * @param conf
   *        Configuration
   * @return
   *        InputStream
   * @throws IOException
   */
  InputStream hdfsCheckpoint(String uri, Configuration conf)
      throws IOException;

  /**
   * getter method
   * @return
   *        Configuration
   */
  Configuration getConf();

  /**
   *  Create an FSDataOutputStream at the indicated Path
   *  with write-progress reporting.
   * @param destPath
   *        path
   * @param conf
   *        Configuration
   * @return
   *        FSDataOutputStream
   * @throws IOException
   */
  OutputStream hdfsOperater(String destPath, Configuration conf)
      throws IOException;

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param srcfilePath
   *        source file path
   * @param conf
   *        Configuration
   * @return
   *        FSDataInputStream
   * @throws IOException
   */
  FSDataInputStream dwhdfs(String srcfilePath, Configuration conf)
      throws IOException;

  /**
   * No more filesystem operations are needed.
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Returns the FileSystem for this URI's scheme and authority.
   * @param hdfsFile
   *        string
   * @param conf
   *        Configuration
   * @throws IOException
   */
  void deleteHdfs(String hdfsFile, Configuration conf)
      throws IOException;

  /**
   *  Mark a path to be deleted when FileSystem is closed.
   * @param hdfsFile
   *        the path to delete.
   * @return
   *        true if deleteOnExit is successful, otherwise false.
   * @throws IOException
   */
  boolean deleteOnExit(String hdfsFile) throws IOException;

  /**
   * Construct a path from a String.
   * @param hdfsFilePath
   *        string
   * @return
   *        path
   */
  Path newPath(String hdfsFilePath);

  /**
   * Returns the FileSystem for this URI's scheme and authority.
   * @param hdfsFilePath
   *        string
   * @param conf
   *        Configuration
   * @return
   *        fs
   * @throws IOException
   */
  FileSystem iSHdfsFileExist(String hdfsFilePath, Configuration conf)
      throws IOException;

  /**
   * Check if exists.
   * @param path
   *        source file
   * @return
   *        true or false
   * @throws IOException
   */
  boolean exists(Path path) throws IOException;

  /**
   * Return all the files that match filePattern and are not checksum files.
   * @param path
   *        a regular expression specifying a pth pattern
   * @return
   *        an array of paths that match the path pattern
   * @throws IOException
   */
  FileStatus[] globStatus(Path path) throws IOException;

  /**
   * Return the ContentSummary of a given Path.
   * @param path
   *        a given Path
   * @return
   *        ContentSummary
   * @throws IOException
   */
  ContentSummary getContentSummary(Path path) throws IOException;

  /**
   *  Returns the configured filesystem implementation.
   * @param job
   *        BSPJob
   * @return
   *        FileSystem
   * @throws IOException
   */
  FileSystem hdfscheckOutputSpecs(BSPJob job) throws IOException;

  /**
   * The file containing this split's data.
   * @param split
   *         FileSplit
   * @return
   *        path
   */
  Path hdfsinitialize(FileSplit split);

  /**
   * get the working directory
   * @return
   *     the working directory
   * @throws IOException
   */
  Path getWorkingDirectory() throws IOException;

  /**
   * Resolve a child path against a parent path.
   * @param dir
   *        a child path
   * @return
   *        path
   * @throws IOException
   */
  Path hdfsgetWorkingDirectory(Path dir) throws IOException;

  /**
   * Resolve a child path against a parent path.
   * @param p
   *        a parent path
   * @param c
   *        a child path
   * @return
   *        path
   */
  Path newpath(String p, String c);

}
