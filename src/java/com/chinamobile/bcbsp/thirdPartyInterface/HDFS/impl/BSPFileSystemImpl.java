
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileSystem;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 *
 * BSPFileSystemImpl A concrete class that implements interface BSPFileSystem.
 *
 */
public class BSPFileSystemImpl implements BSPFileSystem {
  /** State BSPController*/
  private final BSPController controller;
  /** State a FileSystem type of variable fs */
  private FileSystem fs;
  /** State a path */
  private Path systemDirectory = null;

  /**
   * constructor
   * @param uri
   *        URI
   * @param conf
   *        Configuration
   * @throws IOException
   */
  public BSPFileSystemImpl(URI uri, Configuration conf) throws IOException {
    this.controller = null;
    fs = FileSystem.get(uri, conf);

  }

  /**
   * constructor
   * @param controller
   *        BSPController
   * @param jobId
   *        BSPJobID
   * @param conf
   *        Configuration
   * @throws IOException
   */
  public BSPFileSystemImpl(BSPController controller, BSPJobID jobId,
      Configuration conf) throws IOException {
    this.controller = controller;
    Path jobDir = controller.getSystemDirectoryForJob(jobId);
    fs = jobDir.getFileSystem(conf);
  }

  /**
   * constructor
   * @param controller
   *        BSPController
   * @param conf
   *        Configuration
   * @throws IOException
   */
  public BSPFileSystemImpl(BSPController controller, Configuration conf)
      throws IOException {
    this.controller = controller;
    Path sysDir = new Path(this.controller.getSystemDir());
    fs = sysDir.getFileSystem(conf);
  }

  /**
   * constructor
   * @param conf
   *        Configuration
   * @throws IOException
   */
  public BSPFileSystemImpl(Configuration conf) throws IOException {
    this.controller = null;
    fs = FileSystem.get(conf);
  }

  /**
   * constructor
   * @param dir
   *        String
   * @param conf
   *        Configuration
   * @throws IOException
   */
  public BSPFileSystemImpl(String dir, Configuration conf) throws IOException {
    this.controller = null;
    systemDirectory = new Path(dir);
    fs = systemDirectory.getFileSystem(conf);

  }

  /**
   * constructor
   * @param file
   *        FileStatus
   * @param job
   *        BSPJob
   * @throws IOException
   */
  public BSPFileSystemImpl(FileStatus file, BSPJob job) throws IOException {
    this.controller = null;
    Path path = file.getPath();
    fs = path.getFileSystem(job.getConf());

  }

  @Override
  public FileSystem getFs() {
    return fs;
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    // TODO Auto-generated method stub
    return fs.create(f);

  }

  @Override
  public boolean isFile(Path p) throws IOException {
    // TODO Auto-generated method stub
    return fs.isFile(p);
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    // TODO Auto-generated method stub
    return fs.append(f);
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    // TODO Auto-generated method stub
    return fs.open(f);
  }

  @Override
  public boolean delete(Path f, boolean b) throws IOException {
    // TODO Auto-generated method stub
    return fs.delete(f, b);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    // TODO Auto-generated method stub
    return fs.listStatus(f);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    // TODO Auto-generated method stub
    return fs.exists(f);
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    fs.close();
  }

  @Override
  public void copyToLocalFile(Path s, Path d) throws IOException {
    // TODO Auto-generated method stub
    fs.copyToLocalFile(s, d);

  }

  @Override
  public void setWorkingDirectory(Path f) {
    // TODO Auto-generated method stub
    fs.setWorkingDirectory(f);
  }

  /**
   * create a directory with the provided permission.
   * @param fs
   *        file system handle
   * @param dir
   *        the name of the directory to be created
   * @param permission
   *        the permission of the directory
   * @return
   *        true if the directory creation succeeds; false otherwise
   * @throws IOException
   */
  public static boolean mkdirs(FileSystem fs, Path dir, FsPermission permission)
      throws IOException {
    // TODO Auto-generated method stub
    return FileSystem.mkdirs(fs, dir, permission);
  }

  @Override
  public URI getUri() {
    // TODO Auto-generated method stub
    return fs.getUri();
  }

  @Override
  public Path makeQualified(Path path) {
    // TODO Auto-generated method stub
    return fs.makeQualified(path);
  }

}
