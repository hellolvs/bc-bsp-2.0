
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFSDataOutputStream;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 *
 * BSPFSDataOutputStreamImpl
 *  A concrete class that implements interface BSPFSDataOutputStream.
 *
 */
public class BSPFSDataOutputStreamImpl implements BSPFSDataOutputStream {

  /**State a FSDataOutputStream type of variable out */
  private FSDataOutputStream out = null;
  /**State a FileSystem type of variable fs */
  private FileSystem fs = null;


  /**
   * constructor
   * @param fileName
   *        the given filename
   * @param i
   *        flag bit
   * @param conf
   *        Configuration
   * @throws IOException
   */
  public BSPFSDataOutputStreamImpl(String fileName, int i, Configuration conf)
      throws IOException {
    this.fs = FileSystem.get(URI.create(""), conf);
    if (i == 0) {

      out = fs.append(new Path(fileName));
    } else {
      out = fs.create(new Path(fileName));
    }
  }

  /**
   * constructor
   * @param fs
   *        file system handle
   * @param filename
   *        the name of the file to be created
   * @param permission
   *        the permission of the file
   * @throws IOException
   */
  public BSPFSDataOutputStreamImpl(FileSystem fs, Path filename,
      FsPermission permission) throws IOException {
    out = FileSystem.create(fs, filename, permission);
  }

  @Override
  public FSDataOutputStream getOut() {

    return out;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    out.close();
  }

  @Override
  public void flush() throws IOException {
    // TODO Auto-generated method stub
    out.flush();

  }

  @Override
  public void writeUTF(String contents) throws IOException {
    // TODO Auto-generated method stub
    out.writeUTF(contents);

  }

  @Override
  public void write(byte[] b) throws IOException {
    // TODO Auto-generated method stub

    out.write(b);

  }

}
