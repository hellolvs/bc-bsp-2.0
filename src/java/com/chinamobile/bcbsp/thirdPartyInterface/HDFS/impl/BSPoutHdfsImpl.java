
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPoutHdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 *
 * BSPoutHdfsImpl A concrete class that implements interface BSPoutHdfs.
 *
 */
public class BSPoutHdfsImpl implements BSPoutHdfs {
  /** State a FileSystem type of variable fs */
  private FileSystem fs = null;
  /** State a FSDataOutputStream type of variable out */
  private FSDataOutputStream out = null;

  @Override
  public FSDataOutputStream getOut() {
    return out;
  }

  @Override
  public void fsDataOutputStream(Path writePath, Configuration conf)
      throws IOException {
    // TODO Auto-generated method stub
    fs = FileSystem.get(conf);
    out = fs.create(writePath);

  }

  @Override
  public void writeBytes(String s) throws IOException {
    out.writeBytes(s);

  }

  @Override
  public void flush() throws IOException {
    out.flush();

  }

  @Override
  public void close() throws IOException {
    out.close();
  }

}
