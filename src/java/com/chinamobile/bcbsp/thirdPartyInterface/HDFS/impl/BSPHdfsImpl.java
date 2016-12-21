
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.fault.storage.Checkpoint;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPHdfs;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Progressable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * BSPHdfsImpl A concrete class that implements interface BSPHdfs.
 *
 */
public class BSPHdfsImpl implements BSPHdfs {
  /** State Configuration */
  private Configuration conf = null;
  /**State a FileSystem type of variable fs */
  private FileSystem fs = null;
  /**handle log information in checkpoint class*/
  private static final Log LOG = LogFactory.getLog(BSPHdfsImpl.class);

  /**
   * constructor
   */
  public BSPHdfsImpl() {
    conf = new Configuration();
  }

  /**
   * constructor
   * @param b
   *        boolean type variable
   */
  public BSPHdfsImpl(boolean b) {
    conf = new Configuration(b);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void hdfsConf(String corexml) {
    // TODO Auto-generated method stub

    conf.addResource(new Path(corexml));

  }

  @Override
  public String hNhostname() {
    String hostname = conf.get("fs.default.name");
    return hostname;
  }

  @Override
  public InputStream hdfsCheckpoint(String uri, Configuration conf)
      throws IOException {
    // TODO Auto-generated method stub

    fs = FileSystem.get(URI.create(uri), conf);
    InputStream hdfsIn = fs.open(new Path(uri));
    return hdfsIn;

  }

  @Override
  public OutputStream hdfsOperater(String destPath, Configuration conf)
      throws IOException {
    // TODO Auto-generated method stub
    fs = FileSystem.get(URI.create(destPath), conf);
    // OutputStream Out = fs.create(new Path(destPath));
    OutputStream hdfsOut = fs.create(new Path(destPath), new Progressable() {
      public void progress() {

      }
    });

    return hdfsOut;
  }

  @Override
  public FSDataInputStream dwhdfs(String srcfilePath, Configuration conf)
      throws IOException {
    // TODO Auto-generated method stub
    fs = FileSystem.get(URI.create(srcfilePath), conf);
    FSDataInputStream down = fs.open(new Path(srcfilePath));
    return down;

  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    fs.close();
  }

  @Override
  public void deleteHdfs(String hdfsFile, Configuration conf)
      throws IOException {
    // TODO Auto-generated method stub
    fs = FileSystem.get(URI.create(hdfsFile), conf);

  }

  @Override
  public boolean deleteOnExit(String hdfsFile) throws IOException {
    return fs.deleteOnExit(new Path(hdfsFile));

  }

  @Override
  public Path newPath(String hdfsFilePath) {
    // TODO Auto-generated method stub
    Path path = new Path(hdfsFilePath);

    return path;
  }

  @Override
  public FileSystem iSHdfsFileExist(String hdfsFilePath, Configuration conf)
      throws IOException {
    fs = FileSystem.get(URI.create(hdfsFilePath), conf);
    return fs;
  }

  @Override
  public boolean exists(Path path) throws IOException {
    // TODO Auto-generated method stub
	  if(fs == null){
		  LOG.info("Feng nullpointer test! "+fs);
	  }
    boolean b = fs.exists(path);
    return b;

  }

  @Override
  public FileStatus[] globStatus(Path path) throws IOException {
    // TODO Auto-generated method stub
    return fs.globStatus(path);

  }

  @Override
  public ContentSummary getContentSummary(Path path) throws IOException {
    // TODO Auto-generated method stub
    return fs.getContentSummary(path);
  }

  @Override
  public FileSystem hdfscheckOutputSpecs(BSPJob job) throws IOException {
    // TODO Auto-generated method stub
    FileSystem fileSys = FileSystem.get(job.getConf());
    return fileSys;
  }

  @Override
  public Path hdfsinitialize(FileSplit split) {
    // TODO Auto-generated method stub
    final Path file = split.getPath();
    return file;
  }

  @Override
  public Path getWorkingDirectory() throws IOException {
    // TODO Auto-generated method stub
    String name = conf.get(Constants.USER_BC_BSP_JOB_WORKING_DIR);

    if (name != null) {
      return new Path(name);
    } else {
      try {
        Path dir = FileSystem.get(conf).getWorkingDirectory();
        conf.set(Constants.USER_BC_BSP_JOB_WORKING_DIR, dir.toString());
        return dir;
      } catch (IOException e) {
        throw new RuntimeException("get working directory failed",e);
      }
    }
  }

  @Override
  public Path hdfsgetWorkingDirectory(Path dir) throws IOException {
    // TODO Auto-generated method stub

    return new Path(getWorkingDirectory(), dir);
  }

  @Override
  public Path newpath(String p, String c) {
    // TODO Auto-generated method stub
    Path path = new Path(p, c);
    return path;
  }

}
