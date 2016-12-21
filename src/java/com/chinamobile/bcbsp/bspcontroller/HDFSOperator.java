/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chinamobile.bcbsp.bspcontroller;
/*
 * HDFSOperator.java * @author chenchangning 
 * created at 2013-3-26
 * A class for operate hdfs during the HA process
 *  CopyRight by Chinamobile
 */

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFSDataOutputStream;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileSystem;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFSDataOutputStreamImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFileSystemImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

/**
 * A class for operate hdfs during the HA process
 * @author hadoop
 *
 */
public class HDFSOperator {
  /**hdfs configuration*/
  private Configuration conf;
  // private FSDataOutputStream out;
  // private FileSystem fs;
  /**BSP system data output*/
  private BSPFSDataOutputStream bspout;
  /**BSP system file system input and output*/
  private BSPFileSystem bspfs;
  /**handle log file in the class*/
  private static final Log LOG = LogFactory.getLog(HDFSOperator.class);
  /**
   * HDFSOperator construct method
   * @throws IOException
   *         exceptions during initialize BSPfile system.
   */
  public HDFSOperator() throws IOException {
    this.conf = new BSPConfiguration();
    conf.set("fs.hdfs.impl.disable.cache", "true");
    // this.fs = FileSystem.get(URI.create(""), conf);
    this.bspfs = new BSPFileSystemImpl(URI.create(""), conf);
  }
  /**
   * create a file
   * @param uri
   *        create BSP file uri
   * @throws IOException
   *         exceptions during create file
   */
  public void createFile(String uri) throws IOException {
    // synchronized(HDFSOperator.class){
    // this.isFSExist();
    // this.fs = FileSystem.get(URI.create(uri),conf);
    // Path path = new Path(uri);
    // out = fs.create(path);
    // out.flush();
    // out.close();
    bspout = new BSPFSDataOutputStreamImpl(uri, 1, conf);
    bspout.flush();
    bspout.close();
    // fs.close();
    // }
  }

  /**
   * write the file to hdfs
   * @param contents
   *        contents to write into hdfs.
   * @param fileName
   *        hdfs file name.
   * @throws IOException
   *         exceptions during write HDFS
   */
  public void writeFile(String contents, String fileName) throws IOException {
    synchronized (HDFSOperator.class) {
      // if (fs.isFile(new Path(fileName)))
      if (bspfs.isFile(new BSPHdfsImpl().newPath(fileName))) {
        // out = fs.append(new Path(fileName));
        // out.writeUTF(contents);
        // out.flush();
        // out.close();
        bspout = new BSPFSDataOutputStreamImpl(fileName, 0, conf);
        bspout.writeUTF(contents);
        bspout.flush();
        bspout.close();
      } else {
        // out = fs.create(new Path(fileName));
        // out.writeUTF(contents);
        // out.flush();
        // out.close();
        bspout = new BSPFSDataOutputStreamImpl(fileName, 1, conf);
        bspout.writeUTF(contents);
        bspout.flush();
        bspout.close();
      }
    }
  }

  /**
   * read file from hdfs
   * @param file
   *        file name need to read from hdfs
   * @return
   *        file read from hdfs.
   * @throws IOException
   *         exceptions during open hdfs.
   */
  public FSDataInputStream readFile(String file) throws IOException {
    FSDataInputStream in;
    synchronized (HDFSOperator.class) {
      // if (fs.isFile(new Path(file)))
      if (bspfs.isFile(new BSPHdfsImpl().newPath(file))) {
        // Path path = new Path(file);
        // in = fs.open(path);
        in = bspfs.open(new BSPHdfsImpl().newPath(file));
      } else {
        in = null;
      }
    }
    return in;
  }

  /**
   * delete the file from hdfs
   * @param file
   *        file name that need to delete
   * @throws IOException
   *         exceptions during handle hdfs
   */
  public void deleteFile(String file) throws IOException {
    // this.fs = FileSystem.get(URI.create(file),conf);
    // this.isFSExist();
    // Path path = new Path(file);
    // fs.delete(path, true);
    bspfs.delete(new BSPHdfsImpl().newPath(file), true);
    // fs.close();
  }

  /**
   * list all directory of th dir
   * @param dir
   *        file dircetory path
   * @return
   *        directory list
   * @throws IOException
   *         exceptions during handle BSP file
   */
  public FileStatus[] listDirAll(String dir) throws IOException {
    // Path path = new Path(dir);
    // FileStatus[] status = fs.listStatus(path);
    for (FileStatus s : bspfs.listStatus(new BSPHdfsImpl().newPath(dir))) {
      System.out.println(s.getPath());
    }
    return bspfs.listStatus(new BSPHdfsImpl().newPath(dir));
  }

  /**
   * tell if the file exists
   * @param fileName
   *        file name to tell
   * @return
   *        if the file exists
   * @throws IOException
   *         exceptions during handle BSP file.
   */
  public boolean isExist(String fileName) throws IOException {
    // this.isFSExist();
    // this.fs = FileSystem.get(URI.create(fileName),conf);
    // Path path = new Path(fileName);
    // boolean isExists = fs.exists(path);
    boolean isExists = bspfs.exists(new BSPHdfsImpl().newPath(fileName));
    // fs.close();
    return isExists;
  }

  /**
   * serialize the WorkerManager Status
   * @param uri
   *        BSPfile output uri
   * @param wmlist
   *        workerManager list
   * @param staffsLoadFactor
   *        for load balancing
   * @throws IOException
   *        exceptions during handle BSPfile.
   */
  public void serializeWorkerManagerStatus(String uri,
      Collection<WorkerManagerStatus> wmlist, double staffsLoadFactor)
      throws IOException {
    // synchronized(HDFSOperator.class){
    // this.fs = FileSystem.get(URI.create(uri),conf);
    // this.isFSExist();
    Double loadfactor = staffsLoadFactor;
    // Path path = new Path(uri);
    // out = fs.create(path);
    bspout = new BSPFSDataOutputStreamImpl(uri, 1, conf);
    Text factor = new Text(loadfactor.toString());
    // factor.write(out);
    // for (WorkerManagerStatus wmStatus : wmlist) {
    // wmStatus.write(out);
    // }
    // out.flush();
    // out.close();
    factor.write(bspout.getOut());
    for (WorkerManagerStatus wmStatus : wmlist) {
      wmStatus.write(bspout.getOut());
    }
    bspout.flush();
    bspout.close();
    // fs.close();
    // }
  }
  // public void isFSExist() throws IOException{
  // try{
  // fs.exists(new Path("/user"));
  // }catch(IOException e){
  // this.fs = FileSystem.get(URI.create(""),conf);
  // }
  // }
  // public void closeFs() throws IOException{
  // this.fs.close();
  // }
}
