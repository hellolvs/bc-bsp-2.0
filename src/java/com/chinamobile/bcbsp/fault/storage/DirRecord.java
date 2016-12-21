/**
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

package com.chinamobile.bcbsp.fault.storage;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.mortbay.log.Log;

/**
 * record the filePath in local and hdfs
 */
@SuppressWarnings("serial")
public class DirRecord implements Serializable, Cloneable {
  /**hdfs file num*/
  private int hdfsFileNum = 13;
  /**local file num*/
  private int localFileNum = 5;
  /**local file index length*/
  private int indexl = 0;
  /**hdfs file index length*/
  private int indexh = 0;
  /**hdfs file string list*/
  private String[] hdfsfilelists = new String[hdfsFileNum];
  /**local file list*/
  private File[] localfilelists = new File[localFileNum];
  /**file copy flag*/
  boolean copyFlag = false;
  // ---------------------localfile=--------------------------------------
  /**
   * record file path in DirRecord return last month directory position in order
   * to compress
   * @param file
   *        file path to record
   * @return
   *        last month directory position
   */
  public int pushLocalFile(File file) {
    localfilelists[indexl] = file;
    indexl = (indexl + 1) % localFileNum;
    deleteLocalFile(indexl);
    return ((indexl - 2) + localFileNum) % localFileNum;
  }

  /**
   * get local file list
   * @param indexl
   *        file list length
   * @return
   *        local file list.
   */
  public File getLocalFile(int indexl) {
    return localfilelists[indexl];
  }

  /**
   * delete local file.
   * @param indexl
   *        local file list length.
   */
  public void deleteLocalFile(int indexl) {
    if (localfilelists[indexl] == null) {
      return;
    } else if (localfilelists[indexl].exists()) {
      try {
        File localParentFile = localfilelists[indexl].getParentFile();
        del(localfilelists[indexl].getAbsolutePath());
        Log.info("localParentFile == null" + (localParentFile == null));
        if (localParentFile.listFiles().length == 0) { // delete year
          del(localParentFile.getAbsolutePath());
        }
      } catch (IOException e) {
        throw new RuntimeException("[deleteLocalFile] ", e);
        //e.printStackTrace();
      }
      localfilelists[indexl] = null;
    }
  }

  // --------------HDFS--------------------------------

  /**
   * return overdue directory and delete it from hdfs
   * @param filepath
   *        hdfsfile path
   * @return file list length
   */
  public int pushHdfsFile(String filepath) {
    hdfsfilelists[indexh] = filepath;
    indexh = (indexh + 1) % hdfsFileNum;
    return indexh;
  }

  /**
   * get hdfs file list.
   * @param indexh
   *        hdfs file length
   * @return hdfs file list.
   */
  public String getHdfsFile(int indexh) {
    return hdfsfilelists[indexh];
  }

  /**
   * delete hdfs file.
   * @param indexh
   *        hdfs file length.
   */
  public void deleteHdfsFile(int indexh) {
    if (hdfsfilelists[indexh] != null) {
      hdfsfilelists[indexh] = null;
    }
  }

  /**
   * get file list length
   * @return list length
   */
  public int getIndexl() {
    return indexl;
  }

  /**
   * set  local file list length
   * @param indexl
   *        list length to be set.
   */
  public void setIndexl(int indexl) {
    this.indexl = indexl;
  }

  /**
   * get local file list length
   * @return file list length
   */
  public int getIndexh() {
    return indexh;
  }

  /**
   * set hdfs file list length
   * @param indexh
   *        list length to be set.
   */
  public void setIndexh(int indexh) {
    this.indexh = indexh;
  }

  /**
   * get the local file num.
   * @return
   *        local file num.
   */
  public int getLocalFileNum() {
    return localFileNum;
  }

  /**
   * set local file num
   * @param localFileNum
   *        local file num to be set.
   */
  public void setLocalFileNum(int localFileNum) {
    this.localFileNum = localFileNum;
  }

  /**
   * get the hdfs file num
   * @return
   *        hdfs file num.
   */
  public int getHdfsFileNum() {
    return hdfsFileNum;
  }

  /**
   * set hdfs file num
   * @param hdfsFileNum
   *        hdfs file num to be set.
   */
  public void setHdfsFileNum(int hdfsFileNum) {
    this.hdfsFileNum = hdfsFileNum;
  }

  /**
   * delete file.
   * @param filepath
   *        need deleting file path
   * @throws IOException
   *         exceptions during delete the file.
   */
  public void del(String filepath) throws IOException {
    File f = new File(filepath);
    if (f.exists() && f.isDirectory()) {
      if (f.listFiles().length == 0) {
        f.delete();
      } else {
        File[] delFile = f.listFiles();
        int i = f.listFiles().length;
        for (int j = 0; j < i; j++) {
          if (delFile[j].isDirectory()) {
            del(delFile[j].getAbsolutePath());
          }
          delFile[j].delete();
        }
      }
      f.delete();
    }
  }

  /**
   * clone the dirRecord.
   * @return cloned dirrecord.
   * @throws CloneNotSupportedException
   *         incaset the dirrecord doesn't support clone.
   */
  public DirRecord clone() throws CloneNotSupportedException {
    DirRecord dr;
    dr = (DirRecord) super.clone();
    dr.hdfsfilelists = this.hdfsfilelists.clone();
    dr.localfilelists = this.localfilelists.clone();
    return dr;
  }

  /**
   * if the DirRecord is copied
   * @return record copyFlag
   */
  public boolean isCopyFlag() {
    return copyFlag;
  }

  /**
   * set the record copyflag
   * @param copyFlag
   *        copyflag to be set.
   */
  public void setCopyFlag(boolean copyFlag) {
    this.copyFlag = copyFlag;
  }
}
