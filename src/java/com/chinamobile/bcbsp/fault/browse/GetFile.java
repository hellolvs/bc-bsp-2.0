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

package com.chinamobile.bcbsp.fault.browse;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.chinamobile.bcbsp.fault.storage.DirRecord;
import com.chinamobile.bcbsp.fault.tools.GetRecord;
import com.chinamobile.bcbsp.fault.tools.HdfsOperater;
import com.chinamobile.bcbsp.fault.tools.Zip;

/**
 * get some specified directories;
 */
public class GetFile {
  /**localRecordPath string*/
  private String localRecordPath = null;
  /**record on hdfs path*/
  private String hdfsRecordPath = null;
  /**directory record*/
  private DirRecord dr = null;
  /**get directory record*/
  private GetRecord gr = null;
  /**HDFS local fault log dir path*/
  private String hdfsLocalDir = "/bsp/temp/faultLog";

  /**
   * get the local file and hdfs file
   * @param localRecordPath
   *        local record path
   * @param hdfsRecordPath
   *        hdfs record path
   */
  public GetFile(String localRecordPath, String hdfsRecordPath) {
    super();
    this.localRecordPath = localRecordPath;
    this.hdfsRecordPath = hdfsRecordPath;
    this.gr = new GetRecord(this.localRecordPath, this.hdfsRecordPath);
    this.dr = gr.getRecord();
  }

  /**
   * get file from local file or distributed file
   * @param n
   *        file num to get.
   * @return file from from local file or hdfs.
   */
  public List<String> getFile(int n) {
    List<String> monthDirs = new ArrayList<String>();
    if (!dr.isCopyFlag()) {
      if (n < dr.getLocalFileNum()) {
        boolean flag = true;
        int beginIndex = (dr.getIndexl() - 1 + dr.getLocalFileNum()) %
            dr.getLocalFileNum();
        for (int i = 0; i < n; i++) {
          if (dr.getLocalFile(beginIndex) != null && // record has the path ,but
                                                  // local did not have the file
             !dr.getLocalFile(beginIndex).exists()) {
            flag = false;
          }
          beginIndex = (beginIndex - 1 + dr.getLocalFileNum()) %
              dr.getLocalFileNum();
        }
        if (flag) {
          monthDirs = getLocalFile(n);
        } else {
          monthDirs = getDistributeFile(1, n);
        }
        return monthDirs;
      } else {
        boolean flag = true;
        int beginIndex = (dr.getIndexl() - 1 + dr.getLocalFileNum()) %
            dr.getLocalFileNum();
        for (int i = 1; i < dr.getLocalFileNum(); i++) {
          if (dr.getLocalFile(beginIndex) != null &&
              !dr.getLocalFile(beginIndex).exists()) {
            flag = false;
          }
          beginIndex = (beginIndex - 1 + dr.getLocalFileNum()) %
              dr.getLocalFileNum();
        }
        if (flag) {
          monthDirs = getLocalFile(dr.getLocalFileNum() - 1);
          monthDirs.addAll(getDistributeFile((dr.getLocalFileNum() - 1), n -
              (dr.getLocalFileNum() - 1)));
          return monthDirs;
        } else {
          monthDirs = getDistributeFile(1, n);
          return monthDirs;
        }
      }
    } else { // the dirRecord is the copy from hdfs;
      if (n < dr.getLocalFileNum() - 1) {
        boolean flag = true;
        int beginIndex = (dr.getIndexl() - 1 + dr.getLocalFileNum()) %
           dr.getLocalFileNum();
        for (int i = 0; i < n; i++) {
          if (dr.getLocalFile(beginIndex) != null && // record has the path ,but
                                                  // local did not have the file
              !dr.getLocalFile(beginIndex).exists()) {
            flag = false;
          }
          beginIndex = (beginIndex - 1 + dr.getLocalFileNum()) %
              dr.getLocalFileNum();
        }
        if (flag) {
          monthDirs = getLocalFile(n);
        } else {
          monthDirs = getDistributeFile(1, n);
        }
        return monthDirs;
      } else { // n>=( dr.getLocalFileNum()-1) n>3;
        boolean flag = true;
        int beginIndex = (dr.getIndexl() - 1 + dr.getLocalFileNum()) %
            dr.getLocalFileNum();
        for (int i = 1; i < dr.getLocalFileNum() - 1; i++) {
          if (dr.getLocalFile(beginIndex) != null &&
              !dr.getLocalFile(beginIndex).exists()) {
            flag = false;
          }
          beginIndex = (beginIndex - 1 + dr.getLocalFileNum()) %
              dr.getLocalFileNum();
        }
        if (flag) {
          monthDirs = getLocalFile(dr.getLocalFileNum() - 2);
          monthDirs.addAll(getDistributeFile((dr.getLocalFileNum() - 1), n -
              (dr.getLocalFileNum() - 2)));
          return monthDirs;
        } else {
          monthDirs = getDistributeFile(1, n);
          return monthDirs;
        }
      }
    }
  }

  /**
   * get file from local.
   * @param n
   *        file num to get.
   * @return local file has been got.
   */
  private List<String> getLocalFile(int n) {
    List<String> monthDirs = new ArrayList<String>();
    int index = (dr.getIndexl() - 1 + dr.getLocalFileNum()) %
       dr.getLocalFileNum();
    for (int i = 0; i < n; i++) {
      if (dr.getLocalFile(index) != null) {
        monthDirs.add(dr.getLocalFile(index).getAbsolutePath());
        index = (index - 1 + dr.getLocalFileNum()) % dr.getLocalFileNum();
      }
    }
    return monthDirs;
  }

  /**
   * get the directory path from hdfs;
   * @param begin
   *        get the file from hdfs start offset.
   * @param DirNum
   *        total num to get from hdfs.
   * @return hdfs file has been got.
   */
  private List<String> getDistributeFile(int begin, int DirNum) {
    List<String> monthDirs = new ArrayList<String>();
    String hdfsZipDirPath = null;
    List<String> zipPathlist = new ArrayList<String>();
    if (DirNum > dr.getHdfsFileNum() - 1 - (begin - 1)) {
      DirNum = (dr.getHdfsFileNum() - 1) -
          (begin - 1); // hdfs can give the most
                                                       // file num
    }
    int indexstart = (dr.getIndexh() - begin + dr.getHdfsFileNum()) %
        dr.getHdfsFileNum();
    for (int i = 0; i < DirNum; i++) {
      if (dr.getHdfsFile(indexstart) != null) {
        hdfsZipDirPath = dr.getHdfsFile(indexstart);
        indexstart = (indexstart - 1 + dr.getHdfsFileNum()) %
            dr.getHdfsFileNum();
        if (hdfsZipDirPath != null) {
          String destDir = hdfsLocalDir +
              hdfsZipDirPath.substring(
                  hdfsZipDirPath.lastIndexOf('/',
                      hdfsZipDirPath.lastIndexOf('/') - 1),
                  hdfsZipDirPath.length());
          HdfsOperater.downloadHdfs(hdfsZipDirPath, destDir);
          zipPathlist.add(destDir);
        }
      }
    }
    // compress each month's monthdir.zip in local disk
    for (String zipPath : zipPathlist) {
      Zip.decompress(zipPath);
      File zipFile = new File(zipPath);
      monthDirs.add(zipFile.getParent() +
          File.separator +
          zipFile.getName().substring(0,
              zipFile.getName().lastIndexOf(".zip")));
    }
    return monthDirs;
  }

  /**
   * delete the file dir on hdfs.
   */
  public void deletehdfsDir() {
    del(getHdfsLocalDir());
  }

  /**
   * delete specific file depend on filepath.
   * @param filepath
   *        path of the file need delete
   */
  public void del(String filepath) {
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
    }
    f.delete();
  }

  /**
   * get the dir of file on hdfs.
   * @return
   *        hdfs file dir
   */
  public String getHdfsLocalDir() {
    File tempDir = new File(hdfsLocalDir);
    return tempDir.getParentFile().getAbsolutePath();
  }

  /**
   * set the hdfs local dir.
   * @param hdfsLocalDir
   *        hdfs local dir to be set.
   */
  public void setHdfsLocalDir(String hdfsLocalDir) {
    this.hdfsLocalDir = hdfsLocalDir;
  }

  /**
   * clone the file dir.
   * @return cloned dir.
   * @throws CloneNotSupportedException incase the dir doesn't support clone.
   */
  public DirRecord getDirRecord() throws CloneNotSupportedException {
    return dr.clone();
  }
}
