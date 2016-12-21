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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.fault.tools.GetRecord;
import com.chinamobile.bcbsp.fault.tools.HdfsOperater;
import com.chinamobile.bcbsp.fault.tools.Zip;

/**
 * manage the system fault log.
 * @author hadoop
 *
 */
public class ManageFaultLog {
  /**DirRecord handle*/
  private DirRecord dr = null;
  /**record path*/
  private String recordPath = null;
  /**GetRecord handle*/
  private GetRecord gr = null;
  /**record file*/
  private static String recordFile = "DirRecord.bak";
  /*
   * record the file lists in the hdfs and local disk,that can manage easily;
   */
  /**hdfs namenode hostname*/
  private String hdfsNamenodehostName = null;
  /**hdfs faultlog path*/
  private String hdfsDirPath = "/user/faultlog/";
  /**ObjectOutputStream handle*/
  public ObjectOutputStream oos = null;
  /**handle log in MangeFaultlog class*/
  public static final Log LOG = LogFactory.getLog(ManageFaultLog.class);

  /**
   * @param hdfsNamenodehostName
   *        hdfs namenode hostname
   * @param hdfsDirPath
   *        hdfsDirPath equals localDirPath;
   */
  public ManageFaultLog(String hdfsNamenodehostName, String hdfsDirPath) {
    super();
    this.hdfsNamenodehostName = hdfsNamenodehostName;
    if (!hdfsDirPath.substring(hdfsDirPath.length() - 1).equals("/") &&
        !hdfsDirPath.substring(hdfsDirPath.length() - 2).equals("\\")) {
      hdfsDirPath = hdfsDirPath + File.separator;
    }
    this.hdfsDirPath = hdfsDirPath;
    recordPath = this.hdfsDirPath + recordFile;
    gr = new GetRecord(recordPath, this.hdfsNamenodehostName + recordPath);
    dr = gr.getRecord();
    if (dr == null) {
      dr = new DirRecord();
    }
  }

  /**
   * each time the new directory is created ,the method is called . record the
   * file and compress the front file. local disk store 3 month data files ,and
   * the other store in the hdfs that save 12 month data file;
   * @param file
   *        file to record.
   */
  public void record(File file) {
    int compressindex = 0;
    File previousFile = dr.getLocalFile(((dr.getIndexl() - 1) + dr
        .getLocalFileNum()) % dr.getLocalFileNum());
    /*prevent from duplicate write the same Dir in the record;*/
    if (previousFile != null && previousFile.equals(file)) {
      return;
    }
    compressindex = dr.pushLocalFile(file);
    if (dr.isCopyFlag()) {
      dr.setCopyFlag(false);
      saveRecord();
      return;
    }
    if (dr.getLocalFile(compressindex) != null) {
      compress(dr.getLocalFile(compressindex));
    }
    saveRecord();
  }

  /**
   * save the fault record.
   */
  public void saveRecord() {
    File recordfile = new File(recordPath);
    try {
      if (!recordfile.getParentFile().exists()) {
        recordfile.getParentFile().mkdirs();
      }
      oos = new ObjectOutputStream(new FileOutputStream(recordPath));
      oos.writeObject(dr); // write the dr into recordPath;
      oos.close();
    } catch (FileNotFoundException e) {
      //LOG.error("[saveRecord]", e);
      throw new RuntimeException("[saveRecord]", e);
    } catch (IOException e) {
      //LOG.error("[saveRecord]", e);
      throw new RuntimeException("[saveRecord]", e);
    }
    HdfsOperater.uploadHdfs(recordfile.getAbsolutePath(), hdfsNamenodehostName +
        hdfsDirPath + recordfile.getName());
  }

  /**
   * compress the fault log file to store
   * @param file
   *        file to compress
   */
  public void compress(File file) {
    String localZipPath = Zip.compress(file);
    File localZipFile2 = new File(localZipPath);
    String path = HdfsOperater.uploadHdfs(localZipFile2.getAbsolutePath(),
        hdfsNamenodehostName + hdfsDirPath +
            localZipFile2.getParentFile().getName() + File.separator +
            localZipFile2.getName());
    localZipFile2.delete();
    recordHdfs(path);
  }

  /**
   * record the filePath uploading hdfs into DirRecord
   * @param filePath
   *        filepath to record.
   */
  public void recordHdfs(String filePath) {
    if (!filePath.equals("error")) {
      int hdfsDeleteIndex = 0;
      String hdfsPath = null;
      hdfsDeleteIndex = dr.pushHdfsFile(filePath);
      if ((hdfsPath = dr.getHdfsFile(hdfsDeleteIndex)) != null) {
        HdfsOperater.deleteHdfs(hdfsPath);
        String hdfsParentPath = hdfsPath.substring(0,
            hdfsPath.lastIndexOf('/') + 1);
        System.out.println(HdfsOperater.isHdfsDirEmpty(hdfsParentPath));
        if (HdfsOperater.isHdfsDirEmpty(hdfsParentPath)) {
          HdfsOperater.deleteHdfs(hdfsParentPath);
        }
        dr.deleteHdfsFile(hdfsDeleteIndex);
      }
    } else {
      System.out.println("upload to hdfs failed");
    }
  }

  /**
   * delete all the local file and hdfs file
   */
  public void deleteAllFile() {
    for (int index = 0; index < dr.getLocalFileNum(); index++) {
      dr.deleteLocalFile(index);
    }
    for (int indexh = 0; indexh < dr.getHdfsFileNum(); indexh++) {
      HdfsOperater.deleteHdfs(dr.getHdfsFile(indexh));
      dr.deleteHdfsFile(indexh);
    }
    dr = null;
  }

  /**
   * get the record file
   * @return recordfile
   */
  public static String getRecordFile() {
    return recordFile;
  }
}
