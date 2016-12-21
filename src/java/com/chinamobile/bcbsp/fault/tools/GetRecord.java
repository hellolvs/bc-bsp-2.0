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

package com.chinamobile.bcbsp.fault.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.fault.storage.DirRecord;
import com.chinamobile.bcbsp.fault.storage.ManageFaultLog;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPHdfs;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;

/**
 * get record according to file path.
 * @author hadoop
 *
 */
public class GetRecord {
  /**hdfs record path*/
  private String hdfsRecordPath = null;
  /**local record path*/
  private String localRecordPath = null;
  /**ObjectInputStream handle*/
  private ObjectInputStream ois = null;
  /**log handle*/
  public static final Log LOG = LogFactory.getLog(GetRecord.class);
  /**
   * @param localRecordPath
   *        local record path
   * @param hdfsRecordPath
   *        hdfs record path
   */
  public GetRecord(String localRecordPath, String hdfsRecordPath) {
    this.hdfsRecordPath = hdfsRecordPath;
    this.localRecordPath = localRecordPath;
  }

  /**
   * get record from local and hdfs
   * @return file dir record.
   */
  public DirRecord getRecord() {
    DirRecord dr = null;
    if ((new File(localRecordPath)).exists()) {
      dr = getlocalRecord();
    } else {
      if (HdfsOperater.isHdfsFileExist(hdfsRecordPath)) {
        dr = gethdfsRecord();
        if (!checkUniformity(dr)) {
          updateDirRecord(dr);
        }
      }
    }
    return dr;
  }

  /**
   * update the dirrecord from local file and hdfs
   * @param dr
   *        Dir record to update.
   */
  private void updateDirRecord(DirRecord dr) {
    if (dr.getHdfsFile(((dr.getIndexh() - 1) + dr.getHdfsFileNum()) %
        dr.getHdfsFileNum()) == null) { // at least one
      dr = null;
    } else {
      int hdfsIndex = (dr.getIndexh() - 1) % dr.getHdfsFileNum();
      String localFile = dr.getHdfsFile(((dr.getIndexh() - 1) + dr
          .getHdfsFileNum()) % dr.getHdfsFileNum());
      String hdfsFile;
      dr.setIndexl(0);
      for (int indexl = 0; indexl < dr.getLocalFileNum(); indexl++) {
        dr.pushLocalFile(null);
      }
      for (int i = dr.getLocalFileNum() - 2; i > 0; i--) {
        hdfsFile = dr.getHdfsFile(((hdfsIndex - i + 1) + dr.getHdfsFileNum()) %
            dr.getHdfsFileNum());
        if (hdfsFile != null) {
          localFile = downLoadToLocal(hdfsFile);
          dr.pushLocalFile(new File(localFile));
        }
      }
      String localDirPath = new File(localFile).getParentFile().getParentFile()
          .getAbsolutePath() +
          File.separator + ManageFaultLog.getRecordFile();
      String hdfsNameNodeHostName = getHdfsNameNodeHostName();
      // add update the Dirrecord to disk
      dr.setCopyFlag(true);
      syschronizeDirRecordWithDisk(dr, localDirPath, hdfsNameNodeHostName);
    }
  }

  /**
   * get hdfs namenode hostname
   * @return hdfsNamenodehostName
   */
  private String getHdfsNameNodeHostName() {
    String HADOOP_HOME = System.getenv("HADOOP_HOME");
    String corexml = HADOOP_HOME + "/conf/core-site.xml";
    BSPHdfs Hdfsnamennode = new BSPHdfsImpl(false);
    Hdfsnamennode.hdfsConf(corexml);
    String hdfsNamenodehostName = Hdfsnamennode.hNhostname();
    return hdfsNamenodehostName;
  }

  /**
   * syschronize DirRecord to the disk file.
   * @param dr
   *        DirRecord
   * @param localDirPath
   *        local file Path on disk
   * @param hdfsNameNodeHostName
   *        hdfs namenode hostname
   */
  private void syschronizeDirRecordWithDisk(DirRecord dr, String localDirPath,
      String hdfsNameNodeHostName) {
    ObjectOutputStream oos = null;
    File recordfile = new File(localDirPath);
    try {
      if (!recordfile.getParentFile().exists()) {
        recordfile.getParentFile().mkdirs();
      }
      oos = new ObjectOutputStream(new FileOutputStream(localDirPath));
      oos.writeObject(dr); // write the dr into recordPath;
      oos.close();
    } catch (FileNotFoundException e) {
      //LOG.error("[syschronizeDirRecordWithDisk]", e);
      throw new RuntimeException("[syschronizeDirRecordWithDisk]", e);
    } catch (IOException e) {
      //LOG.error("[syschronizeDirRecordWithDisk]", e);
      throw new RuntimeException("[syschronizeDirRecordWithDisk]", e);
    }
    HdfsOperater.uploadHdfs(recordfile.getAbsolutePath(), hdfsNameNodeHostName +
        recordfile.getAbsolutePath());
  }

  /**
   * downhdfs file to local
   * @param hdfsFile
   *        hdfsfile to download
   * @return localfile
   */
  private String downLoadToLocal(String hdfsFile) {
    String LocalZipFile = getLocalFilePath(hdfsFile);
    HdfsOperater.downloadHdfs(hdfsFile, LocalZipFile);
    Zip.decompress(LocalZipFile);
    String localFile = LocalZipFile.substring(0, LocalZipFile.lastIndexOf("."));
    deleteFile(new File(LocalZipFile));
    return localFile;
  }

  /**
   * get the localfile path
   * @param hdfsFile
   *        hdfsfile
   * @return local file path.
   */
  public static String getLocalFilePath(String hdfsFile) {
    String s = hdfsFile;
    int pos;
    for (int i = 0; i < 3; i++) {
      pos = s.indexOf("/");
      s = s.substring(pos + 1);
    }
    s = "/" + s;
    return s;
  }

  /**
   * check the Uniformity of DirRecord and local file.
   * @param dr
   *        DirRecord to check
   * @return check result.
   */
  private boolean checkUniformity(DirRecord dr) {
    int num = dr.getLocalFileNum();
    File localFile = null;
    for (int i = 0; i < num; i++) {
      localFile = dr.getLocalFile(i);
      if (localFile != null && !localFile.exists()) {
        return false;
      }
    }
    return true;
  }

  /**
   * delete file
   * @param file
   *        file to delete
   */
  private void deleteFile(File file) {
    if (file.isFile() || file.listFiles().length == 0) {
      file.delete();
    } else {
      File[] files = file.listFiles();
      for (File f : files) {
        if (f.isFile()) {
          f.delete();
        } else {
          deleteFile(f);
        }
      }
      file.delete();
    }
  }

  /**
   * get hdfsrecord from hdfs and local file.
   * @return dir record.
   */
  private DirRecord gethdfsRecord() {
    HdfsOperater.downloadHdfs(hdfsRecordPath, localRecordPath);
    return getlocalRecord();
  }

  /**
   * get local record from local record path.
   * @return
   *        dir record.
   */
  private DirRecord getlocalRecord() {
    DirRecord dr = null;
    try {
      ois = new ObjectInputStream(new FileInputStream(localRecordPath));
      dr = (DirRecord) ois.readObject();
      ois.close();
    } catch (FileNotFoundException e) {
      //LOG.error("[getlocalRecord]", e);
      throw new RuntimeException("[getlocalRecord]", e);
    } catch (IOException e) {
      //LOG.error("[getlocalRecord]", e);
      throw new RuntimeException("[getlocalRecord]", e);
    } catch (ClassNotFoundException e) {
      //LOG.error("[getlocalRecord]", e);
      throw new RuntimeException("[getlocalRecord]", e);
    }
    return dr;
  }
}
