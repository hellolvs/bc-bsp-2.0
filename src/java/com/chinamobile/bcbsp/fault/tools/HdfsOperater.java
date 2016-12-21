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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFSDataInputStream;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileSystem;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPHdfs;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFSDataInputStreamImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFileSystemImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;

/**
 * hdfs operator upload or download file from hdfs.
 * @author hadoop
 *
 */
public class HdfsOperater {
  /**log handle*/
  public static final Log LOG = LogFactory.getLog(HdfsOperater.class);
  /**
   * update file to hdfs
   * @param localPath
   *        local file path to update
   * @param destPath
   *        destination path in hdfs
   * @return upload result
   */
  public static String uploadHdfs(String localPath, String destPath) {
    InputStream in = null;
    OutputStream out = null;
    try {
      String localSrc = localPath;
      File srcFile = new File(localSrc);
      if (srcFile.exists()) {
        // String dst = hostName + dirPath;
        in = new BufferedInputStream(new FileInputStream(localSrc));
        // Configuration conf = new Configuration();
        // FileSystem fs = FileSystem.get(URI.create(destPath), conf);
        BSPHdfs Hdfsup = new BSPHdfsImpl();
        // out = fs.create(new Path(destPath),
        out = Hdfsup.hdfsOperater(destPath, Hdfsup.getConf());
        // new Progressable() {
        // public void progress() {
        //
        // }
        // });
        IOUtils.copyBytes(in, out, 4096, true);
        out.flush();
        out.close();
        in.close();
        return destPath;
      } else {
        return "error";
      }
    } catch (FileNotFoundException e) {
      LOG.error("[uploadHdfs]", e);
      return "error";
    } catch (IOException e) {
      LOG.error("[uploadHdfs]", e);
      try {
        if (out != null) {
          out.flush();
        }
        if (out != null) {
          out.close();
        }
        if (in != null) {
          in.close();
        }
      } catch (IOException e1) {
        LOG.error("[uploadHdfs]", e1);
      }
      return "error";
    }
  }

  /**
   * download file from hdfs
   * @param srcfilePath
   *        source file path to download
   * @param destFilePath
   *        destination path to download to.
   */
  public static void downloadHdfs(String srcfilePath, String destFilePath) {
    try {
      Configuration conf = new Configuration();
      // FileSystem fs = FileSystem.get(URI.create(srcfilePath), conf);
      // FSDataInputStream hdfsInStream = fs.open(new Path(srcfilePath));
      BSPFileSystem bspfs = new
          BSPFileSystemImpl(URI.create(srcfilePath), conf);
      BSPFSDataInputStream hdfsInStream = new BSPFSDataInputStreamImpl(bspfs,
          new BSPHdfsImpl().newPath(srcfilePath));
      File dstFile = new File(destFilePath);
      if (!dstFile.getParentFile().exists()) {
        dstFile.getParentFile().mkdirs();
      }
      OutputStream out = new FileOutputStream(destFilePath);
      byte[] ioBuffer = new byte[1024];
      int readLen = hdfsInStream.read(ioBuffer);
      while (-1 != readLen) {
        out.write(ioBuffer, 0, readLen);
        readLen = hdfsInStream.read(ioBuffer);
      }
      out.close();
      // hdfsInStream.close();
      hdfsInStream.hdfsInStreamclose();
      bspfs.close();
    } catch (FileNotFoundException e) {
      //LOG.error("[downloadHdfs]", e);
      throw new RuntimeException("[downloadHdfs]", e);
    } catch (IOException e) {
      //LOG.error("[downloadHdfs]", e);
      throw new RuntimeException("[downloadHdfs]", e);
    }
  }

  /**
   * delete file on hdfs.
   * @param hdfsFile
   *        hdfsfile to delete.
   */
  public static void deleteHdfs(String hdfsFile) {
    try {
      Configuration conf = new Configuration();
      // FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
      // fs.deleteOnExit(new Path(hdfsFile));
      BSPHdfs dehdfs = new BSPHdfsImpl();
      dehdfs.deleteHdfs(hdfsFile, dehdfs.getConf());
      dehdfs.deleteOnExit(hdfsFile);
      dehdfs.close();
    } catch (IOException e) {
      //LOG.error("[deleteHdfs]", e);
      throw new RuntimeException("[downloadHdfs]", e);
    }
  }

  /**
   * check the existence of file on hdfs
   * @param hdfsFilePath
   *        hdfsfile path to check
   * @return exist true not false.
   */
  public static boolean isHdfsFileExist(String hdfsFilePath) {
    // Configuration conf = new Configuration();
    // Path path = new Path(hdfsFilePath);
    BSPHdfs IhdfsFE = new BSPHdfsImpl();
    IhdfsFE.newPath(hdfsFilePath);
    try {
      // FileSystem fs = FileSystem.get(URI.create(hdfsFilePath), conf);
      IhdfsFE.iSHdfsFileExist(hdfsFilePath, IhdfsFE.getConf());
      // if (fs.exists(path) == true)
      if (IhdfsFE.exists(IhdfsFE.newPath(hdfsFilePath)) == true) {
        return true;
      } else {
        return false;
      }
    } catch (IOException e) {
      LOG.error("[isHdfsFileExist]", e);
      return false;
    }
  }

  /**
   * judge if the hdfsfile is empty according to file path
   * @param hdfsFilePath
   *        hdfs file path to judge
   * @return empty true not false.
   */
  public static boolean isHdfsDirEmpty(String hdfsFilePath) {
    try {
      // Configuration conf = new Configuration();
      // Path path = new Path(hdfsFilePath);
      BSPHdfs IhdfsDE = new BSPHdfsImpl();
      IhdfsDE.newPath(hdfsFilePath);
      // FileSystem fs = FileSystem.get(URI.create(hdfsFilePath), conf);
      // FileStatus status[] = fs.globStatus(path);
      //
      // if (status == null || status.length == 0)
      if (IhdfsDE.iSHdfsFileExist(hdfsFilePath, IhdfsDE.getConf()).globStatus(
          IhdfsDE.newPath(hdfsFilePath)) == null ||
          IhdfsDE.iSHdfsFileExist(hdfsFilePath, IhdfsDE.getConf())
              .globStatus(IhdfsDE.newPath(hdfsFilePath)).length == 0) {
        throw new FileNotFoundException("Cannot access " + hdfsFilePath +
            ": No such file or directory.");
      }
      for (int i = 0; i < IhdfsDE.globStatus(IhdfsDE.newPath(hdfsFilePath)).length; i++) {
        // long totalSize = fs.getContentSummary(status[i].getPath())
        // .getLength();
        long totalSize = IhdfsDE.getContentSummary(
            (IhdfsDE.globStatus(IhdfsDE.newPath(hdfsFilePath)))[i].getPath())
            .getLength();
        @SuppressWarnings("unused")
        // String pathStr = status[i].getPath().toString();
        String pathStr = (IhdfsDE.globStatus(IhdfsDE.newPath(hdfsFilePath)))[i]
            .getPath().toString();
        return totalSize == 0 ? true : false;
      }
    } catch (IOException e) {
      LOG.error("[isHdfsDirEmpty]", e);
      return false;
    }
    return false;
  }
}
