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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * compress file to zip or decompress zip file.
 * @author hadoop
 *
 */
public class Zip {
  /**log handle*/
  public static final Log LOG = LogFactory.getLog(Zip.class);
  /**buffer size*/
  private static final int BUFFER = 1024;

  /**
   * Compress files to *.zip.
   * @param file
   *        : file to be compress
   * @return compressed file.
   */
  public static String compress(File file) {
    return compress(file.getAbsolutePath());
  }

  /**
   * Compress files to *.zip.
   * @param fileName
   *        file name to compress
   * @return compressed file.
   */
  public static String compress(String fileName) {
    String targetFile = null;
    File sourceFile = new File(fileName);
    Vector<File> vector = getAllFiles(sourceFile);
    try {
      if (sourceFile.isDirectory()) {
        targetFile = fileName + ".zip";
      } else {
        char ch = '.';
        targetFile = fileName.substring(0, fileName.lastIndexOf((int) ch)) +
            ".zip";
      }
      BufferedInputStream bis = null;
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(
          targetFile));
      ZipOutputStream zipos = new ZipOutputStream(bos);
      byte[] data = new byte[BUFFER];
      if (vector.size() > 0) {
        for (int i = 0; i < vector.size(); i++) {
          File file = vector.get(i);
          zipos.putNextEntry(new ZipEntry(getEntryName(fileName, file)));
          bis = new BufferedInputStream(new FileInputStream(file));
          int count;
          while ((count = bis.read(data, 0, BUFFER)) != -1) {
            zipos.write(data, 0, count);
          }
          bis.close();
          zipos.closeEntry();
        }
        zipos.close();
        bos.close();
        return targetFile;
      } else {
        File zipNullfile = new File(targetFile);
        zipNullfile.getParentFile().mkdirs();
        zipNullfile.mkdir();
        return zipNullfile.getAbsolutePath();
      }
    } catch (IOException e) {
      LOG.error("[compress]", e);
      return "error";
    }
  }

  /**
   * Uncompress *.zip files.
   * @param fileName
   *        : file to be uncompress
   */
  @SuppressWarnings("unchecked")
  public static void decompress(String fileName) {
    File sourceFile = new File(fileName);
    String filePath = sourceFile.getParent() + "/";
    try {
      BufferedInputStream bis = null;
      BufferedOutputStream bos = null;
      ZipFile zipFile = new ZipFile(fileName);
      Enumeration en = zipFile.entries();
      byte[] data = new byte[BUFFER];
      while (en.hasMoreElements()) {
        ZipEntry entry = (ZipEntry) en.nextElement();
        if (entry.isDirectory()) {
          new File(filePath + entry.getName()).mkdirs();
          continue;
        }
        bis = new BufferedInputStream(zipFile.getInputStream(entry));
        File file = new File(filePath + entry.getName());
        File parent = file.getParentFile();
        if (parent != null && (!parent.exists())) {
          parent.mkdirs();
        }
        bos = new BufferedOutputStream(new FileOutputStream(file));
        int count;
        while ((count = bis.read(data, 0, BUFFER)) != -1) {
          bos.write(data, 0, count);
        }
        bis.close();
        bos.close();
      }
      zipFile.close();
    } catch (IOException e) {
      //LOG.error("[compress]", e);
      throw new RuntimeException("[compress]", e);
    }
  }

  /**
   * To get a directory's all files.
   * @param sourceFile
   *        : the source directory
   * @return the files' collection
   */
  private static Vector<File> getAllFiles(File sourceFile) {
    Vector<File> fileVector = new Vector<File>();
    if (sourceFile.isDirectory()) {
      File[] files = sourceFile.listFiles();
      for (int i = 0; i < files.length; i++) {
        fileVector.addAll(getAllFiles(files[i]));
      }
    } else {
      fileVector.add(sourceFile);
    }
    return fileVector;
  }

  /**
   * get file entry name according to base
   * @param base
   *        base to split the total file
   * @param file
   *        file to get the entry
   * @return entry name.
   */
  private static String getEntryName(String base, File file) {
    File baseFile = new File(base);
    String filename = file.getPath();
    if (baseFile.getParentFile().getParentFile() == null) {
      return filename.substring(baseFile.getParent().length());
    } // d:/file1 /file2/text.txt
    /*d:/file1/text.txt*/
    return filename.substring(baseFile.getParent().length() + 1);
  }
}
