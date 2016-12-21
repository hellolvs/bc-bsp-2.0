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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.fault.storage.Fault.Level;
import com.chinamobile.bcbsp.fault.storage.Fault.Type;
import com.chinamobile.bcbsp.fault.storage.ManageFaultLog;

/**
 * read Fault log from local path or hdfs path;
 * @author root
 */
public class ReadFaultlog {
  /**HDFS backup default num?*/
  private int defaultNum = 3;
  /**local faultlog path*/
  private String localRecordPath = null;
  /**hdfs faultrecord path*/
  private String hdfsRecordPath = null;
  /**BufferedReader handle*/
  private BufferedReader br = null;
  /**GetFile handle*/
  private GetFile getfile = null;
  /**handle the log in the class*/
  private static final Log LOG = LogFactory.getLog(ReadFaultlog.class);
  /**
   * ReadFaultlog construct method.
   * @param dirPath
   *        fault dirpath
   * @param hdfsHostName
   *        fault log store hostname.
   */
  public ReadFaultlog(String dirPath, String hdfsHostName) {
    this(dirPath, dirPath, hdfsHostName);
  }

  /**
   * @param localDirPath
   *        is the directory of the record.bak
   * @param domainName
   *        the namenode domainName
   * @param hdfsDir
   *        the directory of the record.bak in hdfs
   */
  public ReadFaultlog(String localDirPath, String hdfsDir, String domainName) {
    super();
    if (localDirPath.substring(localDirPath.length() - 1) != "/" &&
        localDirPath.substring(localDirPath.length() - 2) != "\\") {
      localDirPath = localDirPath + File.separator;
    }
    if (hdfsDir.substring(hdfsDir.length() - 1) != "/" &&
        hdfsDir.substring(hdfsDir.length() - 2) != "\\") {
      hdfsDir = hdfsDir + File.separator;
    }
    this.localRecordPath = localDirPath + ManageFaultLog.getRecordFile();
    this.hdfsRecordPath = domainName + hdfsDir + ManageFaultLog.getRecordFile();
    getfile = new GetFile(localRecordPath, hdfsRecordPath);
  }

  /**
   * read fault records from monthDir.
   * @return fault records.
   */
  public List<Fault> read() {
    List<Fault> records = new ArrayList<Fault>();
    List<String> monthDirs = null;
    monthDirs = getfile.getFile(defaultNum);
    for (String eachMonth : monthDirs) {
      records.addAll(readDir(eachMonth));
    }
    getfile.deletehdfsDir(); // delete the distributeFile in the localdisk;
    return records;
  }

  /**
   * read fault records from monthDir.
   * @param n file num to get.
   * @return a list of fault in order to browse
   */
  public List<Fault> read(int n) {
    List<Fault> records = new ArrayList<Fault>();
    List<String> monthDirs = null;
    monthDirs = getfile.getFile(n);
    for (String eachMonth : monthDirs) {
      records.addAll(readDir(eachMonth));
    }
    getfile.deletehdfsDir(); // delete the distributeFile in the localdisk;
    return records;
  }

  /**
   * read fault records from monthDir.
   * @param keys
   *        the given restrain
   * @return the records with the given keys
   */
  public List<Fault> read(String[] keys) {
    List<Fault> records = new ArrayList<Fault>();
    List<String> monthDirs = null;
    monthDirs = getfile.getFile(defaultNum);
    for (String eachMonth : monthDirs) {
      records.addAll(readDirWithKey(eachMonth, keys));
    }
    getfile.deletehdfsDir(); // delete the distributeFile in the localdisk;
    return records;
  }

  /**
   * read fault records from monthDir.
   * @param keys
   *        the given restrain
   * @param n
   *        is the month ready to browse
   * @return the records with the given keys
   */
  public List<Fault> read(String[] keys, int n) {
    List<Fault> records = new ArrayList<Fault>();
    List<String> monthDirs = null;
    monthDirs = getfile.getFile(n);
    for (String eachMonth : monthDirs) {
      records.addAll(readDirWithKey(eachMonth, keys));
    }
    getfile.deletehdfsDir(); // delete the distributeFile in the localdisk;
    return records;
  }

  /**
   * read fault records from monthDir.
   * @param path
   *        the directory of the ready to browse
   * @return each record of the files in the directory
   */
  public List<Fault> readDir(String path) {
    List<Fault> records = new ArrayList<Fault>();
    File srcFile = new File(path);
    if (srcFile.isDirectory()) {
      ArrayList<File> files = getAllFiles(srcFile);
      for (File file : files) {
        if (!records.addAll(readFile(file))) {
          return new ArrayList<Fault>();
        }
      }
    } else {
      if (!records.addAll(readFile(srcFile))) {
        return new ArrayList<Fault>();
      }
    }
    return records;
  }

  /**
   * read fault records in the specific path and with
   * the key restrain.
   * @param path
   *        the directory of the ready to browse
   * @param keys
   *        the given restrain
   * @return records in the path
   */
  public List<Fault> readDirWithKey(String path, String[] keys) {
    List<Fault> records = new ArrayList<Fault>();
    File srcFile = new File(path);
    if (srcFile.isDirectory()) {
      ArrayList<File> files = getAllFiles(srcFile);
      for (File file : files) {
        records.addAll(readFileWithKey(file, keys));
      }
    } else {
      if (!records.addAll(readFileWithKey(srcFile, keys))) {
        return new ArrayList<Fault>();
      }
    }
    return records;
  }

  /**
   * read fault list from the specific file.
   * @param file
   *        fault file to read from.
   * @return fault list.
   */
  private List<Fault> readFile(File file) {
    FileReader fr = null;
    try {
      List<Fault> records = new ArrayList<Fault>();
      fr = new FileReader(file);
      br = new BufferedReader(fr);
      String tempRecord = null;
      String[] filds = null;
      while ((tempRecord = br.readLine()) != null) {
        filds = tempRecord.split("--");
        Fault fault = new Fault();
        fault.setTimeOfFailure(filds[0].trim());
        fault.setType(getType(filds[1].trim()));
        fault.setLevel(getLevel(filds[2].trim()));
        fault.setWorkerNodeName(filds[3].trim());
        fault.setJobName((filds[4].trim()));
        fault.setStaffName(filds[5].trim());
        fault.setExceptionMessage(filds[6].trim());
        fault.setFaultStatus(getBoolean(filds[7].trim()));
        records.add(fault);
      }
      br.close();
      fr.close();
      return records;
    } catch (FileNotFoundException e) {
      LOG.error("file not foutnd Exception! ", e);
      return null;
    } catch (IOException e) {
      LOG.error("read file IOExeption! ", e);
      try {
        br.close();
        fr.close();
      } catch (IOException e1) {
        throw new RuntimeException("close file IOException!", e1);
      }
      return null;
    }
  }

  /**
   * get the fault level
   * @param level
   *        given level
   * @return fault level
   */
  private Fault.Level getLevel(String level) {
    if (level.equals("INDETERMINATE")) {
      return Level.INDETERMINATE;
    } else if (level.equals("WARNING")) {
      return Level.WARNING;
    } else if (level.equals("CRITICAL")) {
      return Level.CRITICAL;
    } else if (level.equals("MAJOR")) {
      return Level.MAJOR;
    } else {
      return Level.MINOR;
    }
  }

  /**
   * get the fault type according to the given type
   * @param type
   *        given fault type
   * @return fault type.
   */
  private Fault.Type getType(String type) {
    if (type.equals("DISK")) {
      return Type.DISK;
    } else if (type.equals("NETWORK")) {
      return Type.NETWORK;
    } else if (type.equals("SYSTEMSERVICE")) {
      return Type.SYSTEMSERVICE;
    } else if (type.equals("FORCEQUIT")) {
      return Type.FORCEQUIT;
    } else {
      return Type.WORKERNODE;
    }
  }

  /**
   * get judge result according to flag
   * @param flag
   *        given flag
   * @return flag equals true return true
   *         not return false.
   */
  private boolean getBoolean(String flag) {
    if (flag.equals("true")) {
      return true;
    }
    return false;
  }

  /**
   * file is the srcFile,and keys is the given key and the keys can be null then
   * the function is read all the record in the file,if none of keys ,it is
   * better to use read(File file) ,the function need not judge for each record
   * return the match condition record
   * @param file
   *        source file
   * @param keys
   *        keys to tell whether need to read the file.
   * @return fault list.
   */
  private List<Fault> readFileWithKey(File file, String[] keys) {
    FileReader fr = null;
    try {
      List<Fault> records = new ArrayList<Fault>();
      fr = new FileReader(file);
      br = new BufferedReader(fr);
      String tempRecord = null;
      String filds[] = null;
      while ((tempRecord = br.readLine()) != null) {
        boolean matching = true;
        if (keys != null) {
          for (String key : keys) {
            if (key == null) {
              continue;
            }
            if ((tempRecord.indexOf(key) == -1) &&
                (tempRecord.toLowerCase().indexOf(key.toLowerCase()) == -1)) {
              matching = false;
              break;
            }
          }
        }
        if (matching == true) {
          filds = tempRecord.split("--");
          Fault fault = new Fault();
          fault.setTimeOfFailure(filds[0].trim());
          fault.setType(getType(filds[1].trim()));
          fault.setLevel(getLevel(filds[2].trim()));
          fault.setWorkerNodeName(filds[3].trim());
          fault.setJobName((filds[4].trim()));
          fault.setStaffName(filds[5].trim());
          fault.setExceptionMessage(filds[6].trim());
          fault.setFaultStatus(getBoolean(filds[7].trim()));
          records.add(fault);
        } else {
          continue;
        }
      }
      br.close();
      fr.close();
      return records;
    } catch (FileNotFoundException e) {
      LOG.error("[readFileWithKey]", e);
      return new ArrayList<Fault>();
    } catch (IOException e) {
      LOG.error("[readFileWithKey]", e);
      try {
        br.close();
        fr.close();
      } catch (IOException e1) {
        LOG.error("[readFileWithKey]", e1);
      }
      return new ArrayList<Fault>();
    }
  }

  /**
   * get all the files in the given directory;
   * @param filesrc
   *        source file to judge.
   * @return all files that in the given directory
   */
  private ArrayList<File> getAllFiles(File filesrc) {
    ArrayList<File> allFiles = new ArrayList<File>();
    File[] files = filesrc.listFiles();
    for (File file : files) {
      if (file.isDirectory()) {
        allFiles.addAll(getAllFiles(file));
      } else {
        allFiles.add(file);
      }
    }
    return allFiles;
  }
}
