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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.fault.storage.Fault;

/**
 * message service for the fault log.
 * @author hadoop
 */
public class MessageService {
  /**message types*/
  public static enum Type {
    TYPE, LEVEL, TIMEOFFAILURE, WORKERNODENAME, JOBNAME, STAFFNAME,
    EXCEPTIONMESSAGE, FAULTSTATUS
  }
  /**current page num*/
  private int currentPage = 1;
  /**default pagesize*/
  private int pageSize = 10;
  /**handle of fault list*/
  private List<Fault> totalList = new ArrayList<Fault>();
  /**handle log information in the class*/
  private static final Log LOG = LogFactory.getLog(MessageService.class);
  /**get the fault total list
   * @return fault total list.
   * */
  private List<Fault> getTotalList() {
    return totalList;
  }
  /**
   * set the fault total list
   * @param totalList
   *        fault list to be set.
   */
  private void setTotalList(List<Fault> totalList) {
    this.totalList = totalList;
  }

  /**
   * set the current page.
   * @param currentPage
   *        currentPage num to be set.
   */
  public void setCurrentPage(int currentPage) {
    this.currentPage = currentPage;
  }

  /**
   * set the pagesize
   * @param pageSize
   *        pagesize to be set.
   */
  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  /**
   * get the current page num
   * @return currentpage
   */
  public int getCurrentPage() {
    return this.currentPage;
  }

  /**
   * get the pagesize.
   * @return pagesize.
   */
  public int getPageSize() {
    return this.pageSize;
  }

  /**
   * get total rows of fault list.
   * @return rows num.
   */
  public int getTotalRows() {
    List<Fault> list = getTotalList();
    return list.size();
  }

  /**
   * get the totalpages num with fault total num and
   * a page size.
   * @return total page num.
   */
  public int getTotalPages() {
    int total = getTotalRows() / getPageSize();
    int left = getTotalRows() % getPageSize();
    if (left != 0) {
      total = total + 1;
    }
    return total;
  }

  /**
   * get fault list from the specific page
   * and get the list accord to the type.
   * @param pageNumber
   *        fault page num to get.
   * @param type
   *        retrive the fault according to the type.
   * @return
   *        fault list.
   */
  public List<Fault> getPageByType(int pageNumber, String type) {
    Browse br = new Browse();
    if (type.equals("TYPE")) {
      setTotalList(br.retrieveByType());
    } else if (type.equals("LEVEL")) {
      setTotalList(br.retrieveByLevel());
    } else if (type.equals("TIME")) {
      LOG.info("+++++++++++enter the getFile(n) from TIME");
      setTotalList(br.retrieveByTime());
    } else if (type.equals("WORKER")) {
      setTotalList(br.retrieveByPosition());
    } else {
      setTotalList(br.retrieveByTime());
    }
    List<Fault> res = new ArrayList<Fault>();
    if (getTotalList().size() == 0) {
      return res;
    }
    this.setCurrentPage(pageNumber);
    int totalPages = getTotalPages();
    if (currentPage <= 1) {
      currentPage = 1;
    }
    if (currentPage >= totalPages) {
      currentPage = totalPages;
    }
    if (currentPage >= 1 && currentPage < totalPages) {
      for (int i = (currentPage - 1) * pageSize; i <
          currentPage * pageSize; i++) {
        res.add(totalList.get(i));
      }
    } else if (currentPage == totalPages) {
      for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
        res.add(totalList.get(i));
      }
    }
    return res;
  }

  /**
   * get fault list from the specific page
   * and get the list accord to the type.
   * @param pageNumber
   *        fault page num to get.
   * @param type
   *        retrive the fault according to the type.
   * @param num
   *        retrieve monthnum
   * @return fault list.
   */
  public List<Fault> getPageByType(int pageNumber, String type, int num) {
    Browse br = new Browse();
    if (type.equals("TYPE")) {
      setTotalList(br.retrieveByType(num));
    } else if (type.equals("LEVEL")) {
      setTotalList(br.retrieveByLevel(num));
    } else if (type.equals("TIME")) {
      setTotalList(br.retrieveByTime(num));
    } else if (type.equals("WORKER")) {
      setTotalList(br.retrieveByPosition(num));
    } else {
      setTotalList(br.retrieveByTime(num));
    }
    List<Fault> res = new ArrayList<Fault>();
    if (getTotalList().size() == 0) {
      return res;
    }
    this.setCurrentPage(pageNumber);
    int totalPages = getTotalPages();
    if (currentPage <= 1) {
      currentPage = 1;
    }
    if (currentPage >= totalPages) {
      currentPage = totalPages;
    }
    if (currentPage >= 1 && currentPage < totalPages) {
      for (int i = (currentPage - 1) * pageSize; i <
          currentPage * pageSize; i++) {
        res.add(totalList.get(i));
      }
    } else if (currentPage == totalPages) {
      for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
        res.add(totalList.get(i));
      }
    }
    return res;
  }

  /**
   * get fault list from the specific page
   * and get the list accord to the key.
   * @param pageNumber
   *         fault page num to get.
   * @param key
   *        specific key
   * @return fault list
   */
  public List<Fault> getPageByKey(int pageNumber, String key) {
    Browse br = new Browse();
    String[] keys = {key};
    setTotalList(br.retrieveWithMoreKeys(keys));
    List<Fault> res = new ArrayList<Fault>();
    if (getTotalList().size() == 0) {
      return res;
    }
    this.setCurrentPage(pageNumber);
    int totalPages = getTotalPages();
    if (currentPage <= 1) {
      currentPage = 1;
    }
    if (currentPage >= totalPages) {
      currentPage = totalPages;
    }
    if (currentPage >= 1 && currentPage < totalPages) {
      for (int i = (currentPage - 1) * pageSize; i <
          currentPage * pageSize; i++) {
        res.add(totalList.get(i));
      }
    } else if (currentPage == totalPages) {
      for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
        res.add(totalList.get(i));
      }
    }
    return res;
  }

  /**
   * get fault list from the specific page
   * and get the list accord to the key.
   * @param pageNumber
   *        fault page num to get.
   * @param key
   *        specific key
   * @param num
   *        num to retrieve the fault list.
   * @return fault list.
   */
  public List<Fault> getPageByKey(int pageNumber, String key, int num) {
    Browse br = new Browse();
    String[] keys = {key};
    if (key != null) {
    }
    setTotalList(br.retrieveWithMoreKeys(keys, num));
    List<Fault> res = new ArrayList<Fault>();
    if (getTotalList().size() == 0) {
      return res;
    }
    this.setCurrentPage(pageNumber);
    int totalPages = getTotalPages();
    if (currentPage <= 1) {
      currentPage = 1;
    }
    if (currentPage >= totalPages) {
      currentPage = totalPages;
    }
    if (currentPage >= 1 && currentPage < totalPages) {
      for (int i = (currentPage - 1) * pageSize; i <
          currentPage * pageSize; i++) {
        res.add(totalList.get(i));
      }
    } else if (currentPage == totalPages) {
      for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
        res.add(totalList.get(i));
      }
    }
    return res;
  }

  /**
   * get fault list from the specific page
   * and get the list accord to the key.
   * @param pageNumber
   *        fault page num to get.
   * @param keys
   *        specific keys
   * @return fault list.
   */
  public List<Fault> getPageByKeys(int pageNumber, String[] keys) {
    Browse br = new Browse();
    setTotalList(br.retrieveWithMoreKeys(keys));
    List<Fault> res = new ArrayList<Fault>();
    if (getTotalList().size() == 0) {
      return res;
    }
    this.setCurrentPage(pageNumber);
    int totalPages = getTotalPages();
    if (currentPage <= 1) {
      currentPage = 1;
    }
    if (currentPage >= totalPages) {
      currentPage = totalPages;
    }
    if (currentPage >= 1 && currentPage < totalPages) {
      for (int i = (currentPage - 1) * pageSize; i <
          currentPage * pageSize; i++) {
        res.add(totalList.get(i));
      }
    } else if (currentPage == totalPages) {
      for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
        res.add(totalList.get(i));
      }
    }
    return res;
  }

  /**
   * get fault list from the specific page
   * and get the list accord to the key.
   * @param pageNumber
   *        fault page num to get.
   * @param keys
   *         specific keys
   * @param num
   *        num to retrieve the fault list.
   * @return fault list.
   */
  public List<Fault> getPageByKeys(int pageNumber, String[] keys, int num) {
    Browse br = new Browse();
    setTotalList(br.retrieveWithMoreKeys(keys, num));
    List<Fault> res = new ArrayList<Fault>();
    if (getTotalList().size() == 0) {
      return res;
    }
    this.setCurrentPage(pageNumber);
    int totalPages = getTotalPages();
    if (currentPage <= 1) {
      currentPage = 1;
    }
    if (currentPage >= totalPages) {
      currentPage = totalPages;
    }
    if (currentPage >= 1 && currentPage < totalPages) {
      for (int i = (currentPage - 1) * pageSize; i <
          currentPage * pageSize; i++) {
        res.add(totalList.get(i));
      }
    } else if (currentPage == totalPages) {
      for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
        res.add(totalList.get(i));
      }
    }
    return res;
  }
}
