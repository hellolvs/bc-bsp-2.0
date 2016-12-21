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

import java.util.*;
import com.chinamobile.bcbsp.fault.browse.MessageService;
import com.chinamobile.bcbsp.fault.storage.Fault;

/**
 * get and set the page information and get
 * the page information in html.
 * @author hadoop
 *
 */
public class PageService {
  /**default pagenum*/
  private int pageNumber = 1;
  /**fault type*/
  private String type;
  /**fault level*/
  private String level;
  /**fault time*/
  private String time;
  /**fault happened worker*/
  private String worker;
  /**fault specific key*/
  private String key;
  /**fault happened month?*/
  private String month;
  /**
   * get fault month
   * @return month information.
   */
  public String getMonth() {
    return month;
  }

  /**
   * set the month information
   * @param month
   *        month to set.
   */
  public void setMonth(String month) {
    if (month == null) {
      this.month = "";
    } else if (month.equals("null")) {
      this.month = "";
    } else {
      this.month = month;
    }
  }

  /**
   * get fault level.
   * @return fault level
   */
  public String getLevel() {
    return level;
  }

  /**
   * set fault level
   * @param level
   *        fault level to set.
   */
  public void setLevel(String level) {
    this.level = level;
  }
  /**
   * get fault happen time
   * @return
   *        fault happen time
   */
  public String getTime() {
    return time;
  }

  /**
   * set fault happened time
   * @param time
   *        fault hadppened to set.
   */
  public void setTime(String time) {
    if (time == null) {
      this.time = "";
    } else if (time.equals("null")) {
      this.time = "";
    } else {
      this.time = time;
    }
  }

  /**
   * get fault happened worker
   * @return fault happened worker
   */
  public String getWorker() {
    return worker;
  }

  /**
   * set fault happened worker
   * @param worker
   *        fault happened worker
   */
  public void setWorker(String worker) {
    if (worker == null) {
      this.worker = "";
    } else if (worker.equals("null")) {
      this.worker = "";
    } else {
      this.worker = worker;
    }
  }

  /**
   * fault specific key
   * @return fault specific key
   */
  public String getKey() {
    return key;
  }

  /**
   * set fault specific key
   * @param key
   *        fault key to be set.
   */
  public void setKey(String key) {
    if (key == null) {
      this.key = "";
    } else if (key.equals("null")) {
      this.key = "";
    } else {
      this.key = key;
    }
  }

  /**
   * get fault type
   * @return fault type
   */
  public String getType() {
    return type;
  }

  /**
   * set fault type
   * @param type
   *        fault type to be set.
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * set pageNum
   * @param pageNumber
   *        pagenum to be set.
   */
  public void setPageNumber(int pageNumber) {
    this.pageNumber = pageNumber;
  }

  /**
   * get the pageNum
   * @return page num
   */
  public int getPageNumber() {
    return this.pageNumber;
  }

  /**
   * get the page into html which contains the fault information get from page
   * according to type
   * @param path
   *        fault storagepath ?
   * @return html in string
   */
  public String getPageByType(String path) {
    MessageService service = new MessageService();
    String monthNum = getMonth();
    List<Fault> res;
    if (monthNum == null || monthNum.equals("") || monthNum.equals("null")) {
      res = service.getPageByType(pageNumber, type);
    } else {
      int month = Integer.valueOf(monthNum);
      if (month < 0 || month > 20) {
        res = service.getPageByType(pageNumber, type);
      } else {
        res = service.getPageByType(pageNumber, type, month);
      }
    }
    StringBuffer html = new StringBuffer();
    html.append("<font size = '3' >");
    html.append("<table frame = 'box'  >");
    html.append("<tr><td width='59' bgcolor='#78A5D1'>TIME</td><td width='59'" +
      "bgcolor='#78A5D1'>TYPE</td><td width='59'" +
      "bgcolor='#78A5D1'>LEVEL</td><td width='59'" +
      "bgcolor='#78A5D1'>WORKER</td>");
    html.append("<td width='59' bgcolor='#78A5D1'>JOBNAME</td><td width='59'" +
      "bgcolor='#78A5D1'>STAFFNAME</td><td width='59'" +
      "bgcolor='#78A5D1'>STATUS</td><td width='59'" +
      "bgcolor='#78A5D1'>EXCEPTIONMESSAGE</td></tr>");
    if (res.size() == 0) {
      html.append("</table>");
      html.append("<label>no such records</label>");
      html.append("</font>");
      return html.toString();
    }
    for (int i = 0; i < res.size(); i++) {
      Fault fault = res.get(i);
      html.append("<tr>");
      html.append("<td>" + fault.getTimeOfFailure() + "</td>");
      html.append("<td>" + fault.getType() + "</td>");
      html.append("<td>" + fault.getLevel() + "</td>");
      html.append("<td>" + fault.getWorkerNodeName() + "</td>");
      html.append("<td>" + fault.getJobName() + "</td>");
      html.append("<td>" + fault.getStaffName() + "</td>");
      html.append("<td>" + fault.isFaultStatus() + "</td>");
      html.append("<td>" + fault.getExceptionMessage() + "</td>");
      html.append("</tr>");
    }
    int totalPages = service.getTotalPages();
    html.append("<tr>");
    if (pageNumber <= 1) {
      html.append("<td>first page</td>");
      html.append("<td>previous page</td>");
      html.append("<td><a href='" + path + "?pageNumber=" + 2 + "&type=" +
          getType() + "&month=" + getMonth() + "'>next page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + totalPages +
         "&type=" + getType() + "&month=" + getMonth()  +
         "'>last page</a></td>");
    } else if (pageNumber > 1 && pageNumber < totalPages) {
      html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&type=" +
          getType() + "&month=" + getMonth() + "'>first page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + (pageNumber - 1) +
          "&type=" + getType() + "&month=" + getMonth() +
          "'>previous page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + (pageNumber + 1) +
          "&type=" + getType() + "&month=" + getMonth() +
          "'>next page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + totalPages +
          "&type=" + getType() + "&month=" + getMonth() +
         "'>last page</a></td>");
    } else if (pageNumber == totalPages) {
      html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&type=" +
          getType() + "&month=" + getMonth() + "'>first page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + (pageNumber - 1) +
          "&type=" + getType() + "&month=" + getMonth() +
          "'>previous page</a></td>");
      html.append("<td>next page</td>");
      html.append("<td>last page</td>");
    }
    html.append("<td>currentPage " + pageNumber + "</td>");
    html.append("<td>TotalPage " + totalPages + "</td>");
    html.append("</tr>");
    html.append("</table>");
    html.append("</font>");
    return html.toString();
  }

  /**
   * get the page into html which contains the fault information get from page
   * according to type
   * @param path
   *        fault storagepath ?
   * @return html in string
   */
  public String getPageBykey(String path) {
    MessageService service = new MessageService();
    String monthNum = getMonth();
    List<Fault> res;
    if (monthNum == null || monthNum.equals("") || monthNum.equals("null")) {
      res = service.getPageByKey(pageNumber, key);
    } else {
      int month = Integer.valueOf(monthNum);
      if (month < 0 || month > 20) {
        res = service.getPageByKey(pageNumber, key);
      } else {
        res = service.getPageByKey(pageNumber, key, month);
      }
    }
    StringBuffer html = new StringBuffer();
    html.append("<font size = '3' >");
    html.append("<table frame = 'box'  >");
    html.append("<tr><td width='59' bgcolor='#78A5D1'>TIME</td><td width='59'" +
      "bgcolor='#78A5D1'>TYPE</td><td width='59'" +
      "bgcolor='#78A5D1'>LEVEL</td><td width='59'" +
      "bgcolor='#78A5D1'>WORKER</td>");
    html.append("<td width='59' bgcolor='#78A5D1'>JOBNAME</td><td width='59'" +
      "bgcolor='#78A5D1'>STAFFNAME</td><td width='59'" +
      "bgcolor='#78A5D1'>STATUS</td><td width='59'" +
      "bgcolor='#78A5D1'>EXCEPTIONMESSAGE</td></tr>");
    if (res.size() == 0) {
      html.append("</table>");
      html.append("<label>no such records</label>");
      html.append("</font>");
      return html.toString();
    }
    for (int i = 0; i < res.size(); i++) {
      Fault fault = res.get(i);
      html.append("<tr>");
      html.append("<td>" + fault.getTimeOfFailure() + "</td>");
      html.append("<td>" + fault.getType() + "</td>");
      html.append("<td>" + fault.getLevel() + "</td>");
      html.append("<td>" + fault.getWorkerNodeName() + "</td>");
      html.append("<td>" + fault.getJobName() + "</td>");
      html.append("<td>" + fault.getStaffName() + "</td>");
      html.append("<td>" + fault.isFaultStatus() + "</td>");
      html.append("<td>" + fault.getExceptionMessage() + "</td>");
      html.append("</tr>");
    }
    int totalPages = service.getTotalPages();
    html.append("<tr>");
    if (pageNumber <= 1) {
      html.append("<td>first page</td>");
      html.append("<td>previous page</td>");
      html.append("<td><a href='" + path + "?pageNumber=" + 2 + "&key=" +
          getKey() + "&month=" + getMonth() + "'>next page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + totalPages +
          "&key=" + getKey() + "&month=" + getMonth() +
          "'>last page</a></td>");
    } else if (pageNumber > 1 && pageNumber < totalPages) {
      html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&key=" +
          getKey() + "&month=" + getMonth() + "'>first page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + (pageNumber - 1) +
          "&key=" + getKey() + "&month=" + getMonth() +
          "'>previous page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + (pageNumber + 1) +
          "&key=" + getKey() + "&month=" + getMonth() +
          "'>next page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + totalPages +
          "&key=" + getKey() + "&month=" + getMonth() +
          "'>last page</a></td>");
    } else if (pageNumber == totalPages) {
      html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&key=" +
          getKey() + "&month=" + getMonth() + "'>first page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + (pageNumber - 1) +
          "&key=" + getKey() + "&month=" + getMonth() +
          "'>previous page</a></td>");
      html.append("<td>next page</td>");
      html.append("<td>last page</td>");
    }
    html.append("<td>currentPage " + pageNumber + "</td>");
    html.append("<td>TotalPage " + totalPages + "</td>");
    html.append("</tr>");
    html.append("</table>");
    html.append("</font>");
    return html.toString();
  }

  /**
   * get the first page into html.
   * @return first page html information.
   */
  public String getFirstPage() {
    StringBuffer html = new StringBuffer();
    html.append("<font size = '3' >");
    html.append("<table frame = 'box'  >");
    html.append("<tr><td width='59' bgcolor='#78A5D1'>TIME</td><td width='59'" +
      "bgcolor='#78A5D1'>TYPE</td><td width='59'" +
      "bgcolor='#78A5D1'>LEVEL</td><td width='59'" +
      "bgcolor='#78A5D1'>WORKER</td>");
    html.append("<td width='59' bgcolor='#78A5D1'>JOBNAME</td><td width='59'" +
      "bgcolor='#78A5D1'>STAFFNAME</td><td width='59'" +
      "bgcolor='#78A5D1'>STATUS</td><td width='59'" +
      "bgcolor='#78A5D1'>EXCEPTIONMESSAGE</td></tr>");
    html.append("</table>");
    html.append("</font>");
    return html.toString();
  }

  /**
   * get the page into html which contains the fault information get from page
   * according to key
   * @param path
   *        fault storagepath ?
   * @return html in string
   */
  public String getPageByKeys(String path) {
    String[] keys = {getType(), getTime(), getLevel(), getWorker()};
    MessageService service = new MessageService();
    String monthNum = getMonth();
    List<Fault> res;
    if (monthNum == null || monthNum.equals("") || monthNum.equals("null")) {
      res = service.getPageByKeys(pageNumber, keys);
    } else {
      int month = Integer.valueOf(monthNum);
      if (month < 0 || month > 20) {
        res = service.getPageByKeys(pageNumber, keys);
      } else {
        res = service.getPageByKeys(pageNumber, keys, month);
      }
    }
    StringBuffer html = new StringBuffer();
    html.append("<font size = '3' >");
    html.append("<table frame = 'box'  >");
    html.append("<tr><td width='59' bgcolor='#78A5D1'>TIME</td><td width='59'" +
      "bgcolor='#78A5D1'>TYPE</td><td width='59'" +
      "bgcolor='#78A5D1'>LEVEL</td><td width='59'" +
      "bgcolor='#78A5D1'>WORKER</td>");
    html.append("<td width='59' bgcolor='#78A5D1'>JOBNAME</td><td width='59'" +
      "bgcolor='#78A5D1'>STAFFNAME</td><td width='59'" +
      "bgcolor='#78A5D1'>STATUS</td><td width='59'" +
      "bgcolor='#78A5D1'>EXCEPTIONMESSAGE</td></tr>");
    if (res.size() == 0) {
      html.append("</table>");
      html.append("<label>no such records</label>");
      html.append("</font>");
      return html.toString();
    }
    for (int i = 0; i < res.size(); i++) {
      Fault fault = res.get(i);
      html.append("<tr>");
      html.append("<td>" + fault.getTimeOfFailure() + "</td>");
      html.append("<td>" + fault.getType() + "</td>");
      html.append("<td>" + fault.getLevel() + "</td>");
      html.append("<td>" + fault.getWorkerNodeName() + "</td>");
      html.append("<td>" + fault.getJobName() + "</td>");
      html.append("<td>" + fault.getStaffName() + "</td>");
      html.append("<td>" + fault.isFaultStatus() + "</td>");
      html.append("<td>" + fault.getExceptionMessage() + "</td>");
      html.append("</tr>");
    }
    int totalPages = service.getTotalPages();
    html.append("<tr>");
    if (pageNumber <= 1) {
      html.append("<td>first page</td>");
      html.append("<td>previous page</td>");
      html.append("<td><a href='" + path + "?pageNumber=" + 2 + "&type=" +
           getType() + "&level=" + getLevel() + "&time=" + getTime() +
          "&worker=" + getWorker() + "&month=" + getMonth() +
          "'>next page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + totalPages +
          "&type=" + getType() + "&level=" + getLevel() + "&time=" +
          getTime() + "&worker=" + getWorker() + "&month=" + getMonth() +
          "'>last page</a></td>");
    } else if (pageNumber > 1 && pageNumber < totalPages) {
      html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&type=" +
          getType() + "&level=" + getLevel() + "&time=" + getTime() +
          "&worker=" + getWorker() + "&month=" + getMonth() +
         "'>first page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + (pageNumber - 1) +
          "&type=" + getType() + "&level=" + getLevel() + "&time=" +
          getTime() + "&worker=" + getWorker() + "&month=" + getMonth() +
          "'>previous page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + (pageNumber + 1) +
          "&type=" + getType() + "&level=" + getLevel() + "&time=" +
          getTime() + "&worker=" + getWorker() + "&month=" + getMonth() +
          "'>next page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + totalPages +
          "&type=" + getType() + "&level=" + getLevel() + "&time=" +
          getTime() + "&worker=" + getWorker() + "&month=" + getMonth() +
          "'>last page</a></td>");
    } else if (pageNumber == totalPages) {
      html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&type=" +
          getType() + "&level=" + getLevel() + "&time=" + getTime() +
          "&worker=" + getWorker() + "&month=" + getMonth() +
          "'>first page</a></td>");
      html.append("<td><a href='" + path + "?pageNumber=" + (pageNumber - 1) +
          "&type=" + getType() + "&level=" + getLevel() + "&time=" +
          getTime() + "&worker=" + getWorker() + "&month=" + getMonth() +
          "'>previous page</a></td>");
      html.append("<td>next page</td>");
      html.append("<td>last page</td>");
    }
    html.append("<td>currentPage " + pageNumber + "</td>");
    html.append("<td>TotalPage " + totalPages + "</td>");
    html.append("</tr>");
    html.append("</table>");
    html.append("</font>");
    return html.toString();
  }
}
