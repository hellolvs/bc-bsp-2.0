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

package com.chinamobile.bcbsp.io.mysql;

import com.chinamobile.bcbsp.io.mysql.DBInputFormat.NullDBWritable;
import com.chinamobile.bcbsp.util.BSPJob;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A container for configuration property names for jobs with
 * DB input/output. <br>
 * The job can be configured using the static methods in this class,
 * {@link DBInputFormat}, and {@link DBOutputFormat}.
 * <p>
 * Alternatively, the properties can be set in the configuration with proper
 * values.
 *
 * @see DBConfiguration#configureDB(JobConf, String, String, String, String)
 * @see DBInputFormat#setInput(JobConf, Class, String, String)
 * @see DBInputFormat#setInput(JobConf, Class, String, String, String,
 *      String...)
 * @see DBOutputFormat#setOutput(JobConf, String, String...)
 */
public class DBConfiguration {
  /** The JDBC Driver class name */
  public static final String DRIVER_CLASS_PROPERTY = "mapred.jdbc.driver.class";

  /** JDBC Database access URL */
  public static final String URL_PROPERTY = "mapred.jdbc.url";

  /** User name to access the database */
  public static final String USERNAME_PROPERTY = "mapred.jdbc.username";

  /** Password to access the database */
  public static final String PASSWORD_PROPERTY = "mapred.jdbc.password";

  /** Input table name */
  public static final String INPUT_TABLE_NAME_PROPERTY =
      "mapred.jdbc.input.table.name";

  /** Class name implementing DBWritable which will hold input tuples */
  public static final String INPUT_CLASS_PROPERTY = "mapred.jdbc.input.class";

  /** Output table name */
  public static final String OUTPUT_TABLE_NAME_PROPERTY =
      "mapred.jdbc.output.table.name";
  /** The current job */
  private BSPJob job;

  /**
   * Constructor.
   *
   * @param job The current job
   */
  public DBConfiguration(BSPJob job) {
    this.job = job;
  }

  /**
   * Sets the DB access related fields in the JobConf.
   *
   * @param job
   *        the job
   * @param driverClass
   *        JDBC Driver class name
   * @param dbUrl
   *        JDBC DB access URL.
   * @param userName
   *        DB access username
   * @param passwd
   *        DB access passwd
   */
  public static void configureDB(BSPJob job, String driverClass, String dbUrl,
      String userName, String passwd) {
    job.set(DRIVER_CLASS_PROPERTY, driverClass);
    job.set(URL_PROPERTY, dbUrl);
    if (userName != null) {
      job.set(USERNAME_PROPERTY, userName);
    }
    if (passwd != null) {
      job.set(PASSWORD_PROPERTY, passwd);
    }
  }

  /**
   * Sets the DB access related fields in the JobConf.
   *
   * @param job
   *        the job
   * @param driverClass
   *        JDBC Driver class name
   * @param dbUrl
   *        JDBC DB access URL.
   */
  public static void configureDB(BSPJob job, String driverClass, String dbUrl) {
    configureDB(job, driverClass, dbUrl, null, null);
  }

  /**
   * Returns a connection object o the DB.
   *
   * @return a connection object o the DB.
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public Connection getConnection()
      throws ClassNotFoundException, SQLException {
    Class.forName(job.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
    if (job.get(DBConfiguration.USERNAME_PROPERTY) == null) {
      return DriverManager.getConnection(job.get(DBConfiguration.URL_PROPERTY));
    } else {
      return DriverManager.getConnection(job.get(DBConfiguration.URL_PROPERTY),
          job.get(DBConfiguration.USERNAME_PROPERTY),
          job.get(DBConfiguration.PASSWORD_PROPERTY));
    }
  }

  String getInputTableName() {
    return job.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
  }

  /**
   * Set input name of the table.
   *
   * @param tableName input name of the table.
   */
  void setInputTableName(String tableName) {
    job.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
  }

  Class<?> getInputClass() {
    return job.getClass(DBConfiguration.INPUT_CLASS_PROPERTY,
        NullDBWritable.class);
  }

  /**
   * Set intput Class.
   *
   * @param inputClass intput Class.
   */
  void setInputClass(Class<? extends DBWritable> inputClass) {
    job.setClass(DBConfiguration.INPUT_CLASS_PROPERTY, inputClass,
        DBWritable.class);
  }

  String getOutputTableName() {
    return job.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);
  }

  /**
   * Set output name of the table.
   *
   * @param tableName output name of the table.
   */
  void setOutputTableName(String tableName) {
    job.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
  }
}
