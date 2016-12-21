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

import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * A OutputFormat that sends the reduce output to a SQL table.
 * <p>
 * {@link DBOutputFormat} accepts &lt;key,value&gt; pairs, where key has a type
 * extending DBWritable. Returned {@link RecordWriter} writes <b>only the
 * key</b> to the database with a batch SQL query.
 */
public class DBOutputFormat extends OutputFormat<Text, Text> {
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(DBOutputFormat.class);
  /** output name of the table */
  private String tableName;

  /**
   * A RecordWriter that writes the reduce output to a SQL table
   *
   * @param <V>
   */
  protected class DBRecordWriter extends RecordWriter<Text, Text> {
    /** a connection object o the DB. */
    private Connection connection;
    /** a static SQL statement */
    private java.sql.Statement statement;

    /**
     * Constructor
     *
     * @param connection A connection object o the DB.
     */
    protected DBRecordWriter(Connection connection) throws SQLException {
      this.connection = connection;
      statement = connection.createStatement();
    }

    @Override
    public void write(Text key, Text value) throws IOException,
        InterruptedException {
      StringBuilder sql = new StringBuilder();
      sql.append("insert into ");
      sql.append(tableName);
      sql.append(" values('");
      sql.append(key);
      sql.append("','");
      sql.append(value);
      sql.append("');");
      try {
        statement.execute(sql.toString());
      } catch (SQLException ex) {
        LOG.error("[write]", ex);
      }
    }

    @Override
    public void write(Text keyValue) throws IOException,
        InterruptedException {
//      String separator = "\t";
      Text key = new Text();
      Text value = new Text();
//      String[] kv = new String[2];
      StringTokenizer str = new StringTokenizer(keyValue.toString(), "\t");
      if (str.hasMoreElements()) {
        key.set(str.nextToken());
      }
      if (str.hasMoreElements()) {
        value.set(str.nextToken());
      }
      StringBuilder sql = new StringBuilder();
      sql.append("insert into ");
      sql.append(tableName);
      sql.append(" values('");
      sql.append(key);
      sql.append("','");
      sql.append(value);
      sql.append("');");
      try {
        statement.execute(sql.toString());
      } catch (SQLException ex) {
        LOG.error("[write]", ex);
      }
    }

    @Override
    public void close(BSPJob job) throws IOException, InterruptedException {
      try {
        statement.close();
        connection.close();
      } catch (SQLException ex) {
        LOG.error("[close]", ex);
        throw new IOException(ex.getMessage());
      }
    }
  }

  /**
   * Initializes the reduce-part of the job with the appropriate output settings
   *
   * @param job
   *        The job
   * @param staffId
   *        the id of this staff attempt
   * @return A RecordWriter that writes the reduce output to a SQL table
   */
  @Override
  public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
      StaffAttemptID staffId) throws IOException, InterruptedException {
    DBConfiguration dbConf = new DBConfiguration(job);
    tableName = dbConf.getOutputTableName();
    try {
      Connection connection = dbConf.getConnection();
      return new DBRecordWriter(connection);
    } catch (Exception ex) {
      LOG.error("[getRecordWriter]", ex);
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Initializes the reduce-part of the job with the appropriate output settings
   *
   * @param job
   *        The job
   * @param staffId
   *        the id of this staff attempt
   * @param writePath
   *        output path
   * @return A RecordWriter that writes the reduce output to a SQL table
   */
  @Override
  public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
      StaffAttemptID staffId, Path writePath) throws IOException,
      InterruptedException {
    DBConfiguration dbConf = new DBConfiguration(job);
    tableName = dbConf.getOutputTableName();
    try {
      Connection connection = dbConf.getConnection();
      return new DBRecordWriter(connection);
    } catch (Exception ex) {
      LOG.error("[getRecordWriter]", ex);
      throw new IOException(ex.getMessage());
    }
  }
}
