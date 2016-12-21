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

import com.chinamobile.bcbsp.io.InputFormat;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.commons.logging.Log;

/**
 * A InputFormat that reads input data from an SQL table.
 * <p>
 * DBInputFormat emits LongWritables containing the record number as key and
 * DBWritables as value. The SQL query, and input class can be using one of the
 * two setInput methods.
 */
public class DBInputFormat extends InputFormat<String, String> {
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(DBInputFormat.class);
  /** a connection object o the DB. */
  private Connection connection;
  /** input name of the table */
  private String tableName;
  /** A container for configuration property names for jobs with
   *  DB input/output. */
  private DBConfiguration dbConf;
  /** Mysql query results */
  private ResultSet results;
  /** a static SQL statement */
  private Statement statement;

  /**
   * A RecordReader that reads records from a SQL table. Emits LongWritables
   * containing the record number as key and DBWritables as value.
   */
  protected class DBRecordReader extends RecordReader<String, String> {
    /** the vertex */
    private String key;
    /** the whole list of edges of the vertex. */
    private String value;

    /**
     * @param split
     *        The InputSplit to read data for
     * @param job
     *        The current job
     * @throws SQLException
     */
    protected DBRecordReader(DBInputSplit split, BSPJob job)
        throws SQLException {
      configure(job);
      statement = connection.createStatement();
      StringBuilder query = new StringBuilder();
      query.append("select * from ");
      query.append(tableName);
      try {
        query.append(" LIMIT ").append(split.getLength());
      } catch (IOException e) {
        LOG.error("[query]", e);
        e.printStackTrace();
      }
      query.append(" OFFSET ").append(split.getStart());
      results = statement.executeQuery(query.toString());
    }

    /** Close the SQL connection and query results */
    public void close() throws IOException {
      try {
        statement.close();
        connection.close();
        results.close();
      } catch (SQLException ex) {
        LOG.error("[close]", ex);
        throw new IOException(ex.getMessage());
      }
    }

    @Override
    public String getCurrentKey() {
      return key;
    }

    public String getCurrentValue() {
      return value;
    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      try {
        if (!results.next()) {
          close();
          return false;
        } else {
          key = results.getString(1);
          value = results.getString(2);
        }
      } catch (SQLException ex) {
        LOG.error("[nextKeyValue]", ex);
        throw new IOException(ex.getMessage());
      }
      return true;
    }

    @Override
    public void initialize(InputSplit genericSplit, Configuration conf)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
    }
  }

  /**
   * A Class that does nothing, implementing DBWritable
   */
  public static class NullDBWritable implements DBWritable, Writable {
    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void readFields(ResultSet arg0) throws SQLException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void write(PreparedStatement arg0) throws SQLException {
    }
  }

  /** Connect the DB
   *
   * @param job The current job
   */
  public void configure(BSPJob job) {
    dbConf = new DBConfiguration(job);
    try {
      tableName = dbConf.getInputTableName();
      this.connection = dbConf.getConnection();
    } catch (Exception ex) {
      LOG.error("[configure]", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Get the record reader for every given split.
   *
   * @param split
   *            The split to work with.
   * @param job
   *            The current job Job.
   * @return The newly created record reader.
   */
  @SuppressWarnings("unchecked")
  public RecordReader<String, String> createRecordReader(InputSplit split,
      BSPJob job) throws IOException {
    try {
      return new DBRecordReader((DBInputSplit) split, job);
    } catch (SQLException ex) {
      LOG.error("[createRecordReader]", ex);
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * @return the query for getting the total number of rows, subclasses can
   * override this for custom behaviour.
   */
  protected String getCountQuery() {
    StringBuilder query = new StringBuilder();
    query.append("SELECT COUNT(*) FROM " + tableName);
    return query.toString();
  }

  /**
   * Initializes the map-part of the job with the appropriate input settings.
   *
   * @param job
   *        The job
   * @param inputClass
   *        the class object implementing DBWritable, which is the Java object
   *        holding tuple fields.
   * @param tableName
   *        The table to read data from
   * @see #setInput(JobConf, Class, String, String)
   */
  public static void setInput(BSPJob job,
      Class<? extends DBWritable> inputClass, String tableName) {
    DBConfiguration dbConf = new DBConfiguration(job);
    dbConf.setInputClass(inputClass);
    dbConf.setInputTableName(tableName);
  }

  @Override
  public List<InputSplit> getSplits(BSPJob job) throws IOException,
      InterruptedException {
    int chunks = 1;
    chunks = job.getNumBspStaff();
    try {
      configure(job);
      statement = connection.createStatement();
      results = statement.executeQuery(getCountQuery());
      results.next();
      long count = results.getLong(1);
      long chunkSize = count / chunks;
      close();
      List<InputSplit> splits = new ArrayList<InputSplit>(chunks);
      // Split the rows into n-number of chunks and adjust the last chunk
      // accordingly
      for (int i = 0; i < chunks; i++) {
        DBInputSplit split;
        if ((i + 1) == chunks) {
          split = new DBInputSplit(i * chunkSize, count);
        } else {
          split = new DBInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
        }
        splits.add(split);
      }
      return splits;
    } catch (SQLException ex) {
      LOG.error("[getSplits]", ex);
      throw new IOException(ex.getMessage());
    }
  }

  /** Close the SQL connection and query results */
  public void close() throws IOException {
    try {
      statement.close();
      connection.close();
      results.close();
    } catch (SQLException ex) {
      LOG.error("[close]", ex);
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * For JUnit test.
   */
  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public Statement getStatement() {
    return statement;
  }

  public void setStatement(Statement statement) {
    this.statement = statement;
  }

  public ResultSet getResults() {
    return results;
  }

  public void setResults(ResultSet results) {
    this.results = results;
  }
}
