
package com.chinamobile.bcbsp.io.mysql;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

public class DBOutputFormatTest {
  /** a connection object o the DB. */
  private Connection connection;
  private BSPConfiguration conf;
  private BSPJob job;
  private static String[] datas = {
  
    "0:10.0\t3:0 1:0 2:0 4:0 0:0 0:0 0:0 0:0 0:0 1:0 0:0 1:0 3:0",

    "1:10.0\t4:0 2:0 0:0 2:0 4:0 1:0 2:0 1:0 1:0 3:0 3:0 3:0 4:0 0:0",

    "2:10.0\t2:0 1:0 4:0 2:0 3:0 3:0 0:0 3:0 0:0 1:0 3:0 4:0 1:0",

    "3:10.0\t4:0 2:0 3:0 0:0 0:0 1:0 2:0 0:0 1:0 2:0 0:0 4:0 0:0 1:0",

    "4:10.0\t4:0 0:0 1:0 1:0 4:0 2:0 1:0 1:0 0:0 4:0 1:0 2:0 3:0 0:0 2:0 4:0"};
  
  @Before
  public void setUp() throws Exception {
    conf = new BSPConfiguration();
    job = new BSPJob(conf, 2);
    job.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, "output");
    DBConfiguration.configureDB(job, "com.mysql.jdbc.Driver",
        "jdbc:mysql://localhost:3306/test", "root", "root");
  }
  
  @Test
  public void testWrite() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    Text keyValue = new Text();
    Text value = new Text();
    DBConfiguration dbConf = new DBConfiguration(job);
    Connection connection = dbConf.getConnection();
    DBOutputFormat output = new DBOutputFormat();
    DBOutputFormat.DBRecordWriter record = output.new DBRecordWriter(
        connection);
    output.getRecordWriter(job, new StaffAttemptID());
    for(int i = 0; i < 5; i++){
      keyValue = new Text(datas[i]);
      record.write(keyValue, value);
    }
  }
}
