
package com.chinamobile.bcbsp.io.mysql;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.io.InputFormat;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.util.BSPJob;

public class DBInputFormatTest {
  private BSPConfiguration conf;
  private BSPJob job;
  
  @Before
  public void setUp() throws Exception {
    conf = new BSPConfiguration();
    job = new BSPJob(conf, 2);
    job.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "input");
    DBConfiguration.configureDB(job, "com.mysql.jdbc.Driver",
        "jdbc:mysql://localhost:3306/test", "root", "root");
    // dbConf = new DBConfiguration(job);
  }
  
  @Test
  public void testGetSplits() throws IOException, InterruptedException,
      ClassNotFoundException, SQLException {
    
    Configuration confs = job.getConf();
    com.chinamobile.bcbsp.io.InputFormat<?, ?> input = ReflectionUtils
        .newInstance(DBInputFormat.class, confs);
    input.getSplits(job);
    
    assertEquals(2, input.getSplits(job).size());
  }
  
  @Test
  public void testCreateRecordReaderInputSplitBSPJob() throws IOException,
      InterruptedException {
    RecordReader input = null;
    InputFormat inputformat = (InputFormat) ReflectionUtils.newInstance(
        DBInputFormat.class, job.getConf());
    
    inputformat.initialize(job.getConf());
    
    input = inputformat.createRecordReader((new DBInputSplit(0, 2)), job);
    while (input.nextKeyValue()) {
      Text key = new Text(input.getCurrentKey().toString());
      System.out.println(key);
    }
  }
  
  @Test
  public void testGetCountQuery() {
    DBInputFormat dbInput = new DBInputFormat();
    dbInput.configure(job);
    
    assertEquals("SELECT COUNT(*) FROM input", dbInput.getCountQuery());
  }
  
  @Test
  public void nextKeyValue() throws SQLException, IOException {
    DBInputFormat input = new DBInputFormat();
    DBInputFormat.DBRecordReader record = input.new DBRecordReader(new DBInputSplit(0, 2), job);
    assertEquals(true, record.nextKeyValue());
  }
  
  @Test
  public void testClose() throws SQLException, Exception {
    DBInputFormat dbInput = new DBInputFormat();
    dbInput.configure(job);
    
    dbInput.setStatement(dbInput.getConnection().createStatement());
    dbInput.setResults(dbInput.getStatement().executeQuery(
        dbInput.getCountQuery()));
    
    System.out.println(dbInput.getConnection().isClosed());
    
    dbInput.close();
    
    assertEquals(true, dbInput.getConnection().isClosed());
  }
}
