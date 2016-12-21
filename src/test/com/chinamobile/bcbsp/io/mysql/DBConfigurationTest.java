package com.chinamobile.bcbsp.io.mysql;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.util.BSPJob;

public class DBConfigurationTest {
  private DBConfiguration db;
  private BSPConfiguration conf;
  private BSPJob job;
  
  @Before
  public void setUp() throws Exception { 
    conf = new BSPConfiguration();
//    conf.set(Constants.USER_BC_BSP_JOB_EXE, new Path(args[1]).toString());
    conf.set(Constants.USER_BC_BSP_JOB_TYPE, "C++");
    job = new BSPJob(conf,2);
    job.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "input");
    DBConfiguration.configureDB(job, "com.mysql.jdbc.Driver",
        "jdbc:mysql://localhost:3306/test", "root", "root");
    db = new DBConfiguration(job);
  }
  
  @Test
  public void testDBConfiguration() throws IOException {
    db = new DBConfiguration(job);
    System.out.println(db);
  }
  
  @Test
  public void testConfigureDBBSPJobStringStringStringString() {
 
    DBConfiguration.configureDB(job, "com.mysql.jdbc.Driver",
        "jdbc:mysql://localhost:3306/test", "root", "root");
   
    System.out.println(job.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY));
    
    System.out.println(job.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
    System.out.println(job.get(DBConfiguration.URL_PROPERTY));
    System.out.println(job.get(DBConfiguration.USERNAME_PROPERTY));
    System.out.println(job.get(DBConfiguration.PASSWORD_PROPERTY));
  }
  
  @Test
  public void testConfigureDBBSPJobStringString() {
    DBConfiguration.configureDB(job, "com.mysql.jdbc.Driver",
        "jdbc:mysql://localhost:3306/test");
    
    System.out.println(job.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
    System.out.println(job.get(DBConfiguration.URL_PROPERTY));
    System.out.println(job.get(DBConfiguration.USERNAME_PROPERTY));
    System.out.println(job.get(DBConfiguration.PASSWORD_PROPERTY));
  }
  
  @Test
  public void testGetConnection() throws ClassNotFoundException, SQLException {
    db.getConnection();
    System.out.println(db.getConnection().toString());
  }
  
}
