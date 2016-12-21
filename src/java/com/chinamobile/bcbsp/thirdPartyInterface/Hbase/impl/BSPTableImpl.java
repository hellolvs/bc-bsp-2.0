
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBPut;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBScan;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPTable;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

/**
 *
 * BSPTableImpl A concrete class that implements interface BSPTable.
 *
 */
public class BSPTableImpl implements BSPTable {
  /** Define Log variable output messages */
  private static final Log LOG = LogFactory.getLog(BSPTableImpl.class);
  /** set HTable */
  private HTable table = null;

  /**
   * constructor
   * @param conf
   *        Configuration
   * @param tableName
   *        the name of table
   * @throws IOException
   */
  public BSPTableImpl(Configuration conf,
      String tableName) throws IOException {

    table = new HTable(conf, tableName);
  }

  @Override
  public HTable table(Configuration conf,
      String tableName) {
    HTable table1;
    try {
      table1 = new HTable(conf, tableName);
      return table1;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.info("Exception has happened and been catched! createtable failed!",
          e);
      return null;
    }

  }

  @Override
  public HTableDescriptor getTableDescriptor() {
    try {
      return table.getTableDescriptor();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.info(
          "Exception has happened and been catched! getTableDescriptor failed!",
          e);
      return null;
    }

  }

  @Override
  public ResultScanner getScanner(byte[] family) {
    try {
      return table.getScanner(family);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.info("Exception has happened and been catched! getScanner failed!",
          e);
      return null;
    }

  }

  @Override
  public Pair<byte[][], byte[][]> getStartEndKeys() {
    try {
      return table.getStartEndKeys();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.info(
          "Exception has happened and been catched! getStartEndKeys failed!",
          e);
      return null;
    }

  }

  @Override
  public HRegionLocation getRegionLocation(String row) throws IOException {
    return table.getRegionLocation(row);
  }

  @Override
  public byte[] getTableName() {
    return table.getTableName();
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    table.setAutoFlush(autoFlush);
  }

  @Override
  public ResultScanner getScanner(BSPHBScan scan) throws IOException {
    return table.getScanner((Scan) scan);
  }

  @Override
  public void flushCommits() throws IOException {
    table.flushCommits();
  }

  @Override
  public void put(Put put) throws IOException {
    table.put(put);
  }

}
