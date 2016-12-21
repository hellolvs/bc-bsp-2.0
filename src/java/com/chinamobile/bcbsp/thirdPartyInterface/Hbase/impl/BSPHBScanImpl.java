
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBScan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;

/**
 *
 * BSPHBScanImpl A concrete class that implements interface BSPHBScan.
 *
 */
public class BSPHBScanImpl implements BSPHBScan {
  /** Define Log variable output messages */
  private static final Log LOG = LogFactory.getLog(BSPHBScanImpl.class);
  /** State Scan */
  private Scan scan;

  /**
   * constructor
   */
  public BSPHBScanImpl() {
    scan = new Scan();
  }

  /**
   * constructor
   * @param scan1
   *        BSPHBScan
   * @throws IOException
   */
  public BSPHBScanImpl(BSPHBScan scan1) throws IOException {
    scan = new Scan((Scan) scan1);
  }

  @Override
  public void write(DataOutput data) {
    try {
      scan.write(data);
    } catch (IOException e) {
      // TODO Auto-generated catch block
     // LOG.info("Exception has happened and been catched! Write failed!", e);
    	throw new RuntimeException("Exception has happened and been catched! Write failed!",e);
    }
  }

  @Override
  public void readFields(DataInput data) {
    try {
      scan.readFields(data);
    } catch (IOException e) {
      // TODO Auto-generated catch block
//      LOG.info("Exception has happened and been catched! " +
//          "readFields failed!", e);
    	throw new RuntimeException("Exception has happened and been catched!readFields failed!",e);
    
    }
  }

  @Override
  public Scan addColumn(byte[] family, byte[] qualifier) {
    return scan.addColumn(family, qualifier);
  }

  @Override
  public Scan addFamily(byte[] family) {
    return scan.addFamily(family);
  }

  @Override
  public Scan setTimeStamp(long arg) {
    return scan.setTimeStamp(arg);
  }

  @Override
  public Scan setTimeRange(long minStamp, long maxStamp) {
    try {
      return scan.setTimeRange(minStamp, maxStamp);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.info("Exception has happened and been catched! setTimeRange failed!",
          e);
      return null;
    }

  }

  @Override
  public Scan setMaxVersions(int maxVersions) {
    return scan.setMaxVersions(maxVersions);
  }

  @Override
  public void setCaching(int caching) {
    scan.setCaching(caching);
  }

  @Override
  public void setCacheBlocks(boolean cacheBlocks) {
    scan.setCacheBlocks(cacheBlocks);
  }

  @Override
  public Scan setStartRow(byte[] startRow) {
    return scan.setStartRow(startRow);
  }

  @Override
  public Scan setStopRow(byte[] stopRow) {
    return scan.setStopRow(stopRow);
  }

  @Override
  public byte[] getStartRow() {
    return scan.getStartRow();
  }

  @Override
  public byte[] getStopRow() {
    return scan.getStopRow();
  }
}
