
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import java.io.IOException;
import java.util.Iterator;

import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBResult;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBResultScanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 *
 * BSPHBResultScannerImpl A concrete class
 *  that implements interface BSPHBResultScanner.
 *
 */
public class BSPHBResultScannerImpl implements BSPHBResultScanner {
  /** Define Log variable output messages */
  private static final Log LOG = LogFactory
      .getLog(BSPHBResultScannerImpl.class);
  /** set ResultScanner */
  private ResultScanner rs = null;

  @Override
  public void close() {
    rs.close();
  }

  @Override
  public BSPHBResult next() {
    try {
      return (BSPHBResult) rs.next();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.info("Exception has happened and been catched! Next failed!", e);
      return null;
    }

  }

  /**
   * A method that encapsulates next(int arg0).
   * @param arg0
   *        int
   * @return
   *        Result[]
   * @throws IOException
   */
  public Result[] next(int arg0) throws IOException {

    return rs.next(arg0);
  }

  /**
   * A method that encapsulates iterator().
   * @return
   *        Iterator<Result>
   */
  public Iterator<Result> iterator() {
    return rs.iterator();
  }
}
