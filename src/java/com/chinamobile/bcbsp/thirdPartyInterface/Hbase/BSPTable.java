
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Pair;

/**
 *
 * BSPTable An interface that encapsulates HTable.
 *
 */

public interface BSPTable {

  /**
   * A method that encapsulates table(Configuration conf, String string).
   * @param conf
   *        Configuration
   * @param string
   *        String
   * @return
   *        HTable
   */
  HTable table(Configuration conf, String string);

  /**
   * A method that encapsulates getTableDescriptor().
   * @return
   *       The value of a HTableDescriptor type
   */
  HTableDescriptor getTableDescriptor();

  /**
   * A method that encapsulates getScanner(byte[] bs).
   * @param bs
   *        byte array
   * @return
   *      The value of a ResultScanner type
   */
  ResultScanner getScanner(byte[] bs);

  /**
   * A method that encapsulates getStartEndKeys().
   * @return
   *      a pair of byte array
   */
  Pair<byte[][], byte[][]> getStartEndKeys();

  /**
   * A method that encapsulates getRegionLocation(String string).
   * @param string
   *        String
   * @return
   *       get the RegionLocation
   * @throws IOException
   */
  HRegionLocation getRegionLocation(String string) throws IOException;

  /**
   * A method that encapsulates getTableName().
   * @return
   *     get the TableName
   */
  byte[] getTableName();

  /**
   * A method that encapsulates setAutoFlush(boolean b).
   * @param b
   *        boolean
   */
  void setAutoFlush(boolean b);

  /**
   * A method that encapsulates getScanner(BSPHBScan scan).
   * @param scan
   *        BSPHBScan
   * @return
   *       get the Scanner
   * @throws IOException
   */
  ResultScanner getScanner(BSPHBScan scan) throws IOException;

  /**
   * A method that encapsulates flushCommits().
   * @throws IOException
   */
  void flushCommits() throws IOException;

  /**
   * A method that encapsulates put(BSPHBPut put).
   * @param put
   *        BSPHBPut
   * @throws IOException
   */
  void put(Put put) throws IOException;
}
