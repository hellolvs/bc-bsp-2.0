
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.hbase.client.Scan;

/**
 *
 * BSPHBScan An interface that encapsulates Scan.
 *
 */
public interface BSPHBScan {

  /**
   * A method that encapsulates write(DataOutput data).
   * @param data
   *        The value of a DataOutput type
   */
  void write(DataOutput data);

  /**
   * A method that encapsulates readFields(DataInput data).
   * @param data
   *        The value of a DataInput type
   */
  void readFields(DataInput data);

  /**
   * A method that encapsulates addColumn(byte[] family, byte[] qualifier).
   * @param family
   *        byte array
   * @param qualifier
   *        byte array
   * @return
   *        The value of a Scan type
   */
  Scan addColumn(byte[] family, byte[] qualifier);

  /**
   * A method that encapsulates addFamily(byte[] family).
   * @param family
   *        The value of a byte array
   * @return
   *        The value of a Scan type
   */
  Scan addFamily(byte[] family);

  /**
   * A method that encapsulates setTimeStamp(long arg).
   * @param arg
   *        The value of a long type
   * @return
   *        The value of a Scan type
   */
  Scan setTimeStamp(long arg);

  /**
   * A method that encapsulates setTimeRange(long minStamp, long maxStamp).
   * @param minStamp
   *        set the minimum Stamp
   * @param maxStamp
   *        set the maximum Stamp
   * @return
   *        The value of a Scan type
   */
  Scan setTimeRange(long minStamp, long maxStamp);

  /**
   *A method that encapsulates setMaxVersions(int maxVersions).
   * @param maxVersions
   *        set the maximum version
   * @return
   *        The value of a Scan type
   */
  Scan setMaxVersions(int maxVersions);

  /**
   * A method that encapsulates setCaching(int caching).
   * @param caching
   *        set the caching
   */
  void setCaching(int caching);

  /**
   * A method that encapsulates setCacheBlocks(boolean cacheBlocks).
   * @param cacheBlocks
   *        set the CacheBlocks
   */
  void setCacheBlocks(boolean cacheBlocks);

  /**
   * A method that encapsulates setStartRow(byte[] startRow).
   * @param startRow
   *        set startRow
   * @return
   *        The value of a Scan type
   */
  Scan setStartRow(byte[] startRow);

  /**
   * A method that encapsulates setStopRow(byte[] stopRow).
   * @param stopRow
   *        set StopRow
   * @return
   *        The value of a Scan type
   */
  Scan setStopRow(byte[] stopRow);

  /**
   * A method that encapsulates getStartRow().
   * @return
   *      The value of a byte array type
   */
  byte[] getStartRow();

  /**
   * A method that encapsulates getStopRow().
   * @return
   *      The value of a byte array type
   */
  byte[] getStopRow();
}
