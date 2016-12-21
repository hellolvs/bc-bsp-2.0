
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase;

/**
 *
 * BSPHBResultScanner An interface that encapsulates ResultScanner.
 *
 */
public interface BSPHBResultScanner {

  /**
   * A method that encapsulates close().
   */
  void close();

  /**
   * A method that encapsulates next().
   * @return
   *      The value of a BSPHBResult type
   */
  BSPHBResult next();

}
