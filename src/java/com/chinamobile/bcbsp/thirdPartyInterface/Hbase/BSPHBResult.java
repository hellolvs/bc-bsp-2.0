
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase;

/**
 *
 * BSPHBResult An method that encapsulates Result.
 *
 */
public interface BSPHBResult {
  /**
   * A method that encapsulates size().
   * @return
   *        The value of a int type
   */
  int size();

  /**
   * A method that encapsulates getRow().
   * @return
   *       The value of a byte array type
   */
  byte[] getRow();

}
