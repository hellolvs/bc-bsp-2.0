
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase;

/**
 *
 * BSPHBbase64 An interface that encapsulates Base64.
 *
 */
public interface BSPHBbase64 {
  /**
   * A method that encapsulates encodeBytes(byte[] source).
   *
   * @param source
   *        The byte array type
   * @return
   *       string
   */
  String encodeBytes(byte[] source);
  /**
   * A method that encapsulates decode(String string).
   * @param string
   *        The value of a String type
   * @return
   *        A byte array
   */
  byte[] decode(String string);
}
