
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase;

/**
 *
 * BSPHBImmutableBytesWritable
 * An interface that encapsulates ImmutableBytesWritable.
 *
 */
public interface BSPHBImmutableBytesWritable {
  /**
   * A method that encapsulates set(byte[] bs).
   * @param bs
   *        The value of a byte array type
   */
  void set(byte[] bs);

  /**
   * A method that encapsulates get().
   * @return
   *      a byte array
   */
  byte[] get();

}
