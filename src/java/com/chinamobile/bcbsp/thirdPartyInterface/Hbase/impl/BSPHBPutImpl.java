
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBPut;

import org.apache.hadoop.hbase.client.Put;

/**
 *
 * BSPHBPutImpl A concrete class that implements interface BSPHBPut.
 *
 */
public class BSPHBPutImpl implements BSPHBPut {
  /** set put */
  private Put put = null;

  /**
   * constructor
   * @param bytes
   *        byte array
   */
  public BSPHBPutImpl(byte[] bytes) {
    put = new Put(bytes);
  }

  @Override
  public Put add(byte[] family, byte[] qualifier, byte[] value) {
    return put.add(family, qualifier, value);
  }

}
