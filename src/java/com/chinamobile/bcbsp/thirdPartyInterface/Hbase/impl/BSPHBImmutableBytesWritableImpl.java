
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBImmutableBytesWritable;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 *
 * BSPHBImmutableBytesWritableImpl A concrete class
 * that implements interface BSPHBImmutableBytesWritable.
 *
 */
public class BSPHBImmutableBytesWritableImpl implements
    BSPHBImmutableBytesWritable {
  /** set ImmutableBytesWritable key */
  private ImmutableBytesWritable key = null;

  /**
   * constructor
   */
  public BSPHBImmutableBytesWritableImpl() {
    key = new ImmutableBytesWritable();
  }

  @Override
  public void set(byte[] bs) {
    key.set(bs);
  }

  @Override
  public byte[] get() {
    return key.get();
  }

}
