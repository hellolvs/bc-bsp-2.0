
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBbase64;

import org.apache.hadoop.hbase.util.Base64;

/**
 * BSPHBBase64Impl A concrete class that implements interface BSPHBbase64.
 */
public class BSPHBBase64Impl implements BSPHBbase64 {
  /** State a Base64 type of variable base */
  private Base64 base;

  @Override
  public String encodeBytes(byte[] source) {
    return Base64.encodeBytes(source);
  }

  @Override
  public byte[] decode(String string) {
    return Base64.decode(string);
  }

}
