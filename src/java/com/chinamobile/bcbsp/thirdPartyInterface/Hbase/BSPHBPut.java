
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase;

import org.apache.hadoop.hbase.client.Put;

/**
 * BSPHBPut An interface that encapsulates Put.
 */
public interface BSPHBPut {
/**
 * A method that encapsulates add(byte[] bytes, byte[] bytes2, byte[] bytes3).
 * @param bytes
 *        byte array
 * @param bytes2
 *        byte array
 * @param bytes3
 *        byte array
 * @return
 *      a value of Put type
 */
  Put add(byte[] bytes, byte[] bytes2, byte[] bytes3);

}
