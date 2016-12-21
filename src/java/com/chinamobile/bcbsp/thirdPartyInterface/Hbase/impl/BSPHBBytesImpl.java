
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * BSPHBBytesImpl An implementation class.
 *
 */
public class BSPHBBytesImpl {
  /**
   * constructor
   */
  private BSPHBBytesImpl() {

  }

  /**
   * A method that String toBytes.
   * @param string
   *        String
   * @return
   *        byte array
   */
  public static byte[] toBytes(String string) {
    return Bytes.toBytes(string);
  }

  /**
   * A method that read ByteArray.
   * @param in
   *        DataInput
   * @return
   *        byte array
   * @throws IOException
   */
  public static byte[] readByteArray(DataInput in) throws IOException {
    return Bytes.readByteArray(in);
  }

  /**
   * A method that byte array toString.
   * @param readByteArray
   *        byte array
   * @return
   *        String
   */
  public static String toString(byte[] readByteArray) {
    return Bytes.toString(readByteArray);
  }

  /**
   * A method that write ByteArray.
   * @param out
   *        DataOutput
   * @param bs
   *        byte array
   * @throws IOException
   */
  public static void writeByteArray(DataOutput out, byte[] bs)
      throws IOException {
    Bytes.writeByteArray(out, bs);
  }

  /**
   * A method that byte array toString.
   * @param startRow
   *        byte array
   * @return
   *        String
   */
  public static String toStringBinary(byte[] startRow) {
    return Bytes.toString(startRow);
  }

  /**
   * A method that compare a pair of byte array.
   * @param startRow
   *        byte array
   * @param startRow1
   *        byte array
   * @return
   *        int
   */
  public static int compareTo(byte[] startRow, byte[] startRow1) {
    return Bytes.compareTo(startRow, startRow1);
  }

  /**
   * A method that equal a pair of byte array.
   * @param tableName
   *        byte array
   * @param tableName1
   *        byte array
   * @return
   *        boolean
   */
  public static boolean equals(byte[] tableName, byte[] tableName1) {
    return Bytes.equals(tableName, tableName1);
  }
}
