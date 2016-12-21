/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chinamobile.bcbsp.io.db;

//import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBScan;
//import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBbase64;
//import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl.BSPHBBase64Impl;
//import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl.BSPHBScanImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Base64;

/**
 * Utility for TableInputFormat
 */
public class TableBCBSPJobUtil {
  /**
   * Writes the given scan into a Base64 encoded string.
   *
   * @param scan
   *        The scan to write out.
   * @return The scan saved in a Base64 encoded string.
   * @throws IOException
   *         When writing the scan fails.
   */
  static String convertScanToString(Scan scan) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(out);
    scan.write(dos);
//    BSPHBbase64 base64 = new BSPHBBase64Impl();
    return Base64.encodeBytes(out.toByteArray());
//    return base64.encodeBytes(out.toByteArray());
  }

  /**
   * Converts the given Base64 string back into a Scan instance.
   *
   * @param base64
   *        The scan details.
   * @return The newly created Scan instance.
   * @throws IOException
   *         When reading the scan instance fails.
   */
   static Scan convertStringToScan(String base64) throws IOException {
   ByteArrayInputStream bis = new ByteArrayInputStream(
   Base64.decode(base64));
   DataInputStream dis = new DataInputStream(bis);
   Scan scan = new Scan();
   scan.readFields(dis);
   return scan;
   }
//  static BSPHBScan convertStringToScan(String base64) throws IOException {
//    BSPHBbase64 base = new BSPHBBase64Impl();
//    ByteArrayInputStream bis = new ByteArrayInputStream(base.decode(base64));
//    DataInputStream dis = new DataInputStream(bis);
//    BSPHBScan scan = new BSPHBScanImpl();
//    scan.readFields(dis);
//    return scan;
//  }
}
