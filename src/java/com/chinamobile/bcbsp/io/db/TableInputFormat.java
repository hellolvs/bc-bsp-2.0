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

import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBScan;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl.BSPHBBytesImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl.BSPHBScanImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl.BSPHBaseConfImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl.BSPTableImpl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * Convert HBase tabular data into a format that is consumable by BCBSP job.
 */
public class TableInputFormat extends TableInputFormatBase {
  /** Job parameter that specifies the input table. */
  public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";

  /** Column Family to Scan */
  public static final String SCAN_COLUMN_FAMILY =
      "hbase.mapreduce.scan.column.family.input";

  /** Space delimited list of columns to scan. */
  public static final String SCAN_COLUMNS =
      "hbase.mapreduce.scan.columns.input";

  /** The timestamp used to filter columns with a specific timestamp. */
  public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";
  /**
   * The starting timestamp used to filter columns with a specific range of
   * versions.
   */
  public static final String SCAN_TIMERANGE_START =
      "hbase.mapreduce.scan.timerange.start";

  /**
   * The ending timestamp used to filter columns with a specific range of
   * versions.
   */
  public static final String SCAN_TIMERANGE_END =
      "hbase.mapreduce.scan.timerange.end";

  /** The maximum number of version to return. */
  public static final String SCAN_MAXVERSIONS =
      "hbase.mapreduce.scan.maxversions";

  /** Set to false to disable server-side caching of blocks for this scan. */
  public static final String SCAN_CACHEBLOCKS =
      "hbase.mapreduce.scan.cacheblocks";

  /** The number of rows for caching that will be passed to scanners. */
  public static final String SCAN_CACHEDROWS =
      "hbase.mapreduce.scan.cachedrows";

  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(TableInputFormat.class);
  // Changed by Zhicheng Liu
  // 2013.4.17
  // Reason:"Scan.addColumns()" do not suit the new version of HBase API
  @Override
  public void initialize(Configuration configuration) {
    this.conf = HBaseConfiguration.create(configuration);
    this.conf.set("hbase.master", configuration.get("hbase.master"));
    this.conf.set("hbase.zookeeper.quorum",
            configuration.get("hbase.zookeeper.quorum"));
    //this.conf.set(INPUT_TABLE, configuration.get(INPUT_TABLE));
    this.conf
            .set(SCAN_COLUMN_FAMILY, configuration.get(SCAN_COLUMN_FAMILY));
    String tableName = conf.get(INPUT_TABLE);
    try {
        setTable(new HTable(conf, tableName));
    } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
    }
    Scan scan = null;
    try {
        scan = new Scan();
        if (conf.get(SCAN_COLUMNS) != null) {
            //scan.addColumns(conf.get(SCAN_COLUMNS));    
            scan.addColumn(Bytes.toBytes(conf.get(SCAN_COLUMN_FAMILY)), Bytes.toBytes(conf.get(SCAN_COLUMNS)));
        }
        if (conf.get(SCAN_COLUMN_FAMILY) != null) {
            scan.addFamily(Bytes.toBytes(conf.get(SCAN_COLUMN_FAMILY)));
        }
        if (conf.get(SCAN_TIMESTAMP) != null) {
            scan.setTimeStamp(Long.parseLong(conf.get(SCAN_TIMESTAMP)));
        }
        if (conf.get(SCAN_TIMERANGE_START) != null
            && conf.get(SCAN_TIMERANGE_END) != null) {
            scan.setTimeRange(
                Long.parseLong(conf.get(SCAN_TIMERANGE_START)),
                Long.parseLong(conf.get(SCAN_TIMERANGE_END)));
        }
        if (conf.get(SCAN_MAXVERSIONS) != null) {
            scan.setMaxVersions(Integer.parseInt(conf
                .get(SCAN_MAXVERSIONS)));
        }
        if (conf.get(SCAN_CACHEDROWS) != null) {
            scan.setCaching(Integer.parseInt(conf.get(SCAN_CACHEDROWS)));
        }
        // false by default, full table scans generate too much BC churn
        scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));
    } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
    }
    setScan(scan);
}
}