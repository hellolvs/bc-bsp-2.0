
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

/**
 *
 * BSPHBZKUtilImpl A concrete implementation class.
 *
 */
public class BSPHBZKUtilImpl {
  /**
   * constructor
   */
  private BSPHBZKUtilImpl() {

  }

  /**
   * A method that encapsulates applyClusterKeyToConf
   * (Configuration conf, String key).
   * @param conf
   *        Configuration
   * @param key
   *        String
   * @throws IOException
   */
  public static void applyClusterKeyToConf(Configuration conf, String key)
      throws IOException {
    ZKUtil.applyClusterKeyToConf(conf, key);
  }

}
