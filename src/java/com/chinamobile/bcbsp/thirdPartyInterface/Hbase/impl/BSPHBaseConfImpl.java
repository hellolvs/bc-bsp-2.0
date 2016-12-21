
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 *
 * BSPHBaseConfImpl An implementation class.
 *
 */
public class BSPHBaseConfImpl {
  /**
   * constructor
   */
  private BSPHBaseConfImpl() {

  }

  /**
   * A method that create the Configuration.
   * @param configuration
   *        Configuration
   * @return
   *        HBaseConfiguration.create(configuration)
   */
  public static Configuration create(Configuration configuration) {
    return HBaseConfiguration.create(configuration);
  }


}
