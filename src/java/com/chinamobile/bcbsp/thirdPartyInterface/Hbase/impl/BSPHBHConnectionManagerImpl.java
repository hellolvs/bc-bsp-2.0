
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import org.apache.hadoop.hbase.client.HConnectionManager;

/**
 *
 * BSPHBHConnectionManagerImpl An implementation class.
 *
 */
public class BSPHBHConnectionManagerImpl {
  /**
   * constructor
   */
  private BSPHBHConnectionManagerImpl() {

  }

  /**
   * A method that delete all connections.
   * @param b
   *        boolean
   */
  public static void deleteAllConnections(boolean b) {
    HConnectionManager.deleteAllConnections(b);
  }

}
