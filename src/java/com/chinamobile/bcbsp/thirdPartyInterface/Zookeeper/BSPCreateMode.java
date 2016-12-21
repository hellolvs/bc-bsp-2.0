
package com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper;

import org.apache.zookeeper.CreateMode;

/**
 *
 * BSPCreateMode An interface that encapsulates CreateMode.
 *
 */
public interface BSPCreateMode {

  /**
   * get the CreateMode.EPHEMERAL
   * @return
   *        CreateMode.EPHEMERAL
   */
  CreateMode getEPHEMERAL();

  /**
   *  get the CreateMode.PERSISTENT
   * @return
   *         CreateMode.PERSISTENT
   */
  CreateMode getPERSISTENT();
}
