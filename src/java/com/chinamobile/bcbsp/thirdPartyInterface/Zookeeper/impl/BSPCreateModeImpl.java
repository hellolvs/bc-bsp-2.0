
package com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPCreateMode;

import org.apache.zookeeper.CreateMode;

/**
 *
 *BSPCreateModeImpl A concrete class that implements interface BSPCreateMode.
 *
 */
public class BSPCreateModeImpl implements BSPCreateMode {
  /** State CreateMode */
  private CreateMode createmode;

  @Override
  public CreateMode getEPHEMERAL() {
    return CreateMode.EPHEMERAL;
  }

  @Override
  public CreateMode getPERSISTENT() {
    return CreateMode.PERSISTENT;
  }
}
