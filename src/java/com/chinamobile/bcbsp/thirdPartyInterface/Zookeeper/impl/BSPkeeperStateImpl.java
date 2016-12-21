
package com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPkeeperState;

import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 *
 * BSPkeeperStateImpl A concrete class that implements interface BSPkeeperState.
 *
 */
public class BSPkeeperStateImpl implements BSPkeeperState {
  /** State KeeperState */
  private KeeperState keeperstate;

  @Override
  public KeeperState getSyncConnected() {
    // TODO Auto-generated method stub
    return KeeperState.SyncConnected;
  }

}
