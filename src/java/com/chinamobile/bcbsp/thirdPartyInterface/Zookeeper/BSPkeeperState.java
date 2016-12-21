
package com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper;

import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 *
 * BSPkeeperState An interface that encapsulates KeeperState.
 *
 */
public interface BSPkeeperState {
  /**
   * get the keeperState
   * @return
   *        keeperState
   */
  KeeperState getSyncConnected();
}
