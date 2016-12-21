/*
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

package com.chinamobile.bcbsp.sync;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPIds;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPCreateModeImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPZookeeperImpl;

/**
 * SynchronizationServer This is an implementation for ZooKeeper.
 * @version 1.0
 */
public class SynchronizationServer implements SynchronizationServerInterface,
    Watcher {
  /**The log of the class.*/
  private static final Log LOG = LogFactory.getLog(SynchronizationServer.class);
  /**The configuration of the bsp.*/
  private BSPConfiguration conf;
  /**The zookeeper of the bsp.*/
  private BSPZookeeper bspzk = null;
  /**The zookeeper address.*/
  private final String zookeeperAddr;
  /**The zookeeper of the bsp.*/
  private final String bspZKRoot;
  /**The volatile variable of the thread.*/
  private volatile Integer mutex = 0;
  /**
   * constructor of the class.
   */
  public SynchronizationServer() {
    this.conf = new BSPConfiguration();
    this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
        + ":"
        + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
            Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
  }
  
  /** For JUnit test. */
  public SynchronizationServer(BSPConfiguration conf) {
    this.conf = conf;
    this.zookeeperAddr =conf.get(Constants.ZOOKEEPER_QUORUM)
            + ":"
            + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
                Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
  }
  @Override
  public boolean startServer() {
    try {
      this.bspzk = new BSPZookeeperImpl(this.zookeeperAddr, 3000, this);
      if (this.bspzk != null) {
        if (bspzk.equaltoStat(this.bspZKRoot, false)) {
          deleteZKNodes(this.bspZKRoot);
        }
        this.bspzk.create(this.bspZKRoot, new byte[0], BSPIds.OPEN_ACL_UNSAFE,
            new BSPCreateModeImpl().getPERSISTENT());
      }
      return true;
    } catch (Exception e) {
      LOG.error("[startServer]", e);
      return false;
    }
  }
  @Override
  public boolean stopServer() {
    try {
      deleteZKNodes(this.bspZKRoot);
      return true;
    } catch (Exception e) {
      LOG.error("[stopServer]", e);
      return false;
    }
  }
  /**
   * To delete zookeeper node.
   * @param node the String of the node.
   * @throws Exception the Exception.
   */
  private void deleteZKNodes(String node) throws Exception {
    if (bspzk.equaltoStat(node, false)) {
      List<String> children = this.bspzk.getChildren(node, false);
      if (children.size() > 0) {
        for (String child : children) {
          deleteZKNodes(node + "/" + child);
        }
      }
      this.bspzk.delete(node, this.bspzk.exists(node, false).getVersion());
    }
  }
  @Override
  public void process(WatchedEvent event) {
    synchronized (mutex) {
      mutex.notify();
    }
  }
}
