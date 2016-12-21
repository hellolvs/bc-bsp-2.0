
package com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper.States;

/**
 *
 * BSPZookeeperImpl A concrete class that implements interface BSPZookeeper.
 *
 */
public class BSPZookeeperImpl implements BSPZookeeper {
  /** State a Zookeeper type of variable zk */
  private ZooKeeper zk = null;


  /**
   * constructor
   * @param connectString
   *        comma separated host:port pairs, each corresponding to a zk
   *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
   *            the optional chroot suffix is used the example would look
   *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
   *            where the client would be rooted at "/app/a" and all paths
   *            would be relative to this root - ie getting/setting/etc...
   *            "/foo/bar" would result in operations being run on
   *            "/app/a/foo/bar" (from the server perspective).
   * @param sessionTimeout
   *         session timeout in milliseconds
   * @param watcher
   *        a watcher object which will be notified of state changes, may
   *            also be notified for node events
   *
   * @throws IOException
   */
  public BSPZookeeperImpl(String connectString, int sessionTimeout,
      Watcher watcher) throws IOException {
    // TODO Auto-generated method stub

    zk = new ZooKeeper(connectString, sessionTimeout, watcher);
  }

  @Override
  public void close() throws InterruptedException {
    // TODO Auto-generated method stub
    zk.close();
  }

  @Override
  public ZooKeeper getZk() {
    // TODO Auto-generated method stub
    return zk;
  }

  @Override
  public Stat exists(String path, boolean watch) throws KeeperException,
      InterruptedException {
    // TODO Auto-generated method stub
    return zk.exists(path, watch);
  }

  @Override
  public String create(String path, byte[] data, List<ACL> acl,
      CreateMode createMode) throws KeeperException, InterruptedException {
    // TODO Auto-generated method stub
    return zk.create(path, data, acl, createMode);
  }

  @Override
  public Stat setData(String path, byte[] data, int version)
      throws KeeperException, InterruptedException {
    // TODO Auto-generated method stub

    return zk.setData(path, data, version);
  }

  @Override
  public boolean equaltostat(String leader, boolean b) throws KeeperException,
      InterruptedException {
    // TODO Auto-generated method stub
    Stat s = null;
    s = zk.exists(leader, b);
    if (s == null) {
      return true;
    }
    return false;
  }

  @Override
  public boolean equaltoState() {
    // TODO Auto-generated method stub
    return States.CONNECTING == zk.getState();
  }

  @Override
  public byte[] getData(String path, boolean watch, Stat stat)
      throws KeeperException, InterruptedException {
    // TODO Auto-generated method stub
    return zk.getData(path, watch, stat);
  }

  @Override
  public List<String> getChildren(String path, boolean watch)
      throws KeeperException, InterruptedException {
    // TODO Auto-generated method stub
    return zk.getChildren(path, watch);
  }

  @Override
  public void delete(String path, int version) throws InterruptedException,
      KeeperException {
    // TODO Auto-generated method stub
    zk.delete(path, version);
  }

  @Override
  public boolean equaltoStat(String leader, boolean b) throws KeeperException,
      InterruptedException {
    // TODO Auto-generated method stub
    Stat s = null;
    s = exists(leader, b);
    if (s != null) {
      return true;
    }
    return false;
  }

}
