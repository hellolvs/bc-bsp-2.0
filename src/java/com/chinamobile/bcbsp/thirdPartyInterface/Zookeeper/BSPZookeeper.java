
package com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 *
 * BSPZookeeper An interface that encapsulates Zookeeper.
 *
 */
public interface BSPZookeeper {

  /**
   * Close this client object. Once the client is closed, its session becomes
   * invalid. All the ephemeral nodes in the ZooKeeper server associated with
   * the session will be removed. The watches left on those nodes (and on
   * their parents) will be triggered.
   * @throws InterruptedException
   */
  void close() throws InterruptedException;

  /**
   * getter method
   * @return
   *        zk
   */
  ZooKeeper getZk();

  /**
   * Return the stat of the node of the given path. Return null if no such a
   * node exists.
   * @param path
   *        the node path
   * @param watch
   *        whether need to watch this node
   * @return
   *        the stat of the node of the given path; return null if no such a
   *         node exists.
   * @throws KeeperException
   * @throws InterruptedException
   */
  Stat exists(String path, boolean watch) throws KeeperException,
      InterruptedException;

  /**
   * Create a node with the given path.
   * @param path
   *        the path for the node
   * @param data
   *        the initial data for the node
   * @param acl
   *         the acl for the node
   * @param createMode
   *        specifying whether the node to be created is ephemeral
   *                and/or sequential
   * @return
   *         the actual path of the created node
   * @throws KeeperException
   * @throws InterruptedException
   */
  String create(final String path, byte[] data, List<ACL> acl,
      CreateMode createMode) throws KeeperException, InterruptedException;

  /**
   * Set the data for the node of the given path if such a node exists and the
   * given version matches the version of the node (if the given version is
   * -1, it matches any node's versions). Return the stat of the node.
   * @param path
   *        the path of the node
   * @param data
   *        the data to set
   * @param version
   *         the expected matching version
   * @return
   *         the state of the node
   * @throws KeeperException
   * @throws InterruptedException
   */
  Stat setData(final String path, byte[] data, int version)
      throws KeeperException, InterruptedException;

  /**
   * Determine stat whether is null.
   * @param leader
   *       the node path
   * @param b
   *         whether need to watch this node
   * @return
   *         if equal return true else false.
   * @throws KeeperException
   * @throws InterruptedException
   */
  boolean equaltostat(String leader, boolean b)
      throws KeeperException, InterruptedException;


  /**
   * Determine state whether is equal.
   * @return
   *       if equal return true else false.
   */
  boolean equaltoState();

  /**
   * Return the data and the stat of the node of the given path.
   * @param path
   *        the given path
   * @param watch
   *        whether need to watch this node
   * @param stat
   *         the stat of the node
   * @return
   *        the data of the node
   * @throws KeeperException
   * @throws InterruptedException
   */
  byte[] getData(String path, boolean watch, Stat stat)
      throws KeeperException, InterruptedException;

  /**
   * Return the list of the children of the node of the given path.
   * @param path
   *        the given path
   * @param watch
   *        whether need to watch this node
   * @return
   *        an unordered array of children of the node with the given path
   * @throws KeeperException
   * @throws InterruptedException
   */
  List<String> getChildren(String path, boolean watch)
      throws KeeperException, InterruptedException;

  /**
   * Delete the node with the given path. The call will succeed if such a node
   * exists, and the given version matches the node's version (if the given
   * version is -1, it matches any node's versions).
   * @param path
   *        the path of the node to be deleted.
   * @param version
   *        the expected node version.
   * @throws InterruptedException
   * @throws KeeperException
   */
  void delete(final String path, int version)
      throws InterruptedException, KeeperException;

  /**
   * Determine stat whether is not null.
   * @param leader
   *        the node path
   * @param b
   *        whether need to watch this node
   * @return
   *        if not equal return true else false.
   * @throws KeeperException
   * @throws InterruptedException
   */
  boolean equaltoStat(String leader, boolean b)
      throws KeeperException, InterruptedException;
}
