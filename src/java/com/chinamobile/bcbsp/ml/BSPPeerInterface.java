
package com.chinamobile.bcbsp.ml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.chinamobile.bcbsp.bspcontroller.Counters.Counter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

interface BSPPeerInterface {
//  /**
//   * Send a data with a tag to another BSPSlave corresponding to hostname.
//   * Messages sent by this method are not guaranteed to be received in a sent
//   * order.
//   * @param peerName
//   * @param msg
//   * @throws IOException
//   */
//  void send(String peerName, PeerMessageInterface msg) throws IOException;
//  
//  /**
//   * @return A message from the peer's received messages queue (a FIFO).
//   * @throws IOException
//   */
//  PeerMessageInterface getCurrentMessage() throws IOException;
  
  /**
   * @return The number of messages in the peer's received messages queue.
   */
 

  int getNumCurrentMessages();

  
  /**
   * @return the count of current super-step
   */
  int getSuperstepCount();
  
  /**
   * @return the name of this peer in the format "hostname:port".
   */
  String getPeerName();
  
//  /**
//   * @return the name of n-th peer from sorted array by name.
//   */
//  String getPeerName(int index);
  
//  /**
//   * @return the index of this peer from sorted array by name.
//   */
//  int getPeerIndex();
  
  /**
   * @return the names of all the peers executing tasks from the same job
   *         (including this peer).
   */
  String[] getAllPeerNames();
  
//  /**
//   * @return the number of peers
//   */
//  int getNumPeers();
//  
//  /**
//   * Clears all queues entries.
//   */
//  void clear();
  
//  /**
//   * Writes a key/value pair to the output collector.
//   * @param key
//   *        your key object
//   * @param value
//   *        your value object
//   * @throws IOException
//   */
//  void write(KeyInterface key, KeyInterface value) throws IOException;
  
//  /**
//   * Deserializes the next input key value into the given objects.
//   * @param key
//   * @param value
//   * @return false if there are no records to read anymore
//   * @throws IOException
//   */
//  boolean readNext(KeyInterface key, KeyInterface value)
//      throws IOException;
  
  /**
   * Reads the next key value pair and returns it as a pair. It may reuse a
   * {@link KeyValuePair} instance to save garbage collection time.
   * @return null if there are no records left.
   * @throws IOException
   * @throws IllegalAccessException 
   * @throws InstantiationException 
   */
  KeyValuePair<KeyInterface, KeyInterface> readNext() throws IOException, InstantiationException, IllegalAccessException;
  
  /**
   * Closes the input and opens it right away, so that the file pointer is at
   * the beginning again.
   */
  void reopenInput() throws IOException;
  
  /**
   * @return the jobs configuration
   */
  BSPJob getConfiguration();
  
  /**
   * Get the {@link Counter} of the given group with the given name.
   * @param name
   *        counter name
   * @return the <code>Counter</code> of the given group/name.
   */
  Counter getCounter(Enum<?> name);
  
  /**
   * Get the {@link Counter} of the given group with the given name.
   * @param group
   *        counter group
   * @param name
   *        counter name
   * @return the <code>Counter</code> of the given group/name.
   */
  Counter getCounter(String group, String name);
  
  /**
   * Increments the counter identified by the key, which can be of any
   * {@link Enum} type, by the specified amount.
   * @param key
   *        key to identify the counter to be incremented. The key can be be any
   *        <code>Enum</code>.
   * @param amount
   *        A non-negative amount by which the counter is to be incremented.
   */
  void incrementCounter(Enum<?> key, long amount);
  
  /**
   * Increments the counter identified by the group and counter name by the
   * specified amount.
   * @param group
   *        name to identify the group of the counter to be incremented.
   * @param counter
   *        name to identify the counter within the group.
   * @param amount
   *        A non-negative amount by which the counter is to be incremented.
   */
  void incrementCounter(String group, String counter, long amount);
  
  /**
   * @return the size of assigned split
   */
  long getSplitSize();
  
  /**
   * @return the current position of the file read pointer
   * @throws IOException
   */
  long getPos() throws IOException;
  
  /**
   * @return the task id of this task.
   */
  StaffAttemptID getStaffId();
  
  boolean hasMorePair();
  
  /**
   * Reset the index of pair.
   */
  void resetPair();
}
