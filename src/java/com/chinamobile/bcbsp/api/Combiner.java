/**
 * CopyRight by Chinamobile
 *
 * Combiner.java
 */

package com.chinamobile.bcbsp.api;

import java.util.Iterator;

/**
 * Combiner Abstract class to be extended by user to implement combine operation
 * to the messages sent to the same vertex.
 * @param <M>
 */
public abstract class Combiner<M> {

  /**
   * Combine the messages to generate a new message, and return it.
   *
   * @param messages
   *        Iterator<BSPMessage>
   * @return message BSPMessage
   */
  public abstract M combine(Iterator<M> messages);

}
