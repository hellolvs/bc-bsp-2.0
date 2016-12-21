
package com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ;

import javax.jms.Connection;

/**
 *
 * BSPActiveMQConnFactory An interface that encapsulates ActiveMQConnection.
 *
 */
public interface BSPActiveMQConnFactory {

  /**
   * A method that encapsulates activeMQConnFactoryMethod(String url).
   *
   * @param url
   *        Make the destination broker's url
   */
  void activeMQConnFactoryMethod(String url);

  /**
   * A method that encapsulates setCopyMessageOnSend(boolean b).
   *
   * @param b
   *        The value of a Boolean type
   */
  void setCopyMessageOnSend(boolean b);

  /**
   * A method that encapsulates createConnection().
   *
   * @return
   *        Create the connection
   */
  Connection createConnection();

  /**
   * A method that encapsulates setOptimizeAcknowledge(boolean b).
   *
   * @param b
   *        The value of a Boolean type
   */
  void setOptimizeAcknowledge(boolean b);
}
