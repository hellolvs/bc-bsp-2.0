
package com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.impl;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.BSPActiveMQConnFactory;

/**
 * BSPActiveMQConnFactoryImpl A concrete class that implements interface
 * BSPActiveMQConnFactory.
 */
public class BSPActiveMQConnFactoryImpl implements BSPActiveMQConnFactory {
  /** Define Log variable output messages */
  private static final Log LOG = LogFactory
      .getLog(BSPActiveMQConnFactoryImpl.class);
  /** State ActiveMQConnection.DEFAULT_USER */
  private String user = ActiveMQConnection.DEFAULT_USER;
  /** State ActiveMQConnection.DEFAULT_PASSWORD */
  private String password = ActiveMQConnection.DEFAULT_PASSWORD;
  /** State a ActiveMQConnectionFactory type of variable connectionFactory */
  private ActiveMQConnectionFactory connectionFactory;

  @Override
  public void activeMQConnFactoryMethod(String url) {
    connectionFactory = new ActiveMQConnectionFactory(user, password, url);
  }

  @Override
  public void setCopyMessageOnSend(boolean b) {
    connectionFactory.setCopyMessageOnSend(false);
  }

  @Override
  public Connection createConnection() {
    try {
      return connectionFactory.createConnection();
    } catch (JMSException e) {
      // TODO Auto-generated catch block
      LOG.info(
          "Exception has happened and been catched! createConnection failed!",
          e);
      return null;
    }

  }

  @Override
  public void setOptimizeAcknowledge(boolean b) {
    connectionFactory.setOptimizeAcknowledge(true);
  }
}
