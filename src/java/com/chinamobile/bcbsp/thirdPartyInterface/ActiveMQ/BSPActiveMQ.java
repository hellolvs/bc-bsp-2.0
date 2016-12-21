
package com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ;

import java.util.List;

import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;

/**
 * BSPActiveMQ An interface that encapsulates ActiveMQ.
 */
public interface BSPActiveMQ {

  /**
   * A method that encapsulates stopBroker().
   */
  void stopBroker();

  /**
   * A method that encapsulates setBrokerName(String hostName).
   *
   * @param hostName
   *        The name of host
   */
  void setBrokerName(String hostName);

  /**
   * A method that encapsulates setDataDirectory(String string).
   *
   * @param string
   *        The value of a String type
   */
  void setDataDirectory(String string);

  /**
   * A method that encapsulates setUseJmx(boolean b).
   *
   * @param b
   *        The value of a Boolean type
   */
  void setUseJmx(boolean b);

  /**
   * A method that encapsulates setPersistent(boolean b).
   *
   * @param b
   *        The value of a Boolean type
   */
  void setPersistent(boolean b);

  /**
   * A method that encapsulates addConnector(String string).
   *
   * @param string
   *        The value of a String type
   */
  void addConnector(String string);

  /**
   * A method that encapsulates start().
   */
  void start();


  /**
   * A method that encapsulates setMemoryLimit(long memoryLimit).
   *
   * @param memoryLimit
   *        The memory limit
   */
  void setMemoryLimit(long memoryLimit);

  /**
   * A method that encapsulates setQueue(String string).
   *
   * @param string
   *        The value of a String type
   */
  void setQueue(String string);

  /**
   * A method that encapsulates add(BSPActiveMQ policy).
   *
   * @param policy
   *        The value of a BSPActiveMQ type
   */
  void add(PolicyEntry policy);


  /**
   * A method that encapsulates setDefaultEntry(BSPActiveMQ policy).
   *
   * @param policy
   *        The value of a BSPActiveMQ type
   */
  
  void setDefaultEntry(PolicyEntry policy);

  /**
   * A method that encapsulates setPolicyEntries(BSPActiveMQ entries).
   *
   * @param entries
   *        The value of a BSPActiveMQ type
   */
  
  void setPolicyEntries(List<PolicyEntry> entries);

  /**
   * A method that encapsulates setDestinationPolicy(BSPActiveMQ policyMap).
   *
   * @param policyMap
   *        The value of a BSPActiveMQ type
   */
  void setDestinationPolicy(PolicyMap policyMap);

  /**
   * A method that encapsulates setLimit(long Limit).
   *
   * @param limit
   *        The value of a long type
   */
  void setLimit(long limit);
 
  /**
   * A method that get Policy.
   * @return
   *        policy
   */
  PolicyEntry getPolicy();
  
  /**
   * A method that get entry.
   * @return
   *        entry
   */
  List<PolicyEntry> getEntries();
  
  /**
   * A method that get PolicyMap.
   * @return
   *        PolicyMap
   */
  PolicyMap getPolicyMap();
}
