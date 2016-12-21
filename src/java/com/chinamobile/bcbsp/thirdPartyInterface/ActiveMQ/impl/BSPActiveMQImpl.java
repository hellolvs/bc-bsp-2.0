
package com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.BSPActiveMQ;

/**
 * BSPActiveMQImpl A concrete class that implements interface BSPActiveMQ.
 */
public class BSPActiveMQImpl implements BSPActiveMQ {
  /** Define Log variable output messages */
  private static final Log LOG = LogFactory.getLog(BSPActiveMQImpl.class);
  /** State a BrokerService type of variable broker*/
  private BrokerService broker;
  /** State a List<PolicyEntry> type of variable entries*/
  private List<PolicyEntry> entries;
  /** State a PolicyEntry type of variable policy*/
  private PolicyEntry policy;
  /** State a PolicyMap type of variable policyMap*/
  private PolicyMap policyMap;
  
  @Override 
  public List<PolicyEntry> getEntries() {
		return entries;
	}
  @Override
  public PolicyEntry getPolicy() {
	return policy;
}
  @Override
  public PolicyMap getPolicyMap() {
	return policyMap;
}

  /**
   * constructor
   */
  public BSPActiveMQImpl(int i){
	  switch(i){
	  case 1:
		  broker = new BrokerService();
		  break;
	  case 2:
		  entries = new ArrayList<PolicyEntry>();
		  break;
	  case 3:
		  policy = new PolicyEntry();
		  break;
	  case 4:
		  policyMap = new PolicyMap();
		  break;
	  }
  }
  
  @Override
  public void stopBroker() {
    if (broker != null) {
      try {
        broker.stop();
      } catch (Exception e) {
    
      throw new RuntimeException("Exception has happened and been catched! stopBroker failed!",e);
      }
    }

  }

  @Override
  public void setBrokerName(String hostName) {
    broker.setBrokerName(hostName);

  }

  @Override
  public void setDataDirectory(String string) {
    broker.setDataDirectory(string);

  }

  @Override
  public void setUseJmx(boolean b) {
    broker.setUseJmx(b);

  }

  @Override
  public void setPersistent(boolean b) {
    broker.setPersistent(b);

  }

  @Override
  public void addConnector(String string) {
    try {
      broker.addConnector(string);
    } catch (Exception e) {
  
    	throw new RuntimeException("Exception has happened and been catched! AddConnector failed!",e);
    }

  }

  @Override
  public void start() {
    try {
      broker.start();
    } catch (Exception e) {
     
      throw new RuntimeException("Exception has happened and been catched! Start failed!",e);
    }

  }


  @Override
  public void setMemoryLimit(long memoryLimit) {
    policy.setMemoryLimit(memoryLimit);

  }

  @Override
  public void setQueue(String string) {
    policy.setQueue(string);

  }

  @Override
  public void add(PolicyEntry policy) {
    entries.add(policy);

  }



  @Override
  public void setDefaultEntry(PolicyEntry policy) {
    policyMap.setDefaultEntry(policy);

  }

  @Override
  public void setPolicyEntries(List<PolicyEntry> entries) {
    policyMap.setPolicyEntries(entries);

  }

  @Override
  public void setDestinationPolicy(PolicyMap policyMap) {
    broker.setDestinationPolicy(policyMap);

  }

  @Override
  public void setLimit(long limit) {
    broker.getSystemUsage().getMemoryUsage().setLimit(limit);
  }
}
