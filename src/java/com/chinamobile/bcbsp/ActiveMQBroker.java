/**
 * CopyRight by Chinamobile
 *
 * ActiveMQBroker.java
 */

package com.chinamobile.bcbsp;

import com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.BSPActiveMQ;
import com.chinamobile.bcbsp.thirdPartyInterface.ActiveMQ.impl.BSPActiveMQImpl;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/**
 *
 * ActiveMQBroker Provides message middleware ActiveMQ messaging service
 *
 */
public final class ActiveMQBroker {

  /** State broker name */
  private String brokerName = "localhost";

  /**
   * constructor
   * @param brokerName
   *        broker name
   */
  public ActiveMQBroker(String brokerName) {
    this.brokerName = brokerName;
  }

  /**
   * The main function
   * @param args
   *        String[]
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    String hostName = args[0];


    BSPActiveMQ broker = new BSPActiveMQImpl(1);

    broker.setBrokerName(hostName);
    broker.setDataDirectory("activemq_data/");
    broker.setUseJmx(true);

    broker.setPersistent(false);
    broker.addConnector("tcp://0.0.0.0:61616");

    broker.start();

    System .out .println("Start broker successfully!");

    /** now lets wait forever to avoid the JVM terminating immediately */
    Object lock = new Object();
    synchronized (lock) {
      lock.wait();
    }

    System .out .println("Run over.");
  }

  /**
   * To the BrokerName: port start message forwarding service for the url.
   * @param port
   *        port number
   * @throws Exception
   */
  public void startBroker(int port) throws Exception {


    BSPActiveMQ broker = new BSPActiveMQImpl(1);

    broker.setBrokerName(this.brokerName);
    broker.setUseJmx(true);
    broker.setPersistent(false);

    broker.setLimit(512 * 1024 * 1024);

    BSPActiveMQ entries = new BSPActiveMQImpl(2);
    //entries.listPolicyEntryMethod();

    BSPActiveMQ policy = new BSPActiveMQImpl(3);
   // policy.policyEntryMethod();

    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
    long max = memoryUsage.getMax();
    long memoryLimit = max / 2;
    policy.setMemoryLimit(memoryLimit);
    policy.setQueue("BSP");
    entries.add(policy.getPolicy());

    BSPActiveMQ policyMap = new BSPActiveMQImpl(4);
    //policyMap.policyMapMethod();
    
    policyMap.setDefaultEntry(policy.getPolicy());
    policyMap.setPolicyEntries(entries.getEntries());
    broker.setDestinationPolicy(policyMap.getPolicyMap());

    String connectortUri = "tcp://0.0.0.0:" + port;

    broker.addConnector(connectortUri);

    broker.start();
  }

  /**
   * Stop message service.
   * @throws Exception
   */
  public void stopBroker() throws Exception {
    stopBroker();

  }
}
