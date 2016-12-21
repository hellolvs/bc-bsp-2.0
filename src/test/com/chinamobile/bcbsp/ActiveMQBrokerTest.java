package com.chinamobile.bcbsp;



import org.junit.Test;

import com.chinamobile.bcbsp.ActiveMQBroker;

public class ActiveMQBrokerTest {

	@Test
	public void testStartBroker() throws Exception {
		
		ActiveMQBroker s = new ActiveMQBroker("192.168.0.68");
        s.startBroker(61616);
		
	}

}
