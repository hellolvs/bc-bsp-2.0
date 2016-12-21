package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import com.chinamobile.bcbsp.bspstaff.AggregationContext;

import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.examples.AggregateValueMsgCount;
import com.chinamobile.bcbsp.examples.PRVertex;


import com.chinamobile.bcbsp.util.BSPJob;

import static org.mockito.Mockito.mock;

public class AggregateValueMsgCountTest {
	private ArrayList<BSPMessage> alist;

	@Test
	public void testInitValue() {
		BSPMessage m1 = new BSPMessage();
		BSPMessage m2 = new BSPMessage();
		alist = new ArrayList<BSPMessage>();
		alist.add(m1);
		alist.add(m2);
		BSPJob job = mock(BSPJob.class);
		PRVertex vertex = new PRVertex();
		AggregationContext aggContext = new AggregationContext(job, vertex, 5);
		AggregateValueMsgCount AggV = new AggregateValueMsgCount();
		AggV.initValue(alist.iterator(), aggContext);
		assertEquals(2, AggV.getValue());
	}

	@Test
	public void testInitValueString(){
		AggregateValueMsgCount AggV = new AggregateValueMsgCount();
		AggV.initValue("25");
		assertEquals(25,AggV.getValue());
	}
	
	@Test
	public void testToString(){
		AggregateValueMsgCount AggV = new AggregateValueMsgCount();
		AggV.initValue("25");
		assertEquals("25",AggV.getValue().toString());
	}
}
