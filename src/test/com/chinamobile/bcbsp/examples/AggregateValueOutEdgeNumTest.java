package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;

import org.junit.Test;

import com.chinamobile.bcbsp.bspstaff.AggregationContext;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.examples.AggregateValueOutEdgeNum;
import com.chinamobile.bcbsp.examples.PRVertex;

import com.chinamobile.bcbsp.util.BSPJob;

public class AggregateValueOutEdgeNumTest {
	private ArrayList<BSPMessage> alist;

	@Test
	public void testInitValueString() {

		AggregateValueOutEdgeNum AggVedge = new AggregateValueOutEdgeNum();
		AggVedge.initValue("100");
		assertEquals(100, AggVedge.getValue());
	}

	@Test
	public void testInitValueIteratorOfBSPMessageAggregationContextInterface() {
		BSPMessage m1 = new BSPMessage();
		BSPMessage m2 = new BSPMessage();
		BSPMessage m3 = new BSPMessage();
		alist = new ArrayList<BSPMessage>();
		alist.add(m1);
		alist.add(m2);
		alist.add(m3);
		BSPJob job = mock(BSPJob.class);
		PRVertex vertex = new PRVertex();
		AggregationContext aggContext = new AggregationContext(job, vertex, 10);
		AggregateValueOutEdgeNum AggVedge = new AggregateValueOutEdgeNum();
        AggVedge.initValue(alist.iterator(), aggContext);
		assertEquals(0, AggVedge.getValue());
	}

}
