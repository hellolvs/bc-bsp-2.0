package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import com.chinamobile.bcbsp.examples.AggregateValueMsgCount;

import com.chinamobile.bcbsp.examples.SumMsgCountAggregator;

public class SumMsgCountAggregatorTest {

	@Test
	public void testAggregate() {
		SumMsgCountAggregator SumM = new SumMsgCountAggregator();
		ArrayList<AggregateValueMsgCount> tmpValues = new ArrayList<AggregateValueMsgCount>();
		AggregateValueMsgCount aggValue0 = new AggregateValueMsgCount();
		AggregateValueMsgCount aggValue1 = new AggregateValueMsgCount();
		aggValue0.setValue(1L);
		aggValue1.setValue(2L);
		tmpValues.add(aggValue0);
		tmpValues.add(aggValue1);
		AggregateValueMsgCount aggValues = SumM.aggregate(tmpValues);
		assertEquals(3L,aggValues.getValue());
		
	}

}
