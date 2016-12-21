package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;


import com.chinamobile.bcbsp.examples.AggregateValueVertexNum;
import com.chinamobile.bcbsp.examples.SumVertexNumAggregator;

public class SumVertexNumAggregatorTest {

	@Test
	public void testAggregate() {
		SumVertexNumAggregator SumV = new SumVertexNumAggregator();
		ArrayList<AggregateValueVertexNum> tmpValues = new ArrayList<AggregateValueVertexNum>();
		AggregateValueVertexNum aggValue0 = new AggregateValueVertexNum();
		AggregateValueVertexNum aggValue1 = new AggregateValueVertexNum();
		aggValue0.setValue(1L);
		aggValue1.setValue(2L);
		tmpValues.add(aggValue0);
		tmpValues.add(aggValue1);
		AggregateValueVertexNum aggValues = SumV.aggregate(tmpValues);
		assertEquals(3L,aggValues.getValue());
		
	}

}
