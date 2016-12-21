package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import com.chinamobile.bcbsp.examples.AggregateValueOutEdgeNum;
import com.chinamobile.bcbsp.examples.MaxAggregator;

public class MaxAggregatorTest {

	@Test
	public void testAggregate() throws InstantiationException,
			IllegalAccessException {

		MaxAggregator maxAgg = new MaxAggregator();
		ArrayList<AggregateValueOutEdgeNum> tmpValues = new ArrayList<AggregateValueOutEdgeNum>();
		AggregateValueOutEdgeNum aggValue0 = new AggregateValueOutEdgeNum();
		AggregateValueOutEdgeNum aggValue1 = new AggregateValueOutEdgeNum();
		aggValue0.setValue(1L);
		aggValue1.setValue(2L);
		tmpValues.add(aggValue0);
		tmpValues.add(aggValue1);
		AggregateValueOutEdgeNum aggValue = maxAgg.aggregate(tmpValues);

		assertEquals(2L, aggValue.getValue());
	}

}
