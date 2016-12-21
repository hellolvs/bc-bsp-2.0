package com.chinamobile.bcbsp.bspstaff;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.examples.AggregateValueVertexNum;

public class SuperStepContextTest {
	
private SuperStepContext context;
    
    private int currentSuperStepCounter = 5;
    private AggregateValueVertexNum aggValue;

	@Before
	public void setUp() throws Exception {
		context = new SuperStepContext(null, currentSuperStepCounter);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSuperStepContext() {
		context = new SuperStepContext(null, currentSuperStepCounter);
	}

	@Test
	public void testAddAggregateValues() {
		aggValue = new AggregateValueVertexNum();
        aggValue.setValue(1L);
        context.addAggregateValues("SUM", aggValue);
        assertEquals(context.getAggregateValue("SUM").getValue(), aggValue.getValue());
	}

	@Test
	public void testGetAggregateValue() {
		aggValue = new AggregateValueVertexNum();
        aggValue.setValue(1L);
        context.addAggregateValues("SUM", aggValue);
        assertEquals(context.getAggregateValue("SUM").getValue(), aggValue.getValue());
	}

	@Test
	public void testGetCurrentSuperStepCounter() {
		assertEquals(context.getCurrentSuperStepCounter(), currentSuperStepCounter);
	}
	@Ignore(value="the get function")
	@Test
	public void testGetJobConf() {
		fail("Not yet implemented");
	}

}
