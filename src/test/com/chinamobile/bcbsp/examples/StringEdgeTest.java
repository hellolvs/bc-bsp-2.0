package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;

import org.junit.Test;


import com.chinamobile.bcbsp.examples.StringEdge;

public class StringEdgeTest {

	@Test
	public void testHashCode() {
		StringEdge se = new StringEdge();
		se.setVertexID("123");
		int i =se.hashCode();
		assertEquals(123,i);
		
		
	}

	@Test
	public void testFromString() throws Exception {
		StringEdge se = new StringEdge();
		se.fromString("2:1");
		assertEquals("2",se.getVertexID());
		assertEquals("1",se.getEdgeValue());
	}

	@Test
	public void testIntoString() {
		StringEdge se = new StringEdge();
		se.setVertexID("1");
		se.setEdgeValue("1");
		String s = se.intoString();
		assertEquals("1:1",s);
	}

	@Test
	public void testEqualsObject() {
		StringEdge se = new StringEdge();
		StringEdge se2 = new StringEdge();
		se.setVertexID("1");
		se2.setVertexID("1");
		boolean b = se.equals(se2);
		assertEquals(true, b);

	}

}
