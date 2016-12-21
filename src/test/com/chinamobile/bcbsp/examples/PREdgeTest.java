package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;

import org.junit.Test;

import com.chinamobile.bcbsp.examples.PREdge;

public class PREdgeTest {

	@Test
	public void testHashCode() {

		PREdge pre = new PREdge();
		pre.setVertexID(2);
		pre.getVertexID();
		int i = pre.hashCode();
		assertEquals(2, i);

	}

	@Test
	public void testFromString() throws Exception {
		PREdge pre = new PREdge();
		pre.fromString("1:0");
		assertEquals(0, pre.getEdgeValue());
		assertEquals(1, pre.getVertexID());
	}

	@Test
	public void testIntoString() {
		PREdge pre = new PREdge();
		pre.setVertexID(1);
		pre.setEdgeValue((byte) 21);
		String s = pre.intoString();
		assertEquals("1:21", s);
	}

	@Test
	public void testEqualsObject() {
		PREdge pre = new PREdge();
		PREdge pre2 = new PREdge();
		pre.setVertexID(1);
		pre2.setVertexID(1);
		boolean b = pre.equals(pre2);
		assertEquals(true, b);

	}

}
