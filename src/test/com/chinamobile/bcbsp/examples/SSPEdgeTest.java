package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;

import org.junit.Test;

import com.chinamobile.bcbsp.examples.SSPEdge;

public class SSPEdgeTest {

	@Test
	public void testHashCode() {
		SSPEdge sspe = new SSPEdge();
		sspe.setVertexID(1);
		int i = sspe.hashCode();
		assertEquals(1,i);
		
	}

	@Test
	public void testFromString() throws Exception {
		SSPEdge sspe = new SSPEdge();
		sspe.fromString("2:1");
		assertEquals(2,sspe.getVertexID());
		assertEquals(1,sspe.getEdgeValue());
		
	}

	@Test
	public void testIntoString() {
		SSPEdge sspe = new SSPEdge();
		sspe.setVertexID(2);
		sspe.setEdgeValue(1);
		String s = sspe.intoString();
		assertEquals("2:1",s);
	}

	@Test
	public void testEqualsObject() {
		SSPEdge sspe = new SSPEdge();
		SSPEdge sspe2 = new SSPEdge();
		sspe.setVertexID(1);
		sspe2.setVertexID(2);
		boolean b = sspe.equals(sspe2);
		assertEquals(false,b);
	}

}
