package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;

import org.junit.Test;


import com.chinamobile.bcbsp.examples.StringEdge;
import com.chinamobile.bcbsp.examples.StringVertex;

public class StringVertexTest {

	@Test
	public void testHashCode() {
		StringVertex sv = new StringVertex();
		sv.setVertexID("1");
		int i = sv.hashCode();
		assertEquals(49,i);
		
	}

	@Test
	public void testAddEdge() {
		StringVertex sv = new StringVertex();
		StringEdge se = new StringEdge();
		sv.addEdge(se);
		assertEquals(1,sv.getEdgesNum());
	}

	@Test
	public void testFromString() throws Exception {
		StringVertex sv = new StringVertex();
		sv.fromString("2:0"+"\t"+"4:3"+" "+"5:1"+" "+"6:3");
		assertEquals(3,sv.getEdgesNum());
	}

	@Test
	public void testIntoString() {
		StringVertex sv = new StringVertex();
		StringEdge se = new StringEdge();
		StringEdge se2 = new StringEdge();
		sv.addEdge(se);
		sv.addEdge(se2);
		se2.setEdgeValue("3");
		se2.setVertexID("3");
		se.setVertexID("2");
		se.setEdgeValue("3");
		sv.setVertexID("1");
		sv.setVertexValue("2");
		String buffer = sv.intoString();
	    assertEquals("1:2"+"\t"+"2:3"+" "+"3:3",buffer);
	}

	@Test
	public void testRemoveEdge() {
		StringVertex sv = new StringVertex();
		StringEdge se = new StringEdge();
		se.setVertexID("1");
		sv.addEdge(se);
		sv.removeEdge(se);
		assertEquals(0,sv.getEdgesNum());
	}

	@Test
	public void testUpdateEdge() {
		StringVertex sv = new StringVertex();
		StringEdge se = new StringEdge();
		se.setVertexID("1");
		sv.addEdge(se);
		sv.updateEdge(se);
		assertEquals(1,sv.getEdgesNum());
	}

}
