package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;

import org.junit.Test;

import com.chinamobile.bcbsp.examples.SSPEdge;
import com.chinamobile.bcbsp.examples.SSPVertex;

public class SSPVertexTest {

	@Test
	public void testHashCode() {
		SSPVertex sspv = new SSPVertex();
		sspv.setVertexID(1);
		int i = sspv.hashCode();
		assertEquals(1,i);
	}

	@Test
	public void testAddEdge() {
		SSPVertex sspv = new SSPVertex();
		SSPEdge sspe = new SSPEdge();
		sspv.addEdge(sspe);
		assertEquals(1,sspv.getEdgesNum());
	}

	@Test
	public void testFromString() throws Exception {
		SSPVertex sspv = new SSPVertex();
		sspv.fromString("2:0"+"\t"+"4:3"+" "+"5:1"+" "+"6:3");
		assertEquals(3,sspv.getEdgesNum());
	}

	@Test
	public void testIntoString() {
		SSPVertex sspv = new SSPVertex();
		SSPEdge sspe = new SSPEdge();
		SSPEdge sspe2 = new SSPEdge();
		sspv.addEdge(sspe);
		sspv.addEdge(sspe2);
		sspe2.setEdgeValue(3);
		sspe2.setVertexID(3);
		sspe.setVertexID(2);
		sspe.setEdgeValue(3);
		sspv.setVertexID(1);
		sspv.setVertexValue(2);
		String buffer = sspv.intoString();
	    assertEquals("1:2"+"\t"+"2:3"+" "+"3:3",buffer);
	}

	@Test
	public void testRemoveEdge() {
		SSPVertex sspv = new SSPVertex();
		SSPEdge sspe = new SSPEdge();
		sspv.addEdge(sspe);
		sspv.removeEdge(sspe);
		assertEquals(0,sspv.getEdgesNum());
	}

	@Test
	public void testUpdateEdge() {
		SSPVertex sspv = new SSPVertex();
		SSPEdge sspe = new SSPEdge();
		sspv.addEdge(sspe);
		sspv.updateEdge(sspe);
		assertEquals(1,sspv.getEdgesNum());
	}

}
