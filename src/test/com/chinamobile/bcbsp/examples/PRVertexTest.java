package com.chinamobile.bcbsp.examples;

import static org.junit.Assert.*;



import org.junit.Test;

import com.chinamobile.bcbsp.examples.PREdge;
import com.chinamobile.bcbsp.examples.PRVertex;

public class PRVertexTest {

	@Test
	public void testHashCode() {
		PRVertex prv = new PRVertex();
		prv.setVertexID(1);
		int i = prv.hashCode();
		assertEquals(1,i);
		
	}

	@Test
	public void testAddEdge() {

		PRVertex prv = new PRVertex();
		PREdge pre = new PREdge();
		prv.addEdge(pre);
		assertEquals(1,prv.getEdgesNum());
	}

	@Test
	public void testFromString() throws Exception {
		PRVertex prv = new PRVertex();
		prv.fromString("1:0"+"\t"+"2:3"+" "+"3:1");
		assertEquals(2,prv.getEdgesNum());
	    
	}

	@Test
	public void testIntoString() {
		
		PRVertex prv = new PRVertex();
		PREdge pre = new PREdge();
		PREdge pre2 = new PREdge();
		prv.addEdge(pre);
		prv.addEdge(pre2);
		pre2.setEdgeValue((byte)3);
		pre2.setVertexID(3);
		pre.setVertexID(2);
		pre.setEdgeValue((byte)3);
		prv.setVertexID(1);
		prv.setVertexValue(2f);
		String buffer = prv.intoString();
	    assertEquals("1:2.0"+"\t"+"2:3"+" "+"3:3",buffer);
		
	}

	@Test
	public void testRemoveEdge() {
		PRVertex prv = new PRVertex();
		PREdge pre = new PREdge();
		prv.addEdge(pre);
		prv.removeEdge(pre);
		assertEquals(0,prv.getEdgesNum());
	}

	@Test
	public void testUpdateEdge() {
		PRVertex prv = new PRVertex();
		PREdge pre = new PREdge();
		prv.addEdge(pre);
		prv.updateEdge(pre);
		assertEquals(1,prv.getEdgesNum());
	}

}
