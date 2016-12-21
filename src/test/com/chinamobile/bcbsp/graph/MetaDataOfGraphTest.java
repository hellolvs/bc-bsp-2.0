package com.chinamobile.bcbsp.graph;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class MetaDataOfGraphTest {

	public MetaDataOfGraph MDG;

	@SuppressWarnings("static-access")
	@Before
	public void setUp() {
		MDG = new MetaDataOfGraph();
		MDG.BCBSP_DISKGRAPH_HASHNUMBER = 5;
	}

	@SuppressWarnings("static-access")
	@Test
	public void testInitStatisticsPerBucket() {
		MDG.initStatisticsPerBucket();
		assertEquals(5, MDG.VERTEX_NUM_PERBUCKET.length);
		assertEquals(5, MDG.EDGE_NUM_PERBUCKET.length);
		for (int i = 0; i < 5; i++) {
			assertEquals(0, MDG.VERTEX_NUM_PERBUCKET[i]);
			assertEquals(0, MDG.EDGE_NUM_PERBUCKET[i]);
		}

	}

	@SuppressWarnings("static-access")
	@Test
	public void testStatisticsPerBucket() {
		MDG.initStatisticsPerBucket();
		assertEquals(5, MDG.VERTEX_NUM_PERBUCKET.length);
		assertEquals(5, MDG.EDGE_NUM_PERBUCKET.length);
		for (int i = 0; i < 5; i++) {
			assertEquals(0, MDG.VERTEX_NUM_PERBUCKET[i]);
			assertEquals(0, MDG.EDGE_NUM_PERBUCKET[i]);
		}

	}

	@SuppressWarnings("static-access")
	@Test
	public void testLogStatisticsPerBucket() {
		MDG.initStatisticsPerBucket();
		assertEquals(5, MDG.VERTEX_NUM_PERBUCKET.length);
		assertEquals(5, MDG.EDGE_NUM_PERBUCKET.length);
		for (int i = 0; i < 5; i++) {
			assertEquals(0, MDG.VERTEX_NUM_PERBUCKET[i]);
			assertEquals(0, MDG.EDGE_NUM_PERBUCKET[i]);
		}

	}

	@Test
	public void testLocalPartitionRule() {
		int hashBucketNumber = 5;
		String dstVertexID = "abc";
		int hashCode = dstVertexID.hashCode();
		int hashIndex = hashCode % 5; // bucket index
		hashIndex = (hashIndex < 0 ? hashIndex + hashBucketNumber : hashIndex);
		assertEquals(hashIndex, MDG.localPartitionRule(dstVertexID));
	}

}
