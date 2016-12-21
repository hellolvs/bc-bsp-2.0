package com.chinamobile.bcbsp.graph;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.examples.PREdge;
import com.chinamobile.bcbsp.examples.PRVertex;

public class EdgeManagerTest {

	public EdgeManager em;
	public String vertexData = "0:10.0\t3:0 1:0 2:0 4:0";

	@Before
	public void setUp() {
		MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = 2;
		MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR = "D:/work/";
		MetaDataOfGraph.BCBSP_GRAPH_LOAD_INIT = 2048;
		em = new EdgeManager(PREdge.class);
	}

	@Test
	public void testEdgeManager() {
		em = new EdgeManager(PREdge.class);
		File newfile = new File(MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR + "/"
				+ "Edges" + "/" + "init");
		assertEquals(2, newfile.list().length);
	}

	@Test
	public void testProcessEdgeSaveForAll() throws Exception {
		MetaDataOfGraph.initStatisticsPerBucket();
		PRVertex vertex = new PRVertex();
		vertex.fromString(vertexData);
		em.processEdgeSaveForAll(vertex);
		em.finishEdgesLoad();
		int bucketid = String.valueOf(0).hashCode() % 2;
		BCBSPSpillingInputBuffer bsi = new BCBSPSpillingInputBuffer(
				MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR + "/" + "Edges" + "/"
						+ "init" + "/" + "/Bucket-" + bucketid, 2048);
		// bsi.
		int length = bsi.readInt();
		assertEquals(4, length);
	}

	@Test
	public void testFinishEdgesLoad() throws Exception {
		MetaDataOfGraph.initStatisticsPerBucket();
		PRVertex vertex = new PRVertex();
		vertex.fromString(vertexData);
		em.processEdgeSaveForAll(vertex);
		em.finishEdgesLoad();
		int bucketid = String.valueOf(0).hashCode() % 2;
		BCBSPSpillingInputBuffer bsi = new BCBSPSpillingInputBuffer(
				MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR + "/" + "Edges" + "/"
						+ "init" + "/" + "/Bucket-" + bucketid, 2048);
		// bsi.
		int length = bsi.readInt();
		assertEquals(4, length);
	}

	@Test
	public void testPrepareBucket() throws IOException {
		em.prepareBucket(0, 0);
		BCBSPSpillingInputBuffer bsi = em.getCurrentReader();

		int length = bsi.readInt();
		assertEquals(4, length);
	}

	@Ignore
	public void testProcessEdgeSave() {
		fail("Not yet implemented");
	}

	@Test
	public void testFinishPreparedBucket() throws Exception {
		em.prepareBucket(0, 0);
		em.finishPreparedBucket();
		assertEquals(null, em.getCurrentReader());
	}

}
