package com.chinamobile.bcbsp.graph;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.examples.PRVertex;

public class VertexManagerTest {

	public VertexManager vm;

	// public MetaDataOfGraph MDG;
	@Before
	public void setUp() {
		MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = 2;
		MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR = "D:/work/";
		MetaDataOfGraph.BCBSP_GRAPH_LOAD_INIT = 2048;
		vm = new VertexManager();
		// MDG=new MetaDataOfGraph();

	}

	@Test
	public void testVertexManager() {
		vm = new VertexManager();
		File newfile = new File(MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR + "/"
				+ "Vertices" + "/" + "init");
		assertEquals(2, newfile.list().length);
	}

	@Test
	public void testInitialize() {

		// System.out.println(MetaDataOfGraph.BCBSP_GRAPH_LOAD_INIT);
		vm.initialize();
		File newfile = new File(MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR + "/"
				+ "Vertices" + "/" + "init");
		assertEquals(2, newfile.list().length);
	}

	@SuppressWarnings({ "unchecked", "rawtypes", "resource" })
	@Test
	public void testProcessVertexSaveForAll() throws IOException {
		MetaDataOfGraph.initStatisticsPerBucket();
		Vertex v = new PRVertex();
		v.setVertexID(123);
		vm.processVertexSaveForAll(v);
		vm.finishVertexLoad();
		int bucketid = String.valueOf(123).hashCode() % 2;
		BCBSPSpillingInputBuffer bsi = new BCBSPSpillingInputBuffer(
				MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR + "/" + "Vertices"
						+ "/" + "init" + "/" + "/Bucket-" + bucketid, 2048);
		// bsi.
		int verteid = bsi.readInt();
		assertEquals(123, verteid);

	}

	@Test
	public void testFinishVertexLoad() {
		vm.finishVertexLoad();
		assertEquals(null, vm.getSpillingBuffers()[0]);
		assertEquals(null, vm.getSpillingBuffers()[0]);
	}

	@Test
	public void testFinishPreparedBucket() {
		vm.prepareBucket(0, 0);

		vm.finishPreparedBucket();
		assertEquals(null, vm.getCurrentReader());
		assertEquals(null, vm.getCurrentWriter());
	}

}
