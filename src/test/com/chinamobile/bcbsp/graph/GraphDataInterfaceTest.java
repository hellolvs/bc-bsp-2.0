package com.chinamobile.bcbsp.graph;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.examples.PREdge;
import com.chinamobile.bcbsp.examples.PRVertex;
import com.chinamobile.bcbsp.graph.GraphDataForDisk.BucketMeta;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class GraphDataInterfaceTest {

	private GraphDataInterface graphdata;
	private static String[] datas = {
			"0:10.0\t3:0 1:0 2:0 4:0 0:0 0:0 0:0 0:0 0:0 1:0 0:0 1:0 3:0",
			"1:10.0\t4:0 2:0 0:0 2:0 4:0 1:0 2:0 1:0 1:0 3:0 3:0 3:0 4:0 0:0",
			"2:10.0\t2:0 1:0 4:0 2:0 3:0 3:0 0:0 3:0 0:0 1:0 3:0 4:0 1:0",
			"3:10.0\t4:0 2:0 3:0 0:0 0:0 1:0 2:0 0:0 1:0 2:0 0:0 4:0 0:0 1:0",
			"4:10.0\t4:0 0:0 1:0 1:0 4:0 2:0 1:0 1:0 0:0 4:0 1:0 2:0 3:0 0:0 2:0 4:0" };
	private static BSPJob job;
	private static int partitionId;
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Class vertexClass = PRVertex.class;
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Class edgeClass = PREdge.class;

	public GraphDataInterfaceTest(GraphDataInterface graphdata) {

		this.graphdata = graphdata;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		job = mock(BSPJob.class);
		when(job.getMemoryDataPercent()).thenReturn(0.8f);
		when(job.getBeta()).thenReturn(0.5f);
		when(job.getHashBucketNumber()).thenReturn(32);
		when(job.getJobID()).thenReturn(new BSPJobID("jtIdentifier", 2));
		when(job.getVertexClass()).thenReturn(vertexClass);
		when(job.getEdgeClass()).thenReturn(edgeClass);
		job.getBeta();
		partitionId = 1;
		GraphDataForDisk graphDataForDisk = new GraphDataForDisk();
		graphDataForDisk.initialize(job, partitionId);
		GraphDataForBDB graphDataForBDB = new GraphDataForBDB();
		graphDataForBDB.initialize(job, partitionId);

		// ce shi le disk mem he bdb
		return Arrays.asList(new Object[][] { { graphDataForDisk },
				{ new GraphDataForMem() }, { graphDataForBDB } });
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Before
	public void setUp() throws Exception {

		for (int i = 0; i < 4; i++) {
			Vertex vertex = new PRVertex();
			vertex.fromString(datas[i]);
			graphdata.addForAll(vertex);
		}

	}

	@SuppressWarnings("rawtypes")
	@After
	public void tearDown() {
		graphdata.clean();
		System.out.println("size:" + graphdata.sizeForAll());
		if (graphdata instanceof GraphDataForDisk) {
			ArrayList<ArrayList<Vertex>> hashBuckets = ((GraphDataForDisk) graphdata)
					.getHashBuckets();
			ArrayList<BucketMeta> metaTable = ((GraphDataForDisk) graphdata)
					.getMetaTable();
			for (int i = 0; i < hashBuckets.size(); i++) {
				if (hashBuckets.get(i) != null) {
					hashBuckets.get(i).clear();
				}
				BucketMeta meta = metaTable.get(i);
				meta.superStepCount = -1;
				meta.onDiskFlag = false;
				meta.length = 0;
				meta.lengthInMemory = 0;
				meta.count = 0;
				meta.activeCount = 0;
			}

			((GraphDataForDisk) graphdata).setTotalCountOfVertex(0);
			((GraphDataForDisk) graphdata).setTotalCountOfEdge(0);
			((GraphDataForDisk) graphdata).setSizeForAll(0);
		}
		if (graphdata instanceof GraphDataForBDB) {
			List<Boolean> activeFlags = ((GraphDataForBDB) graphdata)
					.getActiveFlags();
			((GraphDataForBDB) graphdata).setEdgeSize(0);
			((GraphDataForBDB) graphdata).setTotalHeadNodes(0);
			for (int i = 0; i < activeFlags.size(); i++) {
				activeFlags.set(i, false);
			}
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testAddForAll() throws Exception {
		int sizeBeforAdd = graphdata.size();
		Vertex vertex = new PRVertex();
		vertex.fromString(datas[4]);
		graphdata.addForAll(vertex);
		assertEquals(
				"After add 1 vertex, there are size+1 vertice in graphdata.",
				sizeBeforAdd + 1, graphdata.size());
	}

	@Test
	public void testSize() {
		graphdata.size();

		if (graphdata instanceof GraphDataForDisk) {
			assertEquals("Check getForAll.", new Integer(1),
					(Integer) graphdata.get(0).getVertexID());
		}
		if (graphdata instanceof GraphDataForMem) {
			graphdata.get(1);
			assertEquals("Check getForAll.", new Integer(0),
					(Integer) graphdata.get(0).getVertexID());
		}
	}

	@Ignore
	public void testGet() {
		fail("Not yet implemented");
	}

	@Test
	public void testSet() {
		PRVertex vertex = (PRVertex) graphdata.get(0);
		float oldValue = vertex.getVertexValue();
		vertex.setVertexValue(oldValue + 1);
		graphdata.set(0, vertex, true);
		assertEquals("After set, the value of 0th vertex has changed.",
				oldValue + 1, graphdata.get(0).getVertexValue());
	}

	@Test
	public void testSizeForAll() {
		if (!(graphdata instanceof GraphDataForBDB)) {
			assertEquals("After setUp, there are 4 vertice in graphdata.", 4,
					graphdata.sizeForAll());
		}
	}

	@Test
	public void testGetForAll() {
		graphdata.size();
		PRVertex vertex = (PRVertex) graphdata.getForAll(0);

		float oldValue = vertex.getVertexValue();
		vertex.setVertexValue(oldValue + 1);

		graphdata.set(0, vertex, false);

		if (graphdata instanceof GraphDataForDisk) {
			assertEquals("Check getForAll.", new Integer(3),
					(Integer) graphdata.getForAll(1).getVertexID());
		}
		if (graphdata instanceof GraphDataForMem) {
			graphdata.get(1);
			assertEquals("Check getForAll.", new Integer(1),
					(Integer) graphdata.getForAll(1).getVertexID());
		}

	}

	@Test
	public void testGetActiveFlagForAll() {
		if (!(graphdata instanceof GraphDataForBDB)) {
			assertEquals("Check getActive.", true,
					graphdata.getActiveFlagForAll(0));
			PRVertex vertex = (PRVertex) graphdata.get(0);
			graphdata.set(0, vertex, false);
			assertEquals("Check getActive.", false,
					graphdata.getActiveFlagForAll(0));
		}
	}

	@Ignore
	public void testFinishAdd() {
		fail("Not yet implemented");
	}

	@Test
	public void testClean() {
		graphdata.clean();

		if (graphdata instanceof GraphDataForDisk) {
			ArrayList<ArrayList<Vertex>> hashBuckets = ((GraphDataForDisk) graphdata)
					.getHashBuckets();
			ArrayList<BucketMeta> metaTable = ((GraphDataForDisk) graphdata)
					.getMetaTable();
			for (int i = 0; i < hashBuckets.size(); i++) {
				if (hashBuckets.get(i) != null) {
					hashBuckets.get(i).clear();
				}
				BucketMeta meta = metaTable.get(i);
				meta.superStepCount = -1;
				meta.onDiskFlag = false;
				meta.length = 0;
				meta.lengthInMemory = 0;
				meta.count = 0;
				meta.activeCount = 0;
			}

			((GraphDataForDisk) graphdata).setTotalCountOfVertex(0);
			((GraphDataForDisk) graphdata).setTotalCountOfEdge(0);
			((GraphDataForDisk) graphdata).setSizeForAll(0);
		}
		if (graphdata instanceof GraphDataForBDB) {
			List<Boolean> activeFlags = ((GraphDataForBDB) graphdata)
					.getActiveFlags();
			((GraphDataForBDB) graphdata).setEdgeSize(0);
			((GraphDataForBDB) graphdata).setTotalHeadNodes(0);
			for (int i = 0; i < activeFlags.size(); i++) {
				activeFlags.set(i, false);
			}
		}
		if (!(graphdata instanceof GraphDataForBDB)) {
			assertEquals("Check clean.", 0, graphdata.size());
			assertEquals("Check clean.", 0, graphdata.sizeForAll());
			assertEquals("Check clean.", 0, graphdata.getActiveCounter());
			assertEquals("Check clean.", 0, graphdata.getEdgeSize());
		}
	}

	@Test
	public void testGetActiveCounter() {
		if (!(graphdata instanceof GraphDataForBDB)) {
			assertEquals("After setUp, there are 4 active vertice.", 4,
					graphdata.getActiveCounter());
			PRVertex vertex = (PRVertex) graphdata.get(0);
			graphdata.set(0, vertex, false);
			assertEquals(
					"After set one of them false, there are 3 active vertice.",
					3, graphdata.getActiveCounter());
		}

	}

	@Test
	public void testShowMemoryInfo() {
		graphdata.showMemoryInfo();
	}

	@Test
	public void testGetEdgeSize() {
		assertEquals("Check edge size.", 54, graphdata.getEdgeSize());
	}

}
