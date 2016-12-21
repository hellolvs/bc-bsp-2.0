package com.chinamobile.bcbsp.io.db;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.examples.PRVertex;
import com.chinamobile.bcbsp.graph.GraphDataForMem;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

public class TableRecordWriterTest {
	private BSPConfiguration conf;
	private BSPJob job;
	private RecordWriter output;
	private GraphDataInterface graphData = new GraphDataForMem();
	private static String[] datas = {
			"0:10.0\t3:0 1:0 2:0 4:0 0:0 0:0 0:0 0:0 0:0 1:0 0:0 1:0 3:0",
			"1:10.0\t4:0 2:0 0:0 2:0 4:0 1:0 2:0 1:0 1:0 3:0 3:0 3:0 4:0 0:0",
			"2:10.0\t2:0 1:0 4:0 2:0 3:0 3:0 0:0 3:0 0:0 1:0 3:0 4:0 1:0",
			"3:10.0\t4:0 2:0 3:0 0:0 0:0 1:0 2:0 0:0 1:0 2:0 0:0 4:0 0:0 1:0",
			"4:10.0\t4:0 0:0 1:0 1:0 4:0 2:0 1:0 1:0 0:0 4:0 1:0 2:0 3:0 0:0 2:0 4:0"
	// ":10.0\t4:0 0:0 1:0 1:0 4:0 2:0",
	// "5:\t4:0 0:0 1:0 1:0 4:0 2:0 1:0 1:0 0:0 4:0 1:0 2:0 3:0 0:0 2:0 4:0"
	};

	@Before
	public void setUp() throws Exception {
		conf = new BSPConfiguration();
//		conf.set("hbase.outputtable", "hbaseInput");
		conf.set("hbase.master", "master:60000");
		this.conf.set("hbase.zookeeper.quorum", "master");
		
		job = new BSPJob(conf, 2);
		job.setOutputTableNameForHBase("hbaseInput");
		OutputFormat outputformat = (OutputFormat) ReflectionUtils.newInstance(
				job.getConf().getClass(
						Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS,
						TableOutputFormat.class), job.getConf());
		outputformat.initialize(job.getConf());
		output = outputformat.getRecordWriter(job, new StaffAttemptID());
		for (int i = 0; i < 4; i++) {// At setUp, we add 4 vertice.Those vertice
										// contain 54 edges.
			Vertex vertex = new PRVertex();
			vertex.fromString(datas[i]);
			graphData.addForAll(vertex);
		}
	}

	@Test
	public void testWriteTextText() throws IOException, InterruptedException {
		graphData.saveAllVertices(output);
	}

}
