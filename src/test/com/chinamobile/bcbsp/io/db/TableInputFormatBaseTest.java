package com.chinamobile.bcbsp.io.db;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.io.InputFormat;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl.BSPTableImpl;
import com.chinamobile.bcbsp.util.BSPJob;

public class TableInputFormatBaseTest {
	private BSPConfiguration conf;
	private BSPJob job;

	@Before
	public void setUp() throws Exception {
		conf = new BSPConfiguration();
		conf.set("hbase.mapreduce.inputtable", "hbaseOutput");

		job = new BSPJob(conf, 2);
		job.setInputTableNameForHBase("hbaseOutput");
	}

	@Test
	public void testGetSplits() throws IOException, InterruptedException {
		Configuration confs = job.getConf();
		confs.set("hbase.master", "master:60000");
		confs.set("hbase.zookeeper.quorum", "master");
		confs.set("hbase.mapreduce.scan.column.family.input", "BorderNode");
		// confs.set("hbase.mapreduce.inputtable", "hbaseOutput");

		com.chinamobile.bcbsp.io.InputFormat<?, ?> input = ReflectionUtils
				.newInstance(TableInputFormat.class, job.getConf());
		input.initialize(job.getConf());
		input.getSplits(job);
		assertEquals(1, input.getSplits(job).size());
	}

	@Test
	public void testCreateRecordReaderInputSplitBSPJob() throws IOException, InterruptedException {
		RecordReader input = null;
		Configuration confs = job.getConf();
		confs.set("hbase.master", "master:60000");
		confs.set("hbase.zookeeper.quorum", "master");
		confs.set("hbase.mapreduce.scan.column.family.input", "BorderNode");
		confs.set("hbase.mapreduce.inputtable", "hbaseOutput");
		
//		new TableSplit(table.getTableName(), startRow,
//	            endRow, regionLocation);
		
		InputFormat inputformat = (InputFormat) ReflectionUtils.newInstance(
				job.getConf().getClass(
						Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS,
						TableInputFormat.class), job.getConf());
		com.chinamobile.bcbsp.io.InputFormat<?, ?> input1 = ReflectionUtils
				.newInstance(TableInputFormat.class, job.getConf());
		input1.initialize(job.getConf());
		input1.getSplits(job);
		
		inputformat.initialize(job.getConf());
		input = inputformat.createRecordReader(input1.getSplits(job).get(0), job);
	}
}
