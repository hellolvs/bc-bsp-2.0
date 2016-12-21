package com.chinamobile.bcbsp.io;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.io.db.TableInputFormat;
import com.chinamobile.bcbsp.io.mysql.DBInputFormat;
import com.chinamobile.bcbsp.util.BSPJob;

public class BSPFileInputFormatTest {
	private BSPJob job;

	@Before
	public void setUp() throws Exception {
		BSPConfiguration conf = new BSPConfiguration();
		job = new BSPJob(conf, 2);

		// Please send the path to your path
		KeyValueBSPFileInputFormat.addInputPath(job, new Path(
				"hdfs://Master:9000/user/root/input"));
	}

	@Test
	public void testGetSplits() throws IOException, InterruptedException {
		Configuration confs = job.getConf();
		com.chinamobile.bcbsp.io.InputFormat<?, ?> input = ReflectionUtils
				.newInstance(KeyValueBSPFileInputFormat.class, confs);

		input.getSplits(job);
		assertEquals(1, input.getSplits(job).size());
	}

	@Test
	public void testCreateRecordReaderInputSplitBSPJob() throws IOException,
			InterruptedException {
		RecordReader input = null;

		InputFormat inputformat = (InputFormat) ReflectionUtils.newInstance(
				job.getConf().getClass(
						Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS,
						KeyValueBSPFileInputFormat.class), job.getConf());
		com.chinamobile.bcbsp.io.InputFormat<?, ?> input1 = ReflectionUtils
				.newInstance(KeyValueBSPFileInputFormat.class, job.getConf());
		input1.initialize(job.getConf());
		input1.getSplits(job);

		inputformat.initialize(job.getConf());
		System.out.println(input1.getSplits(job).get(0).getLength());
		input = inputformat.createRecordReader(input1.getSplits(job).get(0),
				job);
		input.initialize(input1.getSplits(job).get(0), job.getConf());

		if (input.nextKeyValue()) {
			Text key = new Text(input.getCurrentKey().toString());
			System.out.println(key);
		}
	}
}
