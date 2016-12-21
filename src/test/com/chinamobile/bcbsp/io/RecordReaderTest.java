package com.chinamobile.bcbsp.io;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.client.BSPJobClient.RawSplit;
import com.chinamobile.bcbsp.examples.PRVertex;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

public class RecordReaderTest {
	private BSPJob job;
	private RawSplit rawSplit1 = new RawSplit();
	private String rawSplitClass = rawSplit1.getClassName();
	private BytesWritable rawSplit = new BytesWritable();
	private RecordReader input = null;

	// private RecordReader input = null;

	@Before
	public void setUp() throws Exception {
		BSPConfiguration conf = new BSPConfiguration();
		job = new BSPJob(conf, 2);

		KeyValueBSPFileInputFormat.addInputPath(job, new Path(
				"hdfs://Master:9000/user/root/input"));

		InputFormat inputformat = (InputFormat) ReflectionUtils.newInstance(
				job.getConf().getClass(
						Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS,
						KeyValueBSPFileInputFormat.class), job.getConf());
		com.chinamobile.bcbsp.io.InputFormat<?, ?> input1 = ReflectionUtils
				.newInstance(KeyValueBSPFileInputFormat.class, job.getConf());
		input1.initialize(job.getConf());
		input1.getSplits(job);

		inputformat.initialize(job.getConf());
		input = inputformat.createRecordReader(input1.getSplits(job).get(0),
				job);
		input.initialize(input1.getSplits(job).get(0), job.getConf());
	}

	@Test
	public void testNextKeyValue() throws IOException, InterruptedException {
		assertEquals(true, input.nextKeyValue());
	}

}
