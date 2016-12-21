package com.chinamobile.bcbsp.bspstaff;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
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
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

public class BSPStaffTest {
	BSPConfiguration bspconf;
	WorkerAgentProtocol umbilical;
	OutputFormat outputformat;
	//BSPStaff staff;
	 RecordWriter output;
	private static String[] datas = {
		"0:10.0\t3:0 1:0 2:0 4:0 0:0 0:0 0:0 0:0 0:0 1:0 0:0 1:0 3:0",
		"1:10.0\t4:0 2:0 0:0 2:0 4:0 1:0 2:0 1:0 1:0 3:0 3:0 3:0 4:0 0:0",
		"2:10.0\t2:0 1:0 4:0 2:0 3:0 3:0 0:0 3:0 0:0 1:0 3:0 4:0 1:0",
		"3:10.0\t4:0 2:0 3:0 0:0 0:0 1:0 2:0 0:0 1:0 2:0 0:0 4:0 0:0 1:0",
		"4:10.0\t4:0 0:0 1:0 1:0 4:0 2:0 1:0 1:0 0:0 4:0 1:0 2:0 3:0 0:0 2:0 4:0" };
	GraphDataInterface graphdata =new GraphDataForMem() ;
	
	
	private BSPJobID jobid;
	Configuration conf = new Configuration();
	private BSPStaff staff;
    private static int NumCopy = 2;
    private static int FailCounter = 3;
    private HashMap<Integer, String> route = new HashMap<Integer, String>();
    private HashMap<Integer, Integer> table = new HashMap<Integer, Integer>();
    private WorkerManager workerManager;
    StaffAttemptID staffID;
	 StaffID staffid;
    private BSPJob bspJob;
    String jobfile;
    int partition;
    String splitclass;
    BytesWritable split;
	@Before
	public void setUp() throws Exception {
		
		bspconf = new BSPConfiguration();
		BSPJob  job = new BSPJob(bspconf, 2);
	
//		bspconf = new BSPConfiguration();
//		umbilical = (WorkerAgentProtocol) RPC.getProxy(
//				WorkerAgentProtocol.class, WorkerAgentProtocol.versionID,
//				new InetSocketAddress("192.168.1.2", 65002), bspconf);
//		bspconf.set(Constants.ZOOKEEPER_QUORUM, "192.168.1.3");
		for (int i = 0; i < 4; i++) {
			Vertex vertex = new PRVertex();
			vertex.fromString(datas[i]);
			graphdata.addForAll(vertex);
		}
		 OutputFormat outputformat = (OutputFormat) ReflectionUtils.newInstance(
			        job.getConf().getClass(Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS,
			            TextBSPFileOutputFormat.class), job.getConf());
			    outputformat.initialize(job.getConf());
			    output = outputformat.getRecordWriter(job,new StaffAttemptID(), new Path(
			    "hdfs://Master.Hadoop:9000/user/user/output2"));
		
		
		jobid=new BSPJobID("jobid",5);
		workerManager=mock(WorkerManager.class);
		staffid= new StaffID(jobid,5);
		staffID= new StaffAttemptID(staffid,5);
		jobfile = "jobfile";
		partition = 10;
		String splitclass="splitclass";
		split = mock(BytesWritable.class);
		staff= new BSPStaff(jobid,jobfile,staffID,partition,splitclass,split);
		bspJob=new BSPJob(jobid,"jobfile");
		//bspJob=mock(BSPJob.class);
		//when(bspJob.getConf()).thenReturn( conf);
		staff.setConf(bspJob);
	}

	@After
	public void tearDown() throws Exception {
		graphdata.clean();
		System.out.println("size:" + graphdata.sizeForAll());
		
	}

	@Test
	public void testCreateRunner() {
		BSPStaffRunner bsr=staff.createRunner(workerManager);
		if(bsr==null)
			System.out.println("bsr is null");
		assertEquals( bsr==null,false);
	}

	@Test
	public void testGetLocalBarrierNumber() {
		assertEquals(  staff.getLocalBarrierNumber("hostname"),0);
		staff.getPartitionToWorkerManagerNameAndPort().put(10, "hostname:qwe");
		assertEquals(  staff.getLocalBarrierNumber("hostname"),1);
	}

	@Test
	public void testSaveResultOfVertex() {
		for(int i=0;i<4;i++){
			Vertex vertex = new PRVertex();
		      try {
				vertex.fromString(datas[i]);
				staff.saveResultOfVertex(vertex, output);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			}
	}

}
