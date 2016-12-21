package com.chinamobile.bcbsp.pipes;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.client.BSPJobClient;
import com.chinamobile.bcbsp.util.BSPJob;

public class SubmitterTest {
	BSPJob bspjob;
	@Before
	public void setUp() throws Exception {
		BSPConfiguration bspconf=new BSPConfiguration();
		Configuration conf=new Configuration();
		bspjob=mock(BSPJob.class);
		BSPJobClient bjc=mock(BSPJobClient.class);
	
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRun() throws Exception {
		/*Submitter submitter=new Submitter();
		String[] args=new String[6];
			args[0]= "-submit";
			args[1]="<FileName.exe>";
			args[2]="nsuperstep";
			args[3]="datainput";
			args[4]="dataoutput";
			args[5]="userconfig";
		submitter.run( args);*/
		fail("Not yet implemented");
	}

	@Test
	public void testDisplayUsage() {
		fail("Not yet implemented");
	}

	@Test
	public void testMain() {
		fail("Not yet implemented");
	}

}
