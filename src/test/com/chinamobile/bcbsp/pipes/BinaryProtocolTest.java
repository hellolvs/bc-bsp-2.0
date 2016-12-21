package com.chinamobile.bcbsp.pipes;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;

public class BinaryProtocolTest {
	BinaryProtocol protocol;
	Configuration conf = new Configuration();
	BSPConfiguration bspconf = new BSPConfiguration();
	BSPJob bspjob ;
	BSPStaff staff;
	StaffAttemptID staffid;
	WorkerAgentProtocol workerAgent;
	@Before
	public void setUp() throws Exception {
		bspjob=mock(BSPJob.class);
		staff=mock(BSPStaff.class);
		workerAgent=mock(WorkerAgentProtocol.class);
		staffid=mock(StaffAttemptID.class);
		conf.set("bcbsp.log.dir", "/root/Desktop");
		when(bspjob.getConf()).thenReturn( conf);
		when(staff.getStaffID()).thenReturn(staffid);
		when(staffid.toString()).thenReturn("123456");
		when(staff.getJobExeLocalPath()).thenReturn("/root/Desktop/BspMerge");
		Socket sock= new Socket();
		TaskHandler hand= new TaskHandler();
		//protocol = new BinaryProtocol(sock,hand);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBinaryProtocol() {
		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	}

	@Test
	public void testClose() {
		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	}

	@Test
	public void testStart() {
		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().start();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	}

	@Test
	public void testEndOfInput() {

		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().endOfInput();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	}

	@Test
	public void testAbort() {

		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().abort();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	}

	@Test
	public void testFlush() {

		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().flush();
		application.getDownlink().close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	}

	@Test
	public void testRunASuperStep() {

		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().runASuperStep();
		application.getDownlink().close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	}

	@Test
	public void testSendMessage() {

		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().sendMessage("asdf");
		application.getDownlink().close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	}

	@Test
	public void testSendStaffId() {


		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().sendStaffId("staffid");
		application.getDownlink().close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	
	}

	@Test
	public void testSendKeyValue() {

		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().sendKeyValue("key", "value");
		application.getDownlink().close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	}

	@Test
	public void testSendKey() {

		try {
		Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		
		application.getDownlink().sendKey("key", 1);
		application.getDownlink().close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	}

	@Test
	public void testGetPartionId() {

		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().getPartionId();
		application.getDownlink().close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	}

	@Test
	public void testSendNewAggregateValue() {

		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
			String str[] = new String[1];
			str[0]="sjz";
		application.getDownlink().sendNewAggregateValue(str);
		application.getDownlink().close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	}

	@Test
	public void testSaveResult() {


		try {
			Application application = new Application(bspjob, "WorkerManager", "/root/Desktop/BspMerge");
		application.getDownlink().saveResult();
		application.getDownlink().close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	
	
	}

}
