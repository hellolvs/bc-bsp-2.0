package com.chinamobile.bcbsp.pipes;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.Communicator;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.comm.MessageQueuesInterface;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;

public class TaskHandlerTest {
	private int partitionID = 0;
	  private BSPConfiguration conf;
	  private BSPJob job;
	  private BSPJobID jobID;
	  private Partitioner<Text> partitioner;
	  private int dstPartitionID = 1;
	  private MessageQueuesInterface messageQueues;
	  private IMessage msg;
	  private Communicator comm;
	 private BSPStaff bspstaff;
	 private WorkerAgentProtocol workerAgent;
	 private StaffAttemptID staffid;
	 private Staff staff;
	 private TaskHandler handler;
	 
	 
	 class MyThread1 implements Runnable{
			private TaskHandler hand;
			 MyThread1(TaskHandler ahand){
				 hand=ahand;
			 }
			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					hand.waitForFinish();
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					System.out.println(e);
					e.printStackTrace();
				}
				
			}  
			
		}
		class MyThread2 implements Runnable{
			private TaskHandler hand;
			 MyThread2(TaskHandler ahand){
				 hand=ahand;
			 }
			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					hand.currentSuperStepDone();
					assertEquals("test for waitforfinish",hand.waitForFinish(),true);
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					System.out.println(e);
					e.printStackTrace();
				}
				
			}  
			
		}
	@Before
	public void setUp() throws Exception {
		job = mock(BSPJob.class);
		staff =mock(Staff.class);
    conf = new BSPConfiguration();
    jobID = new BSPJobID();
    partitioner = new HashPartitioner<Text>();
    staffid=mock(StaffAttemptID.class);
    when(staff.getStaffAttemptId()).thenReturn(staffid);
	when(staffid.toString()).thenReturn("123456");
	handler = new TaskHandler();
	comm = new Communicator(jobID, job, partitionID, partitioner);
	handler.setCommunicator(comm);
	
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTaskHandlerCommunicator() {
		job = mock(BSPJob.class);
	    comm = new Communicator(jobID, job, partitionID, partitioner);
	    TaskHandler handler = new TaskHandler(comm);
	    assertEquals("TaskHandler Constructor:create a runner", handler==null,false);
	}

	@Test
	public void testTaskHandler() {
		TaskHandler handler = new TaskHandler();
		 assertEquals("TaskHandler Constructor:create a runner", handler==null,false);
	}

	@Test
	public void testTaskHandlerBSPJobStaffWorkerAgentProtocol() {
		TaskHandler handler = new TaskHandler(job,staff,workerAgent);
		assertEquals("TaskHandler Constructor:create a runner", handler==null,false);
	}
	@Ignore(value="need communicator.testsend()")
	@Test
	public void testSendNewMessage() throws IOException {
		
		handler.sendNewMessage("1:3:3");
	}

	@Test
	public void testSetAgregateValue() {
		try {
			handler.setAgregateValue("AggregateValue");
		} catch (IOException e) {
			// TODO Auto-generated catch blocknew Communicator(jobID, job, partitionID, partitioner);
			e.printStackTrace();
			System.out.println(e);
		}
	}

	@Test
	public void testCurrentSuperStepDone() {
		try {
			handler.currentSuperStepDone();
			assertEquals("testCurrentSuperStepDone",handler.waitForFinish(),true);
			handler.currentSuperStepDone();
			assertEquals("testCurrentSuperStepDone",handler.waitForFinish(),true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
		
	}

	@Test
	public void testFailed() {
		Throwable e = new Throwable();
		handler.failed(e);
		try {
			assertEquals("testCurrentSuperStepDone",handler.waitForFinish(),false);
		} catch (Throwable e1) {
			// TODO Auto-generated catch block
			System.out.println(e1);
			e1.printStackTrace();
		}
	}

	@Test
	public void testWaitForFinish() {
		try {
			MyThread1 t1=new MyThread1(handler);
			MyThread2 t2=new MyThread2(handler);
			new Thread(t1).start();
			Thread.sleep(10000);
			//Thread.CurrentThread.sleep(1000);
			new Thread(t2).start();
			
			/*handler.waitForFinish();
			assertEquals("testCurrentSuperStepDone",handler.waitForFinish()==false);
			handler.currentSuperStepDone();
			assertEquals("testCurrentSuperStepDone",handler.waitForFinish());*/
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
	}
	@Ignore(value="tested in CommunicatorTest")
	@Test
	public void testGetMessage() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetAggregateValue() {
		try {
			handler.setAgregateValue("1234");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
		ArrayList s=new ArrayList();
		 s=handler.getAggregateValue();
		 boolean a;
		 if(s==null){
			 a=false;
		 }else{
			 a=true;
		 }
		assertEquals("testGetAggregateValue",s==null,false);
		//assertEquals("testCurrentSuperStepDone",handler.waitForFinish()==false);
	}

	@Test
	public void testAddAggregateValue() {
		int i=handler.getAggregateValue().size();
		handler.addAggregateValue("asdf");
		int j=handler.getAggregateValue().size();
		String s=handler.getAggregateValue().get(i);
		assertEquals("testAddAggregateValue",j==i+1&&s.equalsIgnoreCase("asdf"),true);
	}

	@Test
	public void testSetAggregateValue() {
		ArrayList aggregateValue=new ArrayList();
		handler.setAggregateValue(aggregateValue);
		assertEquals("testSetAggregateValue",handler.getAggregateValue().hashCode()==aggregateValue.hashCode(),true);
	}

	@Test
	public void testGetCommunicator() {
		CommunicatorInterface commm= mock(Communicator.class);
		TaskHandler hand=new TaskHandler();
		hand.setCommunicator(commm);
		hand.getCommunicator();
		assertEquals("testGetCommunicator",hand.getCommunicator()==null,false);
	}

	@Test
	public void testSetCommunicator() {
		CommunicatorInterface commm= mock(Communicator.class);
		TaskHandler hand=new TaskHandler();
		hand.setCommunicator(commm);
		hand.getCommunicator();
		assertEquals("testGetCommunicator",hand.getCommunicator()==null,false);
		
	}
	@Ignore(value="unused")
	@Test
	public void testSaveResultStringStringStringArray() {
		fail("Not yet implemented");
	}

	@Test
	public void testOpenDFS() {
		fail("Not yet implemented");
	}
	@Ignore(value="unused")
	@Test
	public void testSaveResultArrayListOfString() {
		fail("Not yet implemented");
	}

	@Test
	public void testSaveResultString() {
		fail("Not yet implemented");
	}

	@Test
	public void testCloseDFS() {
		fail("Not yet implemented");
	}

}
