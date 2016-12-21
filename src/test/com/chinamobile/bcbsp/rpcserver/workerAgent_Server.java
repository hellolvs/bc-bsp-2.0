package com.chinamobile.bcbsp.rpcserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
//import org.apache.hama.HamaConfiguration;
//import org.apache.hama.ipc.JobSubmissionProtocol;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Aggregator;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.pipes.Application;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentInterface;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

public class workerAgent_Server implements WorkerAgentProtocol {

	private int i = 0;
	Server masterServer = null;
	private static int flag = 0;
	private String workerManagerName = "Master.Hadoop";
	private int workerManagerNum = 0;
	/** HashMap for storing partition and workerManagerName */
	private HashMap<Integer, String> partitionToWorkerManagerName = new HashMap<Integer, String>();
	/** Map for storing InetSocketAddress and WorkerAgent */
	private final Map<InetSocketAddress, WorkerAgentInterface> workers = new ConcurrentHashMap<InetSocketAddress, WorkerAgentInterface>();
	/** The port of job */
	private int portForJob;
	/** Define InetSocketAddress */
	private InetSocketAddress workAddress;
	/** Define Server */
	private Server server = null;
	/** Define BSP Configuration */
	private Configuration conf;
	/** ID of BSPJob */
	private BSPJobID jobId;
	/** Define BSPJob object */
	private BSPJob jobConf;
	/** Define WorkerManager object */
	private WorkerManager workerManager;
	/** Map for registering aggregate values */
	private HashMap<String, Class<? extends AggregateValue<?, ?>>> nameToAggregateValue = new HashMap<String, Class<? extends AggregateValue<?, ?>>>();
	/** Map for registering aggregate values */
	private HashMap<String, Class<? extends Aggregator<?>>> nameToAggregator = new HashMap<String, Class<? extends Aggregator<?>>>();
	/** Map for registering aggregate values */
	@SuppressWarnings("rawtypes")
	private HashMap<String, ArrayList<AggregateValue>> aggregateValues = new HashMap<String, ArrayList<AggregateValue>>();
	/** Map for registering aggregate values */
	@SuppressWarnings("rawtypes")
	private HashMap<String, AggregateValue> aggregateResults = new HashMap<String, AggregateValue>();
	/** Define BSPJob object */
	private Counters counters = new Counters();
	/** Define Application object */
	private Application application;

	public workerAgent_Server() {
		i = 1;
		BSPConfiguration bspconf = new BSPConfiguration();
		try {
			masterServer = RPC.getServer(this, "192.168.1.2", 65002, bspconf);
			this.start();
		} catch (Exception e) {
			System.out.println(e);
		}

	}

	public void print() {
		// while (this.flag == 0) {
		// try {
		// Thread.sleep(100);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		System.out.println("the Server is on!");
		// }
	}

	public void start() {

		try {
			masterServer.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void stop() {
		masterServer.stop();
		this.flag = 1;
		System.out.println("the Server is OFF!");
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {

		return WorkerAgentProtocol.versionID;
	}

	@Override
	public boolean localBarrier(BSPJobID jobId, StaffAttemptID staffId,
			int superStepCounter, SuperStepReportContainer ssrc) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getNumberWorkers(BSPJobID jobId, StaffAttemptID staffId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setNumberWorkers(BSPJobID jobId, StaffAttemptID staffId, int num) {
		// TODO Auto-generated method stub
		this.workerManagerNum = num;
	}

	@Override
	public String getWorkerManagerName(BSPJobID jobId, StaffAttemptID staffId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BSPJobID getBSPJobID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWorkerNametoPartitions(BSPJobID jobId, int partitionId,
			String hostName) {
		this.partitionToWorkerManagerName.put(partitionId, hostName + ":"
				+ this.portForJob);

	}

	@Override
	public int getFreePort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setStaffAgentAddress(StaffAttemptID staffID, String addr) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearStaffRC(BSPJobID jobId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Staff getStaff(StaffAttemptID staffId) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean ping(StaffAttemptID staffId) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void done(StaffAttemptID staffid, boolean shouldBePromoted)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void fsError(StaffAttemptID staffId, String message)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setStaffStatus(StaffAttemptID staffId, int staffStatus,
			Fault fault, int stage) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean getStaffRecoveryState(StaffAttemptID staffId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getStaffChangeWorkerState(StaffAttemptID staffId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getFailCounter(StaffAttemptID staffId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void addStaffReportCounter(BSPJobID jobId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addCounters(BSPJobID jobId, Counters pCounters) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getMigrateSuperStep(StaffAttemptID staffId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean updateWorkerJobState(StaffAttemptID staffId) {
		// TODO Auto-generated method stub
		return false;
	}

	public void setWorkerManagerName(String workerManagerName) {
		this.workerManagerName = workerManagerName;
	}

	public static int getFlag() {
		return flag;
	}

	public static void setFlag(int flag) {
		workerAgent_Server.flag = flag;
	}

}
