/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chinamobile.bcbsp.bspstaff;

import com.chinamobile.bcbsp.ActiveMQBroker;
import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.Constants.BspCounters;
import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Aggregator;
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.assistCheckpoint;
//import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspcontroller.Counters;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.Communicator;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.CommunicatorNew;
import com.chinamobile.bcbsp.comm.GraphStaffHandler;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.comm.RPCCommunicator;
import com.chinamobile.bcbsp.comm.io.util.MemoryAllocator;
//import com.chinamobile.bcbsp.examples.bytearray.pagerank.PageRankMessage;
import com.chinamobile.bcbsp.fault.storage.AggValueCheckpoint;
import com.chinamobile.bcbsp.fault.storage.Checkpoint;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.fault.storage.Fault.Level;
import com.chinamobile.bcbsp.fault.storage.Fault.Type;
import com.chinamobile.bcbsp.fault.storage.assistCheckpointDefault;
import com.chinamobile.bcbsp.graph.GraphDataFactory;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.graph.MetaDataOfGraph;
import com.chinamobile.bcbsp.graph.VertexManager;
import com.chinamobile.bcbsp.io.InputFormat;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.ml.BSPPeer;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.partition.HashWithBalancerWritePartition;
import com.chinamobile.bcbsp.partition.HashWritePartition;
import com.chinamobile.bcbsp.partition.NotDivideWritePartition;
import com.chinamobile.bcbsp.partition.RangeWritePartition;
import com.chinamobile.bcbsp.partition.RecordParseDefault;
import com.chinamobile.bcbsp.partition.WritePartition;
import com.chinamobile.bcbsp.pipes.Application;
import com.chinamobile.bcbsp.router.routeparameter;
import com.chinamobile.bcbsp.sync.StaffSSController;
import com.chinamobile.bcbsp.sync.StaffSSControllerInterface;
import com.chinamobile.bcbsp.sync.SuperStepCommand;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileSystem;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPFileSystemImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;
import com.chinamobile.bcbsp.workermanager.WorkerManager;
//import com.chinamobile.bcbsp.pipes.Application;




import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * BSPStaff A BSPStaff is an entity that executes the local computation of a
 * BSPJob. A BSPJob usually consists of many BSPStaffs which are distributed
 * among the workers.
 * 
 * @author
 * @version
 */
public class BSPStaff extends Staff implements GraphStaffHandler {
	/**
	 * The worker agent for staff.
	 */
	private WorkerAgentForStaffInterface staffAgent;
	/**
	 * The BSP Job configuration.
	 */
	private BSPJob bspJob;
	/**
	 * ActiveMQBroker Provides message middleware ActiveMQ messaging service.
	 */
	private ActiveMQBroker activeMQBroker;
	/**
	 * The avtiveMQ port.
	 */
	private int activeMQPort;
	/**
	 * The communication tool for BSP. It manages the outgoing and incoming
	 * queues of each staff.
	 */
	private CommunicatorInterface communicator;
	
	private Configuration conf;
	
	private int checkPointFrequency;

	/* Zhicheng Liu added */
	/**
	 * The partition RPC port.
	 */
	private int partitionRPCPort;
	/**
	 * The split information.
	 */
	private BytesWritable rawSplit = new BytesWritable();
	/**
	 * The split class.
	 */
	private String rawSplitClass;
	/**
	 * The graph data.
	 */
	private GraphDataInterface graphData;

	/**
	 * <partitionID--hostName:port1-port2>.
	 */
	private HashMap<Integer, String> partitionToWorkerManagerHostWithPorts = new HashMap<Integer, String>();
	/**
	 * The hash bucket to partition.
	 */
	private HashMap<Integer, Integer> hashBucketToPartition = null;
	/**
	 * The range router.
	 */
	private HashMap<Integer, Integer> RangeRouter = null;
	/**
	 * The router parameter.
	 */
	private routeparameter routerparameter = new routeparameter();

	/**
	 * Get the range router.
	 * 
	 * @return RangeRouter
	 */
	public HashMap<Integer, Integer> getRangeRouter() {
		return RangeRouter;
	}

	/**
	 * Set the range router.
	 * 
	 * @param rangeRouter
	 *            the range router
	 */
	public void setRangeRouter(HashMap<Integer, Integer> rangeRouter) {
		RangeRouter = rangeRouter;
	}

	// variable for barrier
	/**
	 * The staff super step controller.
	 */
	private StaffSSControllerInterface sssc;
	/**
	 * Total staff number.
	 */
	private int staffNum = 0;
	/**
	 * Total worker manager number.
	 */
	private int workerMangerNum = 0;
	/**
	 * The local barrier number.
	 */
	private int localBarrierNum = 0;

	// variable for local computation
	/**
	 * The max super step number.
	 */
	private int maxSuperStepNum = 0;
	/**
	 * The current super step counter.
	 */
	private int currentSuperStepCounter = 0;
	/**
	 * The active counter.
	 */
	private long activeCounter = 0;
	/**
	 * The local compute flag. true:start the local compute.
	 */
	private boolean flag = true;
	/**
	 * The super Step command.
	 */
	private SuperStepCommand ssc;

	// For Partition
	/**
	 * The partitioner.
	 */
	private Partitioner<Text> partitioner;
	/**
   * 
   */
	private int numCopy = 100;
	/**
	 * The number of verteices from other staff that could not be parsed.
	 */
	private int lost = 0;
	/**
	 * Loadbalance vertex size
	 */
	private int vertexSize = 0;
	// For Aggregation
	/** Map for user registered aggregate values. */
	private HashMap<String, Class<? extends AggregateValue<?, ?>>> nameToAggregateValue = new HashMap<String, Class<? extends AggregateValue<?, ?>>>();
	/** Map for user registered aggregatros. */
	private HashMap<String, Class<? extends Aggregator<?>>> nameToAggregator = new HashMap<String, Class<? extends Aggregator<?>>>();

	/** Map to cache of the aggregate values aggregated for each vertex. */
	@SuppressWarnings("unchecked")
	private HashMap<String, AggregateValue> aggregateValues = new HashMap<String, AggregateValue>();
	/** Map to instance of the aggregate values for the current vertex. */
	@SuppressWarnings("unchecked")
	private HashMap<String, AggregateValue> aggregateValuesCurrent = new HashMap<String, AggregateValue>();

	/** Map to cache of the aggregate values calculated last super step. */
	@SuppressWarnings("unchecked")
	private HashMap<String, AggregateValue> aggregateResults = new HashMap<String, AggregateValue>();

	/**
	 * The record parse to parse graph data from HDFS.
	 */
	private RecordParse recordParse = null;
	/**
	 * The log in log4j,to write logs.
	 */
	private static final Log LOG = LogFactory.getLog(BSPStaff.class);
	/**
	 * The recovery times.
	 */
	private int recoveryTimes = 0;

	/** The rpc server */
	private Server server;

	/** The c++ application in java */
	private Application application;
	// add by chen
	/**
	 * The counters in system.
	 */
	private Counters counters;

	/* Zhicheng Liu added */
	/**
	 * The staff start time.
	 */
	private long staffStartTime = 0;
	/**
	 * The staff end time.
	 */
	private long staffEndTime = 0;
	/**
	 * The staff run time.
	 */
	private long staffRunTime = 0; // ms
	/**
	 * Read and write check point time.
	 */
	private long rwCheckPointT = 0; // ms
	/**
	 * Load data time.
	 */
	private long loadDataT = 0; // ms
	/**
	 * The migrate Staff cost.
	 */
	private long migrateCost = 0; // ms
	/** The byte number of graphData. */
	private long graphBytes = 0;
	/** The per message length. */
	private int messagePerLength = 0;
	/** The byte number of incoming messages. */
	private long messageBytes = 0;
	/** The migrate flag,true:migrate the Staff. */
	private boolean migrateFlag = false;
	/** The Staff is a slow staff. */
	private boolean hasMigrateStaff = false;
	/** The migrate mode,true:open migrate mode. */
	private boolean openMigrateMode = false;
	
	/** Judge need assist checkpoint. */
	private boolean aggCpFlag = false;
	
	private String assCKinfo = null;

	/***/
	private int fs = 0;
	/** added for migrate staff string messages */
	private ConcurrentLinkedQueue<String> migrateMessagesString = new ConcurrentLinkedQueue<String>();
	/**Biyahui added*/
	private ConcurrentLinkedQueue<String> checkPointMessages = new ConcurrentLinkedQueue<String>();
	/** added for migrated staff flag */
	private boolean migratedStaffFlag = false;
	/**BSP compute.*/
	private BSP bsp;
	/**income message for migrate.*/
	private Map<String, LinkedList<IMessage>> icomMess = null;
	//Biyahui added
	private boolean recoveryFlag=false;
	/** vertex class. */
	private Class<? extends Vertex<?, ?, ?>> vertexClass;
	/** vertex manager. */
	private VertexManager vManager;
	  
	/** The default constructor. */
	public BSPStaff() {

	}

	/**
	 * The constructor of BSPStaff.
	 * 
	 * @param jobId
	 *            The current BSP Job id.
	 * @param jobFile
	 *            The BSP Job file.
	 * @param staffId
	 *            The current BSP Staff id.
	 * @param partition
	 *            The partition owns by the current staff
	 * @param splitClass
	 *            The split class
	 * @param split
	 */
	public BSPStaff(BSPJobID jobId, String jobFile, StaffAttemptID staffId,
			int partition, String splitClass, BytesWritable split) {
		this.setJobId(jobId);
		this.setJobFile(jobFile);
		this.setSid(staffId);
		this.setPartition(partition);

		this.rawSplitClass = splitClass;
		this.rawSplit = split;
	}

	/**
	 * Get staff number.
	 * 
	 * @return staff number
	 */
	public int getStaffNum() {
		return staffNum;
	}

	/**
	 * Get the
	 * 
	 * @return
	 */
	public int getNumCopy() {
		return numCopy;
	}

	/**
	 * 
	 * @param numCopy
	 */
	public void setNumCopy(int numCopy) {
		this.numCopy = numCopy;
	}

	/**
	 * Get the Hash bucket to partition.
	 * 
	 * @return Hash bucket to partition
	 */
	public HashMap<Integer, Integer> getHashBucketToPartition() {
		return this.hashBucketToPartition;
	}

	/**
	 * Set the Hash bucket to partition.
	 * 
	 * @param hashBucketToPartition
	 *            the Hash bucket to partition.
	 */
	public void setHashBucketToPartition(
			HashMap<Integer, Integer> hashBucketToPartition) {
		this.hashBucketToPartition = hashBucketToPartition;
	}

	/**
	 * Get graph data.
	 * 
	 * @return graph data
	 */
	public GraphDataInterface getGraphData() {
		return graphData;
	}

	/**
	 * Set graph data.
	 * 
	 * @param graph
	 *            graph data
	 */
	public void setGraphData(GraphDataInterface graph) {
		this.graphData = graph;
	}

	@Override
	public BSPStaffRunner createRunner(WorkerManager workerManager) {
		return new BSPStaffRunner(this, workerManager, this.bspJob);
	}

	/** just for test */
	public void loadData(BSPJob job) throws ClassNotFoundException,
			IOException, InterruptedException {
		int i = 0;
		this.partitioner = (Partitioner<Text>) ReflectionUtils.newInstance(
				job.getConf().getClass(
						Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
						HashPartitioner.class), job.getConf());
		if (i == 1) {
			throw new ClassNotFoundException();
		} else if (i == 2) {
			throw new IOException();
		} else if (i == 3) {
			throw new InterruptedException();
		}
	}

	/**
	 * loadData: load data for the staff.
	 * 
	 * @param job
	 *            BSP job configuration
	 * @param workerAgent
	 *            Protocol that staff child process uses to contact its parent
	 *            process
	 * @return boolean
	 * @throws ClassNotFoundException
	 * @throws IOException
	 *             e
	 * @throws InterruptedException
	 *             e
	 */
	@SuppressWarnings("unchecked")
	public boolean loadData(BSPJob job, WorkerAgentProtocol workerAgent,
			WorkerAgentForStaffInterface aStaffAgent)
			throws ClassNotFoundException, IOException, InterruptedException {
		// rebuild the input split
		RecordReader input = null;
		org.apache.hadoop.mapreduce.InputSplit split = null;
		if (rawSplitClass.equals("no")) {
			input = null;
		} else {

			DataInputBuffer splitBuffer = new DataInputBuffer();
			splitBuffer.reset(rawSplit.getBytes(), 0, rawSplit.getLength());
			SerializationFactory factory = new SerializationFactory(
					job.getConf());
			Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deserializer = (Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>) factory
					.getDeserializer(job.getConf()
							.getClassByName(rawSplitClass));
			deserializer.open(splitBuffer);
			split = deserializer.deserialize(null);

			// rebuild the InputFormat class according to the user configuration
			InputFormat inputformat = (InputFormat) ReflectionUtils
					.newInstance(
							job.getConf()
									.getClass(
											Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS,
											InputFormat.class), job.getConf());
			inputformat.initialize(job.getConf());
			input = inputformat.createRecordReader(split, job);
			input.initialize(split, job.getConf());
		}
		SuperStepReportContainer ssrc = new SuperStepReportContainer();
		ssrc.setPartitionId(this.getPartition());
		this.numCopy = (int) (1 / (job.getConf().getFloat(
				Constants.USER_BC_BSP_JOB_BALANCE_FACTOR,
				(float) Constants.USER_BC_BSP_JOB_BALANCE_FACTOR_DEFAULT)));
		ssrc.setNumCopy(numCopy);
		ssrc.setCheckNum(this.staffNum);
		StaffSSControllerInterface lsssc = new StaffSSController(
				this.getJobId(), this.getSid(), workerAgent);
		long start = System.currentTimeMillis();
		LOG.info("in BCBSP with PartitionType is: Hash" + " start time:"
				+ start);
		if (this.staffNum == 1
				|| job.getConf().getBoolean(Constants.USER_BC_BSP_JOB_ISDIVIDE,
						false)) {

			this.partitioner = (Partitioner<Text>) ReflectionUtils.newInstance(
					job.getConf().getClass(
							Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
							HashPartitioner.class), job.getConf());
			this.partitioner.setNumPartition(this.staffNum);
			this.partitioner.intialize(job, split);

			WritePartition writePartition = new NotDivideWritePartition();
			/*
			 * RecordParse recordParse = (RecordParse) ReflectionUtils
			 * .newInstance( job.getConf() .getClass(
			 * Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
			 * RecordParseDefault.class), job .getConf());
			 * recordParse.init(job); //add by chen for null bug
			 * this.recordParse = recordParse; //this.recordParse.init(job);
			 */
			writePartition.setRecordParse(this.recordParse);
			writePartition.setStaff(this);
			writePartition.write(input);

			ssrc.setDirFlag(new String[] { "1" });
			ssrc.setCheckNum(this.staffNum);
			lsssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);

			LOG.info("The number of verteices from other staff"
					+ " that cound not be parsed:" + this.lost);
			LOG.info("in BCBSP with PartitionType is:HASH"
					+ " the number of HeadNode in this partition is:"
					+ graphData.sizeForAll());

			graphData.finishAdd();
			ssrc.setCheckNum(this.staffNum * 2);
			ssrc.setDirFlag(new String[] { "2" });
			lsssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);

		} else {
			this.partitioner = (Partitioner<Text>) ReflectionUtils.newInstance(
					job.getConf().getClass(
							Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
							HashPartitioner.class), job.getConf());

			WritePartition writePartition = (WritePartition) ReflectionUtils
					.newInstance(
							job.getConf()
									.getClass(
											Constants.USER_BC_BSP_JOB_WRITEPARTITION_CLASS,
											HashWritePartition.class), job
									.getConf());
			int multiple = 1;
			if (writePartition instanceof HashWithBalancerWritePartition) {
				this.partitioner.setNumPartition(this.staffNum * numCopy);
				multiple = 2;
			} else {

				this.partitioner.setNumPartition(this.staffNum);
				multiple = 1;
				if (writePartition instanceof RangeWritePartition) {
					multiple = 2;
				}
			}
			this.partitioner.intialize(job, split);
			/*
			 * RecordParse recordParse = (RecordParse) ReflectionUtils
			 * .newInstance( job.getConf() .getClass(
			 * Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
			 * RecordParseDefault.class), job .getConf());
			 * recordParse.init(job); // this.recordParse = (RecordParse)
			 * ReflectionUtils.newInstance( // job.getConf().getClass( //
			 * Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS, //
			 * RecordParseDefault.class), job.getConf()); //
			 * this.recordParse.init(job); this.recordParse = recordParse;
			 */
			writePartition.setPartitioner(partitioner);
			writePartition.setRecordParse(this.recordParse);
			writePartition.setStaff(this);
			writePartition.setWorkerAgent(aStaffAgent);
			writePartition.setSsrc(ssrc);
			writePartition.setSssc(lsssc);

			writePartition.setTotalCatchSize(job.getConf().getInt(
					Constants.USER_BC_BSP_JOB_TOTALCACHE_SIZE,
					Constants.USER_BC_BSP_JOB_TOTALCACHE_SIZE_DEFAULT));

			int threadNum = job.getConf().getInt(
					Constants.USER_BC_BSP_JOB_SENDTHREADNUMBER,
					Constants.USER_BC_BSP_JOB_SENDTHREADNUMBER_DEFAULT);
			if (threadNum > this.staffNum) {
				threadNum = this.staffNum - 1;
			}
			writePartition.setSendThreadNum(threadNum);
			writePartition.write(input);

			ssrc.setDirFlag(new String[] { "1" });
			ssrc.setCheckNum(this.staffNum * multiple);
			lsssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);

			LOG.info("The number of verteices from other staff that"
					+ " cound not be parsed:" + this.lost);
			LOG.info("in BCBSP with PartitionType is:HASH"
					+ " the number of HeadNode in this partition is:"
					+ graphData.sizeForAll());

			graphData.finishAdd();

			ssrc.setCheckNum(this.staffNum * (multiple + 1));
			ssrc.setDirFlag(new String[] { "2" });
			lsssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);
		}

		long end = System.currentTimeMillis();
		LOG.info("in BCBSP with PartitionType is:HASH" + " end time:" + end);
		LOG.info("in BCBSP with PartitionType is:HASH" + " using time:"
				+ (float) (end - start) / 1000 + " seconds");
		
		return true;
	}

	/**
	 * loadData: load data for the staff c++.
	 * 
	 * @param job
	 *            BSP job configuration.
	 * @param workerAgent
	 *            Protocol that staff child process uses to contact its parent
	 *            process
	 * @param application
	 *            communication with c++ process.
	 * @return boolean
	 * @throws ClassNotFoundException
	 *             e
	 * @throws IOException
	 *             e
	 * @throws InterruptedException
	 *             e
	 */
	@SuppressWarnings("unchecked")
	public boolean loadData(BSPJob job, WorkerAgentProtocol workerAgent,
			WorkerAgentForStaffInterface aStaffAgent, Application application)
			throws ClassNotFoundException, IOException, InterruptedException {
		// rebuild the input split
		RecordReader input = null;
		org.apache.hadoop.mapreduce.InputSplit split = null;
		if (rawSplitClass.equals("no")) {
			input = null;
		} else {

			DataInputBuffer splitBuffer = new DataInputBuffer();
			splitBuffer.reset(rawSplit.getBytes(), 0, rawSplit.getLength());
			SerializationFactory factory = new SerializationFactory(
					job.getConf());
			Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deserializer = (Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>) factory
					.getDeserializer(job.getConf()
							.getClassByName(rawSplitClass));
			deserializer.open(splitBuffer);
			split = deserializer.deserialize(null);

			// rebuild the InputFormat class according to the user configuration
			InputFormat inputformat = (InputFormat) ReflectionUtils
					.newInstance(
							job.getConf()
									.getClass(
											Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS,
											InputFormat.class), job.getConf());
			inputformat.initialize(job.getConf());
			input = inputformat.createRecordReader(split, job);
			input.initialize(split, job.getConf());
		}
		SuperStepReportContainer ssrc = new SuperStepReportContainer();
		ssrc.setPartitionId(this.getPartition());
		this.numCopy = (int) (1 / (job.getConf().getFloat(
				Constants.USER_BC_BSP_JOB_BALANCE_FACTOR,
				(float) Constants.USER_BC_BSP_JOB_BALANCE_FACTOR_DEFAULT)));
		ssrc.setNumCopy(numCopy);
		ssrc.setCheckNum(this.staffNum);
		StaffSSControllerInterface lsssc = new StaffSSController(
				this.getJobId(), this.getSid(), workerAgent);
		long start = System.currentTimeMillis();
		LOG.info("in BCBSP with PartitionType is: Hash" + " start time:"
				+ start);
		if (this.staffNum == 1
				|| job.getConf().getBoolean(Constants.USER_BC_BSP_JOB_ISDIVIDE,
						false)) {

			this.partitioner = (Partitioner<Text>) ReflectionUtils.newInstance(
					job.getConf().getClass(
							Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
							HashPartitioner.class), job.getConf());
			this.partitioner.setNumPartition(this.staffNum);
			this.partitioner.intialize(job, split);

			WritePartition writePartition = new NotDivideWritePartition();
			/*
			 * RecordParse recordParse = (RecordParse) ReflectionUtils
			 * .newInstance( job.getConf() .getClass(
			 * Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
			 * RecordParseDefault.class), job .getConf());
			 * recordParse.init(job); //add by chen for null bug
			 * this.recordParse = recordParse; //this.recordParse.init(job);
			 */
			writePartition.setRecordParse(this.recordParse);
			writePartition.setStaff(this);
			LOG.info("in loadData if");
			writePartition.write(input, application, staffNum);

			ssrc.setDirFlag(new String[] { "1" });
			ssrc.setCheckNum(this.staffNum);
			lsssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);

			LOG.info("The number of verteices from other staff that"
					+ " cound not be parsed:" + this.lost);
			// LOG.info("in BCBSP with PartitionType is:HASH"
			// + " the number of HeadNode in this partition is:"
			// + graphData.sizeForAll());

			// graphData.finishAdd();
			ssrc.setCheckNum(this.staffNum * 2);
			ssrc.setDirFlag(new String[] { "2" });
			lsssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);

		} else {
			this.partitioner = (Partitioner<Text>) ReflectionUtils.newInstance(
					job.getConf().getClass(
							Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
							HashPartitioner.class), job.getConf());

			WritePartition writePartition = (WritePartition) ReflectionUtils
					.newInstance(
							job.getConf()
									.getClass(
											Constants.USER_BC_BSP_JOB_WRITEPARTITION_CLASS,
											HashWritePartition.class), job
									.getConf());
			int multiple = 1;
			if (writePartition instanceof HashWithBalancerWritePartition) {
				this.partitioner.setNumPartition(this.staffNum * numCopy);
				multiple = 2;
			} else {

				this.partitioner.setNumPartition(this.staffNum);
				multiple = 1;
				if (writePartition instanceof RangeWritePartition) {
					multiple = 2;
				}
			}
			this.partitioner.intialize(job, split);
			/*
			 * RecordParse recordParse = (RecordParse) ReflectionUtils
			 * .newInstance( job.getConf() .getClass(
			 * Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
			 * RecordParseDefault.class), job .getConf());
			 * recordParse.init(job); // this.recordParse = (RecordParse)
			 * ReflectionUtils.newInstance( // job.getConf().getClass( //
			 * Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS, //
			 * RecordParseDefault.class), job.getConf()); //
			 * this.recordParse.init(job); this.recordParse = recordParse;
			 */
			writePartition.setPartitioner(partitioner);
			writePartition.setRecordParse(this.recordParse);
			writePartition.setStaff(this);
			writePartition.setWorkerAgent(aStaffAgent);
			writePartition.setSsrc(ssrc);
			writePartition.setSssc(lsssc);

			writePartition.setTotalCatchSize(job.getConf().getInt(
					Constants.USER_BC_BSP_JOB_TOTALCACHE_SIZE,
					Constants.USER_BC_BSP_JOB_TOTALCACHE_SIZE_DEFAULT));

			int threadNum = job.getConf().getInt(
					Constants.USER_BC_BSP_JOB_SENDTHREADNUMBER,
					Constants.USER_BC_BSP_JOB_SENDTHREADNUMBER_DEFAULT);
			if (threadNum > this.staffNum) {
				threadNum = this.staffNum - 1;
			}
			writePartition.setSendThreadNum(threadNum);
			LOG.info("in loadData else");
			writePartition.write(input, application, staffNum);

			ssrc.setDirFlag(new String[] { "1" });
			ssrc.setCheckNum(this.staffNum * multiple);
			lsssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);

			LOG.info("The number of verteices from other staff that"
					+ " cound not be parsed:" + this.lost);
			ssrc.setCheckNum(this.staffNum * (multiple + 1));
			ssrc.setDirFlag(new String[] { "2" });
			lsssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);
		}

		long end = System.currentTimeMillis();
		LOG.info("in BCBSP with PartitionType is:HASH" + " end time:" + end);
		LOG.info("in BCBSP with PartitionType is:HASH" + " using time:"
				+ (float) (end - start) / 1000 + " seconds");

		return true;
	}

	/**
	 * saveResult: save the local computation result on the HDFS(SequenceFile)
	 * Changed IN 20140313 For Reconstruction..
	 * 
	 * @param job
	 *            BSP Job configuration
	 * @param staff
	 *            the current BSP Staff
	 * @return boolean
	 */
	@SuppressWarnings("unchecked")
	public boolean saveResult(BSPJob job, Staff staff,
			WorkerAgentProtocol workerAgent) {
		try {
			OutputFormat outputformat = (OutputFormat) ReflectionUtils
					.newInstance(
							job.getConf()
									.getClass(
											Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS,
											OutputFormat.class), job.getConf());
			outputformat.initialize(job.getConf());
			RecordWriter output = outputformat.getRecordWriter(job,
					this.getSid());
			// Note Changed 20140313
			this.graphData.saveAllVertices(this, output);
			output.close(job);
			graphData.clean();
		} catch (Exception e) {
			LOG.error("Exception has been catched in BSPStaff--saveResult !", e);
			BSPConfiguration conf = new BSPConfiguration();
			if (this.recoveryTimes < conf.getInt(
					Constants.BC_BSP_JOB_RECOVERY_ATTEMPT_MAX, 0)) {
				recovery(job, staff, workerAgent);
			} else {
				workerAgent.setStaffStatus(
						this.getSid(),
						Constants.SATAFF_STATUS.FAULT,
						new Fault(Fault.Type.DISK, Fault.Level.INDETERMINATE,
								workerAgent.getWorkerManagerName(
										job.getJobID(), this.getSid()), e
										.toString(), job.toString(), this
										.getSid().toString()), 2);
				LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*"
						+ "=*=*=*=*=*=*=*=*=*");
				LOG.error("Other Exception has happened and been catched, "
						+ "the exception will be reported to WorkerManager", e);
			}
		}
		return true;
	}

	/**
	 * saveResult:save the local computation result on the HDFS(SequenceFile)
	 * c++.
	 * 
	 * @param job
	 * @param staff
	 * @throws IOException
	 *             e
	 */
	@SuppressWarnings("unchecked")
	public void saveResultC() throws IOException {
		this.application.getDownlink().saveResult();
	}

	/**
	 * Judge if this Staff should recovery.
	 * 
	 * @return true:the Staff should recovery.
	 */
	public boolean recovery(BSPJob job, Staff staff,
			WorkerAgentProtocol workerAgent) {
		this.recoveryTimes++;
		boolean success = saveResult(job, staff, workerAgent);
		return success == true;
	}

	// Just for testing
	/**
	 * Test the first route.
	 */
	public void displayFirstRoute() {
		for (Entry<Integer, String> e : this
				.getPartitionToWorkerManagerNameAndPort().entrySet()) {
			LOG.info("partitionToWorkerManagerName : " + e.getKey() + " "
					+ e.getValue());
		}
	}

	// Just for testing
	/**
	 * Test the second route.
	 */
	public void displaySecondRoute() {
		for (Entry<Integer, Integer> e : this.hashBucketToPartition.entrySet()) {
			LOG.info("partitionToRange : " + e.getKey() + " " + e.getValue());
		}
	}

	/**
	 * Get the local barrier number.
	 * 
	 * @param hostName
	 *            the current compute node name.
	 * @return local barrier nmuber.
	 */
	public final int getLocalBarrierNumber(final String hostName) {
		int localBarrierNumber = 0;
		for (Entry<Integer, String> entry : this
				.getPartitionToWorkerManagerNameAndPort().entrySet()) {
			String workerManagerName = entry.getValue().split(":")[0];
			if (workerManagerName.equals(hostName)) {
				localBarrierNumber++;
			}
		}
		return localBarrierNumber;
	}

	/**
	 * Delete the old check point.
	 * 
	 * @param oldCheckpoint
	 *            the old check point
	 * @param job
	 *            BSP job configuration
	 * @return true:successfuly delete.
	 */
	private boolean deleteOldCheckpoint(int oldCheckpoint, BSPJob job) {
		LOG.info("deleteOldCheckpoint--oldCheckpoint: " + oldCheckpoint);
		try {
			Configuration conf = new Configuration();
			BSPConfiguration bspConf = new BSPConfiguration();
			String uri = bspConf.get(Constants.BC_BSP_HDFS_NAME)
					+ job.getConf().get(Constants.BC_BSP_CHECKPOINT_WRITEPATH)
					+ "/" + job.getJobID().toString() + "/" + oldCheckpoint
					+ "/";

			// FileSystem fs = FileSystem.get(URI.create(uri), conf);
			BSPFileSystem bspfs = new BSPFileSystemImpl(URI.create(uri), conf);
			// if (fs.exists(new Path(uri))) {
			// fs.delete(new Path(uri), true);
			// }
			if (bspfs.exists(new BSPHdfsImpl().newPath(uri))) {
				bspfs.delete(new BSPHdfsImpl().newPath(uri), true);
			}
		} catch (IOException e) {
			LOG.error("Exception has happened and been catched!", e);
			return false;
		}
		return true;
	}

	/**
	 * Run c++ local compute process for BSP job.
	 * 
	 * @param job
	 *            BSP job configuration
	 * @param workerAgent
	 *            Protocol that staff child process uses to contact its parent
	 *            process
	 * @param recovery
	 *            true:the Staff is a recovery Staff
	 * @param changeWorkerState
	 *            To change worker state
	 * @param failCounter
	 *            fail times
	 * @param hostName
	 *            the current compute node name.
	 */
	public void runC(BSPJob job, Staff staff, WorkerAgentProtocol workerAgent,
			boolean recovery, boolean changeWorkerState, int failCounter,
			String hostName) {

		LOG.info("debug: job type is " + job.getJobType());
		WritePartition writePartition = (WritePartition) ReflectionUtils
				.newInstance(
						job.getConf().getClass(
								Constants.USER_BC_BSP_JOB_WRITEPARTITION_CLASS,
								HashWritePartition.class), job.getConf());
		// record the number of failures of this staff
		LOG.info("BSPStaff---run()--changeWorkerState: " + changeWorkerState);
		LOG.info("BSPStaff---run()--revovery: " + recovery);
		staff.setFailCounter(failCounter);
		LOG.info("[HostName] " + hostName);
		LOG.info("debug:in runC job.exe local path is "
				+ staff.getJobExeLocalPath());

		// 计算用于消息收发的同步时间
		long mssgTime = 0;
		long syncTime = 0;
		// initialize the relative variables
		this.bspJob = job;
		long start = 0, end = 0;

		int superStepCounter = 0;
		this.maxSuperStepNum = job.getNumSuperStep();

		this.staffNum = job.getNumBspStaff();
		SuperStepReportContainer ssrc = new SuperStepReportContainer();
		ssrc.setPartitionId(this.getPartition());
		sssc = new StaffSSController(this.getJobId(), this.getSid(),
				workerAgent);

		Checkpoint cp = new Checkpoint(job);

		try {
			this.application = new Application(job, this, workerAgent, "staff");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("application:" + e.getMessage(), e);
			LOG.error("sjz test:" + e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error("application:" + e.getMessage());
		}

		// if (graphDataFactory == null)
		// graphDataFactory = new GraphDataFactory(job.getConf());

		try {
			if (!recovery) {

				// ***note for putHeadNode Error @author Liu Jinpeng 2013/06/26

				{
					this.recordParse = (RecordParse) ReflectionUtils
							.newInstance(
									job.getConf()
											.getClass(
													Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
													RecordParseDefault.class),
									job.getConf());
					this.recordParse.init(job);

				}
				// if it is a recovery staff
				// schedule Staff Barrier
				LOG.info("this.staffNum is :" + this.staffNum);
				ssrc.setCheckNum(this.staffNum);
				int runc_partitionRPCPort = workerAgent.getFreePort();
				ssrc.setPort1(runc_partitionRPCPort);
				this.activeMQPort = workerAgent.getFreePort();
				ssrc.setPort2(this.activeMQPort);
				LOG.info("[BSPStaff] Get the port for partitioning RPC is : "
						+ runc_partitionRPCPort + "!");
				LOG.info("[BSPStaff] Get the port for ActiveMQ Broker is : "
						+ this.activeMQPort + "!");

				this.partitionToWorkerManagerHostWithPorts = sssc
						.scheduleBarrier(ssrc);

				// record the map from partitions to workermanagers
				for (Integer e : this.partitionToWorkerManagerHostWithPorts
						.keySet()) {
					String[] nameAndPorts = this.partitionToWorkerManagerHostWithPorts
							.get(e).split(":");
					String[] ports = nameAndPorts[1].split("-");
					this.getPartitionToWorkerManagerNameAndPort().put(e,
							nameAndPorts[0] + ":" + ports[1]);
				}

				// For partition and for WorkerManager to invoke rpc method of
				// Staff.
				this.staffAgent = new WorkerAgentForStaff(job.getConf());
				workerAgent.setStaffAgentAddress(this.getSid(),
						this.staffAgent.address());

				// initialize the number of local staffs and the number of
				// workers of the same job
				this.localBarrierNum = getLocalBarrierNumber(hostName);
				this.workerMangerNum = workerAgent.getNumberWorkers(
						this.getJobId(), this.getSid());
				displayFirstRoute();

				// load Data for the staff
				/**
				 * Review comment: there are too many if else structure which
				 * may lead to a obstacle against extension and reusing Review
				 * time: 2011-11-30 Reviewer: HongXu Zhang Fix log: we use the
				 * factory pattern to implement the creation of a graph data
				 * object Fix time: 2011-12-2 Programmer: Hu Zheng
				 */

				/** Clock */
				start = System.currentTimeMillis();

				// just for test
				// loadData(job);
				// this.application.getDownlink().sendKeyValue("aaaaaaaa",
				// "bbbbbbbbbb");
				LOG.info("before loadData");
				if (this.application == null) {
					LOG.info("sjz test: this.application is null!");
				}
				loadData(job, workerAgent, this.staffAgent, this.application);
				LOG.info("after loadData");
				// 告知c++端java端图数据已发送完毕
				this.application.getDownlink().endOfInput();
				LOG.info("after endOfInput");
				if (this.application == null) {
					LOG.error("sjz: the application is null!");
				}
				end = System.currentTimeMillis();
				LOG.info("[==>Clock<==] <load Data> used " + (end - start)
						/ 1000f + " seconds");

			}

		} catch (ClassNotFoundException cnfE) {
			LOG.error("Exception has been catched in BSPStaff--run--before"
					+ " local computing!", cnfE);
			workerAgent.setStaffStatus(
					staff.getStaffAttemptId(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL,
							workerAgent.getWorkerManagerName(job.getJobID(),
									staff.getStaffAttemptId()),
							cnfE.toString(), job.toString(), staff
									.getStaffAttemptId().toString()), 0);
			return;
		} catch (IOException ioE) {
			LOG.error("Exception has been catched in BSPStaff--run--before"
					+ " local computing !", ioE);
			workerAgent.setStaffStatus(
					staff.getStaffAttemptId(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.DISK, Fault.Level.INDETERMINATE,
							workerAgent.getWorkerManagerName(job.getJobID(),
									staff.getStaffAttemptId()), ioE.toString(),
							job.toString(), staff.getStaffAttemptId()
									.toString()), 0);
			return;
		} catch (InterruptedException iE) {
			LOG.error("Exception has been catched in BSPStaff--run--before"
					+ " local computing !", iE);
			workerAgent.setStaffStatus(
					staff.getStaffAttemptId(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL,
							workerAgent.getWorkerManagerName(job.getJobID(),
									staff.getStaffAttemptId()), iE.toString(),
							job.toString(), staff.getStaffAttemptId()
									.toString()), 0);
			return;
		}
		String commOption = job.getCommucationOption();
		// String commOption = Constants.ACTIVEMQ_VERSION;

		// ========================== ===========================
		try {

			/** Clock */
			start = System.currentTimeMillis();

			// Start an ActiveMQ Broker, create a communicator, initialize it,
			// and start it
			if (commOption.endsWith(Constants.ACTIVEMQ_VERSION)) {
				startActiveMQBroker(hostName);
				this.communicator = new Communicator(this.getJobId(), job,
						this.getPartition(), partitioner);
			}
			// 构造RPC communicator 而且启动RPC Server
			else {
				this.communicator = new RPCCommunicator(this.getJobId(), job,
						this.getPartition(), partitioner);

			}
			this.communicator.start(hostName, this);
			// this.communicator.initialize(this.getHashBucketToPartition(),
			// this.getPartitionToWorkerManagerNameAndPort(),
			// this.graphData);
			this.routerparameter.setPartitioner(partitioner);
			if (writePartition instanceof HashWithBalancerWritePartition) {
				this.routerparameter.setHashBucketToPartition(this
						.getHashBucketToPartition());

			} else {
				if (writePartition instanceof RangeWritePartition) {
					this.routerparameter.setRangeRouter(this.getRangeRouter());
				}
			}
			// 100 is just for test , and in fact this number need get from c++
			// after loaded Data
			this.communicator.initialize(this.routerparameter,
					this.getPartitionToWorkerManagerNameAndPort(), 100);

			this.communicator.start();

			end = System.currentTimeMillis();
			LOG.info("[==>Clock<==] <Initialize Communicator> used "
					+ (end - start) / 1000f + " seconds");

			this.counters = new Counters();
			ArrayList<String> aggregateValue = null;
			this.communicator.setStaffId(staff.getStaffID().toString());
			this.application.setCommunicator(this.communicator);
			// begin local computation
			while (this.flag) { // 本地计算循环开始
				this.activeCounter = 0;
				if (!recovery) {
					superStepCounter = this.currentSuperStepCounter;
				} else {
					superStepCounter = 0;
					recovery = false;
				}

				this.communicator.begin(superStepCounter);

				// this.application.getDownlink().start();
				this.application.getDownlink().runASuperStep();
				try {
					LOG.info("wait for superstep "
							+ this.currentSuperStepCounter + " to finish");
					this.application.waitForFinish();
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					LOG.error("the current superStep failed " + e.getMessage());
				}
				aggregateValue = this.application.getHandler()
						.getAggregateValue();
				Iterator<String> it = aggregateValue.iterator();
				while (it.hasNext()) {
					LOG.info("aggregatevalue is : " + it.next());
				}
				/** Clock */
				start = System.currentTimeMillis();
				LOG.info("BSPStaff--run: superStepCounter: " + superStepCounter);

				long computeTime = 0;

				LOG.info("[==>Clock<==] ...(Compute Time) used " + computeTime
						/ 1000f + " seconds");
				/** Clocks */

				/** Clock */
				start = System.currentTimeMillis();
				// Notify the communicator that there will be no more messages
				// for sending.
				this.communicator.noMoreMessagesForSending();
				// Wait for all of the messages have been sent
				// over.这里发送线程及其发送实例线程在迭代循环过程中一直存活
				while (true) {
					if (this.communicator.isSendingOver()) {
						break;
					}
				}
				end = System.currentTimeMillis();
				// ========================
				mssgTime = mssgTime + end - start;
				LOG.info("[==>Clock<==] <Wait for sending over> used "
						+ (end - start) / 1000f + " seconds");
				/** Clock */
				LOG.info("===========Sending Over============");

				/** Clock */
				start = end;
				// Barrier for sending messages over.
				ssrc.setLocalBarrierNum(this.localBarrierNum);
				ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);//
				ssrc.setDirFlag(new String[] { "1" });
				ssrc.setCheckNum(this.workerMangerNum);
				sssc.firstStageSuperStepBarrier(this.currentSuperStepCounter,
						ssrc);
				LOG.info("debug:incomingQueueSize is "
						+ this.communicator.getIncomingQueuesSize());
				end = System.currentTimeMillis();
				// sync time for sending msg
				syncTime = end - start;

				/** Clock */
				LOG.info("[==>Clock<==] <Sending over sync> used "
						+ (end - start) / 1000f + " seconds");

				/** Clock */
				start = end;
				// Notify the communicator that there will be no more
				// messages接收线程是否完成或者

				while (true) {
					if (this.communicator.isReceivingOver()) {
						break;
					}
				}
				this.communicator.noMoreMessagesForReceiving();
				end = System.currentTimeMillis();
				mssgTime = mssgTime + end - start;
				/** Clock */
				LOG.info("[==>Clock<==] <Wait for receiving over> used "
						+ (end - start) / 1000f + " seconds");
				LOG.info("===========Receiving Over===========");

				/** Clock */
				start = end;
				// Barrier for receiving messages over.
				ssrc.setLocalBarrierNum(this.localBarrierNum);
				ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
				ssrc.setDirFlag(new String[] { "2" });
				ssrc.setCheckNum(this.workerMangerNum * 2);
				sssc.firstStageSuperStepBarrier(this.currentSuperStepCounter,
						ssrc);
				end = System.currentTimeMillis();
				syncTime = end - start;
				/** Clock */
				LOG.info("[==>Clock<==] <Receiving over sync> used "
						+ (end - start) / 1000f + " seconds");

				// this.graphData.showMemoryInfo();
				// add by chen
				this.counters.findCounter(BspCounters.MESSAGES_NUM_SENT)
						.increment(
								this.communicator
										.getCombineOutgoMessageCounter());
				this.counters.findCounter(BspCounters.MESSAGES_NUM_RECEIVED)
						.increment(
								this.communicator.getReceivedMessageCounter());
				this.counters.findCounter(BspCounters.MESSAGE_BYTES_SENT)
						.increment(
								this.communicator
										.getCombineOutgoMessageBytesCounter());
				this.counters.findCounter(BspCounters.MESSAGE_BYTES_RECEIVED)
						.increment(
								this.communicator
										.getReceivedMessageBytesCounter());
				//add by lvs
				//count the vertexNum and edgeNum of the GraphData
				if(this.currentSuperStepCounter == 0){
					this.counters.findCounter(BspCounters.VERTEXES_NUM_OF_GRAPHDATA)
					.increment(MetaDataOfGraph.BCBSP_GRAPH_VERTEXNUM);
					this.counters.findCounter(BspCounters.EDGES_NUM_OF_GRAPHDATA)
					.increment(MetaDataOfGraph.BCBSP_GRAPH_EDGENUM);
				}

				// this.counters.findCounter(BspCounters.MESSAGES_NUM_SENT)
				// .increment(this.communicator.getSendMessageCounter());
				// this.counters.findCounter(BspCounters.MESSAGES_NUM_RECEIVED)
				// .increment(this.communicator.getIncomingQueuesSize());
				// this.counters.findCounter(BspCounters.MESSAGE_BYTES_SENT)
				// .increment(this.communicator.getCombineOutgoMessageBytesCounter());
				// this.counters.findCounter(BspCounters.MESSAGE_BYTES_RECEIVED)
				// .increment(this.communicator.getReceivedMessageBytesCounter());

				// LOG.info("****************************************************");
				// this.counters.log(LOG);
				// Exchange the incoming and incomed queues.
				// LOG.info("debug:before exchange incomingQueueSize is " +
				// this.communicator.getIncomingQueuesSize());
				this.communicator.exchangeIncomeQueues();

				LOG.info("[BSPStaff] Communicator has received "
						+ this.communicator.getIncomedQueuesSize()
						+ " messages totally for the super step <"
						+ this.currentSuperStepCounter + ">");

				// decide whether to continue the next super-step or not
				if ((this.currentSuperStepCounter + 1) >= this.maxSuperStepNum) {
					this.communicator.clearOutgoingQueues();
					this.communicator.clearIncomedQueues();

					// just for test
					this.activeCounter = 0;
				} else {
					this.activeCounter = 1;
				}

				/** Clock */
				start = System.currentTimeMillis();
				// Encapsulate the aggregate values into String[].
				String[] aggValues = aggregateValue.toArray(new String[0]);
				this.application.getHandler().getAggregateValue().clear();
				end = System.currentTimeMillis();
				LOG.info("[==>Clock<==] <Encapsulate aggregate values> used "
						+ (end - start) / 1000f + " seconds");
				/** Clock */

				/** Clock */
				start = end;
				// Set the aggregate values into the super step report
				// container.
				ssrc.setAggValues(aggValues);

				ssrc.setLocalBarrierNum(this.localBarrierNum);
				ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.SECOND_STAGE);
				// to here
				LOG.info("[WorkerManagerNum]" + this.workerMangerNum);
				ssrc.setCheckNum(this.workerMangerNum + 1);
				ssrc.setJudgeFlag(this.activeCounter
						+ this.communicator.getIncomedQueuesSize());

				// add by chen
				sssc.setCounters(this.counters);
				this.counters.clearCounters();
				// this.counters.log(LOG);
				this.ssc = sssc.secondStageSuperStepBarrier(
						this.currentSuperStepCounter, ssrc);

				LOG.info("[==>Clock<==] <StaffSSController's rebuild session> used "
						+ StaffSSController.rebuildTime / 1000f + " seconds");

				StaffSSController.rebuildTime = 0;
				// if (ssc.getCommandType() ==
				// Constants.COMMAND_TYPE.START_AND_RECOVERY) {
				// LOG.info("[Command]--[routeTableSize]"
				// + ssc.getPartitionToWorkerManagerNameAndPort()
				// .size());
				// this.setPartitionToWorkerManagerNameAndPort(ssc
				// .getPartitionToWorkerManagerNameAndPort());
				// ArrayList<String> tmp = new ArrayList<String>();
				// for (String str : this.partitionToWorkerManagerNameAndPort
				// .values()) {
				// if (!tmp.contains(str)) {
				// tmp.add(str);
				// }
				// }
				// this.localBarrierNum = getLocalBarrierNumber(hostName);
				// workerAgent.setNumberWorkers(this.jobId, this.sid,
				// tmp.size());
				// tmp.clear();
				// this.workerMangerNum = workerAgent.getNumberWorkers(
				// this.jobId, this.sid);
				//
				// displayFirstRoute();
				// }
				end = System.currentTimeMillis();
				/** Clock */
				LOG.info("[==>Clock<==] <SuperStep sync> used " + (end - start)
						/ 1000f + " seconds");

				/** Clock */
				start = end;
				// Get the aggregate values from the super step command.
				// Decapsulate the aggregate values from String[].
				// 改： aggValues = this.ssc.getAggValues();
				// // if (aggValues != null) {
				// // decapsulateAggregateValues(aggValues);
				// // }

				// this.application.getDownlink()
				// .sendNewAggregateValue(this.ssc.getAggValues());
				// this.application.getDownlink().endOfInput();

				aggValues = this.ssc.getAggValues();
				this.application.getDownlink().sendNewAggregateValue(aggValues);
				this.application.getDownlink().endOfInput();
				end = System.currentTimeMillis();
				/** Clock */
				LOG.info("[==>Clock<==] <Decapsulate aggregate values> used "
						+ (end - start) / 1000f + " seconds");

				/** Clock */
				start = end;
				switch (ssc.getCommandType()) {
				case Constants.COMMAND_TYPE.START:
					LOG.info("Get the CommandType is : START");
					this.currentSuperStepCounter = ssc.getNextSuperStepNum();
					this.flag = true;
					break;
				case Constants.COMMAND_TYPE.START_AND_CHECKPOINT:
					LOG.info("Get the CommandTye is : START_AND_CHECKPOINT");
					boolean success = cp.writeCheckPoint(this.graphData,
							new BSPHdfsImpl().newPath(ssc.getInitWritePath()),
							job, staff);
					if (success) {
						deleteOldCheckpoint(ssc.getOldCheckPoint(), job);
					}
					ssrc.setLocalBarrierNum(this.localBarrierNum);
					ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.WRITE_CHECKPOINT_SATGE);
					ssrc.setDirFlag(new String[] { "write" });
					ssrc.setCheckNum(this.workerMangerNum * 3);
					sssc.checkPointStageSuperStepBarrier(
							this.currentSuperStepCounter, ssrc);

					this.currentSuperStepCounter = ssc.getNextSuperStepNum();
					this.flag = true;
					break;
				case Constants.COMMAND_TYPE.START_AND_RECOVERY:
					LOG.info("Get the CommandTye is : START_AND_RECOVERY");
					this.currentSuperStepCounter = ssc.getAbleCheckPoint();

					// clean first
					int version = job.getGraphDataVersion();
					this.graphData = this.getGraphDataFactory()
							.createGraphData(version, this);
					this.graphData.clean();
					this.graphData = cp.readCheckPoint(
							new BSPHdfsImpl().newPath(ssc.getInitReadPath()),
							job, staff);

					ssrc.setPartitionId(this.getPartition());
					ssrc.setLocalBarrierNum(this.localBarrierNum);
					ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE);
					ssrc.setDirFlag(new String[] { "read" });
					ssrc.setCheckNum(this.workerMangerNum * 1);

					ssrc.setPort2(this.activeMQPort);
					LOG.info("[BSPStaff] Get the port for ActiveMQ Broker is : "
							+ this.activeMQPort + "!");
					this.setPartitionToWorkerManagerNameAndPort(sssc
							.checkPointStageSuperStepBarrier(
									this.currentSuperStepCounter, ssrc));
					displayFirstRoute();
					this.communicator.setPartitionToWorkerManagerNamePort(this
							.getPartitionToWorkerManagerNameAndPort());

					this.currentSuperStepCounter = ssc.getNextSuperStepNum();

					this.communicator.clearOutgoingQueues();
					this.communicator.clearIncomedQueues();

					recovery = true;
					this.flag = true;
					break;
				case Constants.COMMAND_TYPE.STOP:
					LOG.info("songjianze0");
					LOG.info("Get the CommandTye is : STOP");
					LOG.info("Staff will save the computation result and then quit!");
					this.currentSuperStepCounter = ssc.getNextSuperStepNum();
					this.flag = false;
					break;
				default:
					LOG.error("ERROR! "
							+ ssc.getCommandType()
							+ " is not a valid CommandType, so the staff will save the "
							+ "computation result and quit!");
					flag = false;
				}
				// Report the status at every superstep.
				workerAgent.setStaffStatus(this.getSid(),
						Constants.SATAFF_STATUS.RUNNING, null, 1);
			} // 本地计算循环结束
			LOG.info("songjianze1");
			this.communicator.complete();
			LOG.info("songjianze2");

		} catch (IOException ioe) { // try 结束
			LOG.error("Exception has been catched in BSPStaff--run--during"
					+ " local computing !", ioe);
			workerAgent.setStaffStatus(
					this.getSid(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.DISK, Fault.Level.CRITICAL,
							workerAgent.getWorkerManagerName(job.getJobID(),
									this.getSid()), ioe.toString(), job
									.toString(), this.getSid().toString()), 1);
			LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*="
					+ "*=*=*=*=*=*=*=*=*=*");
			LOG.error("IO Exception has happened and been catched, "
					+ "the exception will be reported to WorkerManager", ioe);
			LOG.error("Staff will quit abnormally");
			return;
		} catch (Exception e) {
			LOG.error("Exception has been catched in BSPStaff--run--during"
					+ " local computing !", e);
			workerAgent.setStaffStatus(
					this.getSid(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.SYSTEMSERVICE,
							Fault.Level.INDETERMINATE, workerAgent
									.getWorkerManagerName(job.getJobID(),
											this.getSid()), e.toString(), job
									.toString(), this.getSid().toString()), 1);
			LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*"
					+ "=*=*=*=*=*=*");
			LOG.error("Other Exception has happened and been catched, "
					+ "the exception will be reported to WorkerManager", e);
			LOG.error("Staff will quit abnormally");
			return;
		}

		// save the computation result
		try {
			// tell c++ local compute is over
			saveResultC();
			// this.application.getDownlink().close();
			ssrc.setLocalBarrierNum(this.localBarrierNum);
			ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.SAVE_RESULT_STAGE);
			ssrc.setDirFlag(new String[] { "1", "2", "write", "read" });
			sssc.saveResultStageSuperStepBarrier(this.currentSuperStepCounter,
					ssrc);
			// cleanup after local computation
			// bsp.cleanup(staff);
			stopActiveMQBroker();
			done(workerAgent);
			workerAgent.setStaffStatus(this.getSid(),
					Constants.SATAFF_STATUS.SUCCEED, null, 1);
			LOG.info("The max SuperStep num is " + this.maxSuperStepNum);
			LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*="
					+ "*=*=*=*=*=*=*=*=*");
			LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*:the send and receive"
					+ " time accumulation is  " + mssgTime / 1000f);
			LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*:the synchronization time"
					+ " accumulation is  " + syncTime / 1000f);
			LOG.info("Staff is completed successfully");
		} catch (Exception e) {
			LOG.error("Exception has been catched in BSPStaff--run--afte"
					+ "r local computing !", e);
			workerAgent.setStaffStatus(
					this.getSid(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.SYSTEMSERVICE,
							Fault.Level.INDETERMINATE, workerAgent
									.getWorkerManagerName(job.getJobID(),
											this.getSid()), e.toString(), job
									.toString(), this.getSid().toString()), 2);
			LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*="
					+ "*=*=*=*");
			LOG.error("Other Exception has happened and been catched, "
					+ "the exception will be reported to WorkerManager", e);
		}

	}

	@SuppressWarnings({ "unchecked", "unused" })
	@Override
	/**
	 * run the local computation.
	 * @param job BSP job configuration
	 * @param staff the current Staff
	 * @param workerAgent Protocol that staff child process uses
	 *  to contact its parent process
	 * @param recovery true:the Staff is a recovery Staff
	 * @param changeWorkerState To change worker state
	 * @param failCounter fail times
	 * @param hostName the current compute node name.
	 * Review comment:
	 *      (1) The codes inside this method are too messy.
	 * Review time: 2011-11-30
	 * Reviewer: Hongxu Zhang.
	 * Fix log:
	 *      (1) To make the codes neat and well-organized, I use more empty
	 *       lines and annotations
	 *      to organize the codes.
	 * Fix time:    2011-12-1
	 * Programmer: Hu Zheng.
	 */
	/*
	 * Review suggestion: allow user to determine whether to use load balance
	 * Zhicheng Liu 2013/10/9
	 */
	public void run(BSPJob job, Staff staff, WorkerAgentProtocol workerAgent,
			boolean recovery, boolean changeWorkerState, int migrateSuperStep,
			int failCounter, String hostName) {
		
		//Biyahui added
		this.recoveryFlag=recovery;
		WritePartition writePartition = (WritePartition) ReflectionUtils
				.newInstance(
						job.getConf().getClass(
								Constants.USER_BC_BSP_JOB_WRITEPARTITION_CLASS,
								HashWritePartition.class), job.getConf());

		// record the number of failures of this staff
		LOG.info("BSPStaff---run()--changeWorkerState: " + changeWorkerState);
		staff.setFailCounter(failCounter);
		LOG.info("[HostName] " + hostName);

		// Note Memory Deploy 20140312
		MemoryAllocator ma = new MemoryAllocator(job);
		ma.PrintMemoryInfo(LOG);
		ma.setupBeforeLoadGraph(LOG);

		// 计算用于消息收发的同步时间
		long mssgTime = 0;
		long syncTime = 0;
		// initialize the relative variables
		this.bspJob = job;
		long start = 0, end = 0;

		int superStepCounter = 0;
		this.maxSuperStepNum = job.getNumSuperStep();

		this.staffNum = job.getNumBspStaff();
		SuperStepReportContainer ssrc = new SuperStepReportContainer();
		ssrc.setPartitionId(this.getPartition());
		sssc = new StaffSSController(this.getJobId(), this.getSid(),
				workerAgent);

		Checkpoint cp = new Checkpoint(job);
		/* Feng added */
		 assistCheckpoint asscp = (assistCheckpoint) ReflectionUtils
				  .newInstance( job.getConf() .getClass(
				  Constants.DEFAULT_BC_BSP_JOB_CHECKPOINT_USER_DEFINE,
				  assistCheckpointDefault.class), job.getConf());
				  asscp.init(job);

		/* Zhicheng Liu added */
		this.openMigrateMode = this.bspJob.getConf()
				.get("bcbsp.loadbalance", "false").equals("true") ? true
				: false;
		/* Feng added */
		this.aggCpFlag = this.bspJob.getConf()
				.get("bcbsp.assValuesCheckpoint", "false").equals("true") ? true
				: false;
		if (this.getGraphDataFactory() == null) {
			this.setGraphDataFactory(new GraphDataFactory(job.getConf()));
		}
		try {

			/* Zhicheng Liu added */
			if (openMigrateMode && migrateSuperStep != 0) { // Whether the staff
				// is a migrate
				// staff
				boolean staffMigrateFlag = true;// flag to judge a staff migrate
												// to set the init vertex path
				LOG.info("Migrate new staff " + this.getSid());

				this.currentSuperStepCounter = migrateSuperStep - 1;

				// Get superstep command for recovery
				this.ssc = sssc
						.secondStageSuperStepBarrierForRecovery(this.currentSuperStepCounter);

				// Set the port for communication
				this.setPartitionToWorkerManagerNameAndPort(ssc
						.getPartitionToWorkerManagerNameAndPort());
				this.localBarrierNum = getLocalBarrierNumber(hostName);

				ArrayList<String> tmp = new ArrayList<String>();

				for (String str : this.getPartitionToWorkerManagerNameAndPort()
						.values()) {
					if (!tmp.contains(str)) {
						tmp.add(str);
					}
				}

				// Update the worker information
				workerAgent.setNumberWorkers(this.getJobId(), this.getSid(),
						tmp.size());
				tmp.clear();
				this.workerMangerNum = workerAgent.getNumberWorkers(
						this.getJobId(), this.getSid());

				// Clean first
				int version = job.getGraphDataVersion();
				this.graphData = this.getGraphDataFactory().createGraphData(
						version, this);
				this.graphData.clean();
				// this.graphData.setMigratedStaffFlag(true);

				BSPConfiguration bspConf = new BSPConfiguration();
				String uri = bspConf.get(Constants.BC_BSP_HDFS_NAME)
						+ this.getJobId() + "/" + this.getSid()
						+ "/migrate/graph.cp";

				long tmpTS = System.currentTimeMillis();

				this.graphData = cp.readCheckPoint(
						new BSPHdfsImpl().newPath(uri), job, staff);
				// read graph data for recovery

				long tmpTE = System.currentTimeMillis();
				this.rwCheckPointT = (tmpTE - tmpTS) * 2;

				if (graphData.getVertexSize() == 0) {
					vertexSize = 10;
				}
				this.graphBytes = graphData.sizeForAll() * vertexSize;

				LOG.info("readGraph from checkpoint: this.graphBytes is "
						+ this.graphBytes);

				// Delete from hdfs
				Configuration conf = new Configuration();
				// FileSystem fs = FileSystem.get(URI.create(uri), conf);
				BSPFileSystem bspfs = new BSPFileSystemImpl(URI.create(uri),
						conf);
				// if (fs.exists(new Path(uri))) {
				// fs.delete(new Path(uri), true);
				// LOG.info("Has deleted the checkpoint of graphData on hdfs");
				// }
				if (bspfs.exists(new BSPHdfsImpl().newPath(uri))) {
					bspfs.delete(new BSPHdfsImpl().newPath(uri), true);
					LOG.info("Has deleted the checkpoint of graphData on hdfs");
				}

				this.currentSuperStepCounter = ssc.getNextSuperStepNum();

				LOG.info("Now, this super step count is "
						+ this.currentSuperStepCounter);

				this.partitioner = (Partitioner<Text>) ReflectionUtils
						// create partitioner
						.newInstance(
								job.getConf()
										.getClass(
												Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
												HashPartitioner.class), job
										.getConf());

				if (writePartition instanceof HashWithBalancerWritePartition) {
					this.partitioner.setNumPartition(this.staffNum * numCopy);
				} else {
					this.partitioner.setNumPartition(this.staffNum);
				}
				org.apache.hadoop.mapreduce.InputSplit split = null;
				if (rawSplitClass.equals("no")) {

				} else {

					DataInputBuffer splitBuffer = new DataInputBuffer();
					splitBuffer.reset(rawSplit.getBytes(), 0,
							rawSplit.getLength());
					SerializationFactory factory = new SerializationFactory(
							job.getConf());
					Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deserializer = (Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>) factory
							.getDeserializer(job.getConf().getClassByName(
									rawSplitClass));
					deserializer.open(splitBuffer);
					split = deserializer.deserialize(null);
				}
				this.partitioner.intialize(job, split);
				displayFirstRoute();

			} else if (!recovery) {
				// if it is a recovery staff
				// schedule Staff Barrier
				ssrc.setCheckNum(this.staffNum);
				int runpartitionRPCPort = workerAgent.getFreePort();

				/* Zhicheng Liu added */
				this.partitionRPCPort = runpartitionRPCPort;

				ssrc.setPort1(runpartitionRPCPort);
				this.activeMQPort = workerAgent.getFreePort();
				ssrc.setPort2(this.activeMQPort);
				LOG.info("[BSPStaff] Get the port for partitioning RPC is : "
						+ runpartitionRPCPort + "!");
				LOG.info("[BSPStaff] Get the port for ActiveMQ Broker is : "
						+ this.activeMQPort + "!");

				// ***note for putHeadNode Error @author Liu Jinpeng 2013/06/26
				{
					this.recordParse = (RecordParse) ReflectionUtils
							.newInstance(
									job.getConf()
											.getClass(
													Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
													RecordParseDefault.class),
									job.getConf());
					this.recordParse.init(job);

					int version = job.getGraphDataVersion();
					this.graphData = this.getGraphDataFactory()
							.createGraphData(version, this);
				}

				this.partitionToWorkerManagerHostWithPorts = sssc
						.scheduleBarrier(ssrc);

				// record the map from partitions to workermanagers
				for (Integer e : this.partitionToWorkerManagerHostWithPorts
						.keySet()) {
					String[] nameAndPorts = this.partitionToWorkerManagerHostWithPorts
							.get(e).split(":");
					String[] ports = nameAndPorts[1].split("-");
					this.getPartitionToWorkerManagerNameAndPort().put(e,
							nameAndPorts[0] + ":" + ports[1]);
				}

				// For partition and for WorkerManager to invoke rpc method of
				// Staff.
				this.staffAgent = new WorkerAgentForStaff(job.getConf());
				workerAgent.setStaffAgentAddress(this.getSid(),
						this.staffAgent.address());

				// initialize the number of local staffs and the number of
				// workers of the same job
				this.localBarrierNum = getLocalBarrierNumber(hostName);
				this.workerMangerNum = workerAgent.getNumberWorkers(
						this.getJobId(), this.getSid());
				displayFirstRoute();

				// load Data for the staff
				/**
				 * Review comment: there are too many if else structure which
				 * may lead to a obstacle against extension and reusing Review
				 * time: 2011-11-30 Reviewer: HongXu Zhang Fix log: we use the
				 * factory pattern to implement the creation of a graph data
				 * object Fix time: 2011-12-2 Programmer: Hu Zheng
				 */

				/** Clock */
				start = System.currentTimeMillis();

				loadData(job, workerAgent, this.staffAgent);
				end = System.currentTimeMillis();
				LOG.info("[==>Clock<==] <load Data> used " + (end - start)
						/ 1000f + " seconds");

				/* Zhicheng Liu added */
				if (this.openMigrateMode) {
					this.loadDataT = (end - start) * 2;
					if (graphData.getVertexSize() == 0) {
						vertexSize = 8;
					}
					this.graphBytes = graphData.sizeForAll() * vertexSize;
				}

			} else {
				LOG.info("The recoveried staff begins to read checkpoint");
				LOG.info("The fault SuperStepCounter is : "
						+ job.getInt("staff.fault.superstep", 0));

				// schedule a barrier
				this.ssc = sssc.secondStageSuperStepBarrierForRecovery(job
						.getInt("staff.fault.superstep", 0));

				this.setPartitionToWorkerManagerNameAndPort(ssc
						.getPartitionToWorkerManagerNameAndPort());
				this.localBarrierNum = getLocalBarrierNumber(hostName);
				ArrayList<String> tmp = new ArrayList<String>();
				for (String str : this.getPartitionToWorkerManagerNameAndPort()
						.values()) {
					if (!tmp.contains(str)) {
						tmp.add(str);
					}
				}
				workerAgent.setNumberWorkers(this.getJobId(), this.getSid(),
						tmp.size());
				tmp.clear();
				this.workerMangerNum = workerAgent.getNumberWorkers(
						this.getJobId(), this.getSid());
				this.currentSuperStepCounter = ssc.getAbleCheckPoint();

				// clean first
				int version = job.getGraphDataVersion();
				this.graphData = this.getGraphDataFactory().createGraphData(
						version, this);
				this.graphData.clean();

				/* Zhicheng Liu added */
				long tmpTS = System.currentTimeMillis();
				if(conf==null){
					LOG.info("conf is null! "+conf);
					conf = job.getConf();
				}
				this.setCheckPointFrequency();
				String oripathTest1 = conf.get(Constants.BC_BSP_CHECKPOINT_WRITEPATH) +
				          "/" +
				          this.getJobId() + "/" + this.checkPointFrequency;
				/*Biyahui revised*/
				if(this.checkPointFrequency==ssc.getAbleCheckPoint()){
					this.graphData = cp.readCheckPoint(new BSPHdfsImpl().newPath(oripathTest1),job,staff);
				}else{
					this.graphData = cp.readIncCheckPoint(new BSPHdfsImpl().newPath(oripathTest1),
							new BSPHdfsImpl().newPath(ssc.getInitReadPath()), job,
							staff);
				}
				/* Zhicheng Liu added */
				long tmpTE = System.currentTimeMillis();
				this.rwCheckPointT = (tmpTE - tmpTS) * 2;

				ssrc.setLocalBarrierNum(this.localBarrierNum);
				ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE);
				ssrc.setDirFlag(new String[] { "read" });
				ssrc.setCheckNum(this.workerMangerNum * 1);

				// Get the new port of ActiveMQ.
				this.activeMQPort = workerAgent.getFreePort();
				ssrc.setPort2(this.activeMQPort);
				LOG.info("[BSPStaff] ReGet the port for ActiveMQ Broker is : "
						+ this.activeMQPort + "!");

				this.setPartitionToWorkerManagerNameAndPort(sssc
						.checkPointStageSuperStepBarrier(
								this.currentSuperStepCounter, ssrc));
				displayFirstRoute();
				this.currentSuperStepCounter = ssc.getNextSuperStepNum();

				this.partitioner = (Partitioner<Text>) ReflectionUtils
						.newInstance(
								job.getConf()
										.getClass(
												Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
												HashPartitioner.class), job
										.getConf());

				if (writePartition instanceof HashWithBalancerWritePartition) {
					this.partitioner.setNumPartition(this.staffNum * numCopy);
				} else {
					this.partitioner.setNumPartition(this.staffNum);
				}
				org.apache.hadoop.mapreduce.InputSplit split = null;
				if (rawSplitClass.equals("no")) {

				} else {

					DataInputBuffer splitBuffer = new DataInputBuffer();
					splitBuffer.reset(rawSplit.getBytes(), 0,
							rawSplit.getLength());
					SerializationFactory factory = new SerializationFactory(
							job.getConf());
					Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deserializer = (Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>) factory
							.getDeserializer(job.getConf().getClassByName(
									rawSplitClass));
					deserializer.open(splitBuffer);
					split = deserializer.deserialize(null);
				}
				this.partitioner.intialize(job, split);
				displayFirstRoute();
			}
		} catch (ClassNotFoundException cnfE) {
			LOG.error("Exception has been catched in BSPStaff--run--before"
					+ " local computing !", cnfE);
			workerAgent.setStaffStatus(
					staff.getStaffAttemptId(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL,
							workerAgent.getWorkerManagerName(job.getJobID(),
									staff.getStaffAttemptId()),
							cnfE.toString(), job.getJobID().toString(), staff
									.getStaffAttemptId().toString()), 0);
			return;
		} catch (IOException ioE) {
			LOG.error("Exception has been catched in BSPStaff--run--before"
					+ " local computing !", ioE);
			workerAgent.setStaffStatus(
					staff.getStaffAttemptId(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.DISK, Fault.Level.INDETERMINATE,
							workerAgent.getWorkerManagerName(job.getJobID(),
									staff.getStaffAttemptId()), ioE.toString(),
							job.getJobID().toString(), staff
									.getStaffAttemptId().toString()), 0);
			return;
		} catch (InterruptedException iE) {
			LOG.error("Exception has been catched in BSPStaff--run--before"
					+ " local computing !", iE);
			workerAgent.setStaffStatus(
					staff.getStaffAttemptId(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL,
							workerAgent.getWorkerManagerName(job.getJobID(),
									staff.getStaffAttemptId()), iE.toString(),
							job.getJobID().toString(), staff
									.getStaffAttemptId().toString()), 0);
			return;
		}

		BSP bsp = (BSP) ReflectionUtils.newInstance(
				job.getConf().getClass(Constants.USER_BC_BSP_JOB_WORK_CLASS,
						BSP.class), job.getConf());

		/** Clock */
		start = System.currentTimeMillis();
		// load aggregators and aggregate values.
		loadAggregators(job);
		end = System.currentTimeMillis();
		LOG.info("[==>Clock<==] <loadAggregators> used " + (end - start)
				/ 1000f + " seconds");

		/* Zhicheng Liu added */
		if (openMigrateMode && migrateSuperStep != 0) {
			String[] aggValues = this.ssc.getAggValues();
			if (aggValues != null) {
				decapsulateAggregateValues(aggValues);
			}
		}

		// ========================== add for RPC communication integration
		// ==========================
		String commOption = job.getCommucationOption();

		// ========================== ===========================
		try {
			// configuration before local computation
			bsp.setup(staff);

			/* Zhicheng Liu added */
			// Read incoming message checkpoint
			Map<String, LinkedList<IMessage>> icomMess = null;
			//Biyahui added
			//Map<String, LinkedList<IMessage>> icomRecoveryMess = null;
			if (openMigrateMode && migrateSuperStep != 0) { // read message from
				// hdfs for recovery
				LOG.info("read message checkpoint from hdfs for migrate");
				BSPConfiguration bspConf = new BSPConfiguration();
				String uri = bspConf.get(Constants.BC_BSP_HDFS_NAME)
						+ this.getJobId() + "/" + this.getSid()
						+ "/migrate/message.cp";

				icomMess = cp.readMessages(new BSPHdfsImpl().newPath(uri), job,
						staff);// read migrate staff incomed messages
				LOG.info("Migrate messages size! " + icomMess.size());
				// Delete from hdfs
				Configuration conf = new Configuration();
				// FileSystem fs = FileSystem.get(URI.create(uri), conf);
				BSPFileSystem bspfs = new BSPFileSystemImpl(URI.create(uri),
						conf);
				// if (fs.exists(new Path(uri))) {
				// LOG.info("Has delete message checkpoint from hdfs");
				// fs.delete(new Path(uri), true);
				// }
				if (bspfs.exists(new BSPHdfsImpl().newPath(uri))) {
					LOG.info("Has delete message checkpoint from hdfs");
					bspfs.delete(new BSPHdfsImpl().newPath(uri), true);
				}

				ssrc.setCheckNum(this.staffNum);
				int runpartitionRPCPort = workerAgent.getFreePort();
				this.partitionRPCPort = runpartitionRPCPort;
				ssrc.setPort1(runpartitionRPCPort);
				this.activeMQPort = workerAgent.getFreePort();
				ssrc.setPort2(this.activeMQPort);
				LOG.info("[BSPStaff] Get the port for partitioning RPC is : "
						+ runpartitionRPCPort + "!");
				LOG.info("[BSPStaff] Get the port for ActiveMQ Broker is : "
						+ this.activeMQPort + "!");
				this.partitionToWorkerManagerHostWithPorts = sssc
						.scheduleBarrierForMigrate(ssrc);
				// Get the globle information of route

				// record the map from partitions to workermanagers
				for (Integer e : this.partitionToWorkerManagerHostWithPorts
						.keySet()) {
					String[] nameAndPorts = this.partitionToWorkerManagerHostWithPorts
							.get(e).split(":");
					String[] ports = nameAndPorts[1].split("-");
					this.getPartitionToWorkerManagerNameAndPort().put(e,
							nameAndPorts[0] + ":" + ports[1]);
				}

				// Added later
				this.staffAgent = new WorkerAgentForStaff(job.getConf());
				workerAgent.setStaffAgentAddress(this.getSid(),
						this.staffAgent.address());
			}

			/** Clock */
			start = System.currentTimeMillis();

			//
			this.routerparameter.setPartitioner(partitioner);
			if (writePartition instanceof HashWithBalancerWritePartition) {
				this.routerparameter.setHashBucketToPartition(this
						.getHashBucketToPartition());

			} else {
				if (writePartition instanceof RangeWritePartition) {
					this.routerparameter.setRangeRouter(this.getRangeRouter());
				}
			}

			// ==Note Tidy The Code Block About Communication Setup Into One
			// Function Of Class BSPStaff.20140312..

			this.issueCommunicator(commOption, hostName, migrateSuperStep, job,
					icomMess);

			// Ever Add 201403112
			BSPStaffContext context = new BSPStaffContext(job, superStepCounter);
			context.setCommHandler(communicator);

			end = System.currentTimeMillis();
			LOG.info("[==>Clock<==] <Initialize Communicator> used "
					+ (end - start) / 1000f + " seconds");

			// add by chen
			this.counters = new Counters();
			// begin local computation
			while (this.flag) { // 本地计算循环开始
				//Biyahui added
				this.recoveryFlag=recovery;
				// Baoxing Yang added
				this.fs++;
				/* Zhicheng Liu added */
				if (this.openMigrateMode && this.hasMigrateStaff
						&& migrateSuperStep == 0) { // waiting migrate staff for
					// synchronize
					LOG.info("Having staff needed to migrate, so update the"
							+ " globle routing");
					this.hasMigrateStaff = false;

					ssrc.setCheckNum(this.staffNum);

					ssrc.setPort1(this.partitionRPCPort);

					ssrc.setPort2(this.activeMQPort);
					LOG.info("[BSPStaff migrate] Get the port for partitioning RPC is : "
							+ this.partitionRPCPort + "!");
					LOG.info("[BSPStaff migrate] Get the port for ActiveMQ Broker is : "
							+ this.activeMQPort + "!");
					this.partitionToWorkerManagerHostWithPorts = sssc // Get the
							// globle
							// route
							// information
							.scheduleBarrierForMigrate(ssrc);

					// record the map from partitions to workermanagers
					for (Integer e : this.partitionToWorkerManagerHostWithPorts
							.keySet()) {
						String[] nameAndPorts = this.partitionToWorkerManagerHostWithPorts
								.get(e).split(":");
						String[] ports = nameAndPorts[1].split("-");
						this.getPartitionToWorkerManagerNameAndPort().put(e,
								nameAndPorts[0] + ":" + ports[1]);
					}

					ArrayList<String> tmp = new ArrayList<String>();

					for (String str : this
							.getPartitionToWorkerManagerNameAndPort().values()) {
						String workerName = str.split(":")[0];
						if (!tmp.contains(workerName)) {
							tmp.add(workerName);
						}
					}

					workerAgent.setNumberWorkers(this.getJobId(),
							this.getSid(), tmp.size());
					tmp.clear();
					this.workerMangerNum = workerAgent.getNumberWorkers(
							this.getJobId(), this.getSid());
					LOG.info("get globle partitiontoWorkerNanagerNameAndPort is "
							+ this.getPartitionToWorkerManagerNameAndPort());
					this.localBarrierNum = getLocalBarrierNumber(hostName);
					/*
					 * Feng added for new version loadbalance reinitialize
					 * communicator for send messages
					 */
					this.communicator = new CommunicatorNew(this.getJobId(),
							job, this.getPartition(), partitioner);
					this.communicator.initialize(this.routerparameter,
							this.getPartitionToWorkerManagerNameAndPort(),
							this.graphData);
					this.communicator.start(hostName, this);
					this.communicator.setPartitionToWorkerManagerNamePort(this
							.getPartitionToWorkerManagerNameAndPort());

				}

				staffStartTime = System.currentTimeMillis();
				this.activeCounter = 0;
				/* Zhicheng Liu added */
				if (openMigrateMode && migrateSuperStep != 0) {
					superStepCounter = this.currentSuperStepCounter;
					recovery = false;
					//Biyahui added
					this.recoveryFlag=recovery;
					LOG.info("Test migrateSuperStep counter! "
							+ migrateSuperStep);
					migrateSuperStep = 0;
					this.migratedStaffFlag = true;
					LOG.info("this.currentSuperStepCounter is "
							+ this.currentSuperStepCounter);
				} else if (!recovery) {
					superStepCounter = this.currentSuperStepCounter;
					LOG.info("this.currentSuperStepCounter! "
							+ superStepCounter);
					LOG.info("recovery flag! " + recovery);
					this.graphData.setRecovryFlag(recovery);
				} else if (job.getGraphDataVersion() == 3) {
					LOG.info("currentSuperStepCounter! "
							+ this.currentSuperStepCounter);
					LOG.info("superstepCounter! " + superStepCounter);
					// superStepCounter = 0;
					superStepCounter = this.currentSuperStepCounter;
					this.graphData.setRecovryFlag(recovery);// added by feng for
															// staff recovery
					recovery = false;
				} else {
					superStepCounter = 0;
					recovery = false;
					//Biyahui added
					this.recoveryFlag=recovery;
				}
				// //begin next superStep barrier
				// // Barrier for sending messages over.
				// ssrc.setLocalBarrierNum(this.localBarrierNum);
				// ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);//
				// ssrc.setDirFlag(new String[] { "0" });
				// ssrc.setCheckNum(this.workerMangerNum);
				// sssc.firstStageSuperStepBarrier(this.currentSuperStepCounter,
				// ssrc);
				// Begin the communicator.From this moment,
				// the parallel sending and receiving threads have begun.
				// LOG.info("the current superStep is " + superStepCounter);
				this.communicator.setStaffId(staff.getStaffID().toString());

				this.communicator.begin(superStepCounter);
				context.refreshSuperStep(superStepCounter);
				LOG.info("##############################   "
						+ context.getCurrentSuperStepCounter());
				// Note Add 20140312
				if (false) {
					ma.setupOnEachSuperstep(superStepCounter, LOG);
				}

				// Initialize before each super step.
				SuperStepContext ssContext = new SuperStepContext(job,
						superStepCounter);
				publishAggregateValues(ssContext);
				bsp.initBeforeSuperStep(ssContext);
				initBeforeSuperStepForAggregateValues(ssContext);

				/** Clock */
				start = System.currentTimeMillis();
				LOG.info("BSPStaff--run: superStepCounter: " + superStepCounter);
				// Note Reconstructed For Better Sealing And Bucket Processing.
				if (this.openMigrateMode == true
						&& this.migrateMessagesString != null) {
					this.migrateMessagesString.clear();
				}
				// this.migrateMessages.clear();//clear last superstep migrate
				// messages
				this.graphData.setMigratedStaffFlag(migratedStaffFlag);
				this.graphData.processingByBucket(this, bsp, job,
						superStepCounter, context);
				
				LOG.info("[BSPStaff] Vertex computing is over for the super step <"
						+ this.currentSuperStepCounter + ">");

				end = System.currentTimeMillis();
				/** Clocks */
				LOG.info("[==>Clock<==] <Vertex computing> used "
						+ (end - start) / 1000f + " seconds");
				LOG.info("[==>Clock<==] ...(Load Graph Data Time) used "
						+ loadGraphTime / 1000f + " seconds");
				this.loadGraphTime = 0;
				LOG.info("[==>Clock<==] ...(Aggregate Time) used "
						+ aggregateTime / 1000f + " seconds");
				this.aggregateTime = 0;
				LOG.info("[==>Clock<==] ...(Compute Time) used " + computeTime
						/ 1000f + " seconds");
				LOG.info("[==>Clock<==] ...(Collect Messages Time) used "
						+ collectMsgsTime / 1000f + " seconds");
				this.collectMsgsTime = 0;

				/** Clock */
				start = System.currentTimeMillis();
				bsp.initAfterSuperStep(staff.getSid());
				// Notify the communicator that there will be no more messages
				// for sending.
				LOG.info("This communicator test! "
						+ this.communicator
								.getpartitionToWorkerManagerNameAndPort());
				this.communicator.noMoreMessagesForSending();
				// Wait for all of the messages have been sent
				// over.这里发送线程及其发送实例线程在迭代循环过程中一直存活
				while (true) {
					if (this.communicator.isSendingOver()) {
						break;
					}
				}
				end = System.currentTimeMillis();
				// ========================
				mssgTime = mssgTime + end - start;
				LOG.info("[==>Clock<==] <Wait for sending over> used "
						+ (end - start) / 1000f + " seconds");
				/** Clock */
				LOG.info("===========Sending Over============");
				/** Clock */
				start = end;
				/* Zhicheng Liu */
				staffEndTime = System.currentTimeMillis();
				staffRunTime = staffEndTime - staffStartTime;
				// Barrier for sending messages over.
				ssrc.setLocalBarrierNum(this.localBarrierNum);
				ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
				ssrc.setDirFlag(new String[] { "1" });
				ssrc.setCheckNum(this.workerMangerNum);
				sssc.firstStageSuperStepBarrier(this.currentSuperStepCounter,
						ssrc);
				LOG.info("debug:incomingQueueSize is "
						+ this.communicator.getIncomingQueuesSize());
				end = System.currentTimeMillis();
				// sync time for sending msg
				syncTime = end - start;
				/** Clock */
				LOG.info("[==>Clock<==] <Sending over sync> used "
						+ (end - start) / 1000f + " seconds");
				/** Clock */
				start = end;
				// Notify the communicator that there will be no more.
				// Be changed in sequence ever.[Uncertain]
				this.communicator.noMoreMessagesForReceiving();

				while (true) {
					if (this.communicator.isReceivingOver()) {
						break;
					}
				}

				end = System.currentTimeMillis();
				mssgTime = mssgTime + end - start;
				/** Clock */
				LOG.info("[==>Clock<==] <Wait for receiving over> used "
						+ (end - start) / 1000f + " seconds");
				LOG.info("===========Receiving Over===========");

				/** Clock */
				start = end;

				/* Zhicheng Liu added */
				// Count incoming messages size
				if (this.openMigrateMode) {
					if (communicator.getIncomingQueuesSize() == 0) {
						this.messagePerLength = 0;
						LOG.info("incoming message is null, so this.messagePerLength = 0");
					} else {
						while (true) {
							BSPMessage msg = (BSPMessage) communicator
									.checkAMessage();
							if (msg != null) {
								this.messagePerLength = msg.intoString()
										.getBytes().length;
								LOG.info("superstep is"
										+ this.currentSuperStepCounter);
								LOG.info("a message length is "
										+ this.messagePerLength);
								break;
							}
						} // while
					}
				} // if
					// Barrier for receiving messages over.
				ssrc.setLocalBarrierNum(this.localBarrierNum);
				ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
				ssrc.setDirFlag(new String[] { "2" });
				ssrc.setCheckNum(this.workerMangerNum * 2);
				sssc.firstStageSuperStepBarrier(this.currentSuperStepCounter,
						ssrc);
				end = System.currentTimeMillis();
				syncTime = end - start;
				/** Clock */
				LOG.info("[==>Clock<==] <Receiving over sync> used "
						+ (end - start) / 1000f + " seconds");

				// this.graphData.showMemoryInfo();
				// add by chen
				this.counters.findCounter(BspCounters.MESSAGES_NUM_SENT)
						.increment(
								this.communicator
										.getCombineOutgoMessageCounter());
				this.counters.findCounter(BspCounters.MESSAGES_NUM_RECEIVED)
						.increment(
								this.communicator.getReceivedMessageCounter());
				this.counters.findCounter(BspCounters.MESSAGE_BYTES_SENT)
						.increment(
								this.communicator
										.getCombineOutgoMessageBytesCounter());
				this.counters.findCounter(BspCounters.MESSAGE_BYTES_RECEIVED)
						.increment(
								this.communicator
										.getReceivedMessageBytesCounter());
				//add by lvs
				//count the vertexNum and edgeNum of the GraphData
				if(this.currentSuperStepCounter == 0){
					this.counters.findCounter(BspCounters.VERTEXES_NUM_OF_GRAPHDATA)
					.increment(MetaDataOfGraph.BCBSP_GRAPH_VERTEXNUM);
					this.counters.findCounter(BspCounters.EDGES_NUM_OF_GRAPHDATA)
					.increment(MetaDataOfGraph.BCBSP_GRAPH_EDGENUM);
				}

				// LOG.info("****************************************************");
				// this.counters.log(LOG);
				// Exchange the incoming and incomed queues.
				// LOG.info("debug:before exchange incomingQueueSize is " +
				// this.communicator.getIncomingQueuesSize());
				this.communicator.exchangeIncomeQueues();
				//end added
				/* Zhicheng Liu added */
				this.messageBytes = communicator.getIncomedQueuesSize()
						* this.messagePerLength;

				LOG.info("[BSPStaff] Communicator has received "
						+ this.communicator.getIncomedQueuesSize()
						+ " messages totally for the super step <"
						+ this.currentSuperStepCounter + ">");

				// decide whether to continue the next super-step or not
				if ((this.currentSuperStepCounter + 1) >= this.maxSuperStepNum) {
					this.communicator.clearOutgoingQueues();
					this.communicator.clearIncomedQueues();
					this.activeCounter = 0;
				} else {
					this.activeCounter = this.graphData.getActiveCounter();
				}
				LOG.info("[Active Vertex]" + this.activeCounter);

				/** Clock */
				start = System.currentTimeMillis();
				// Encapsulate the aggregate values into String[].
				String[] aggValues = encapsulateAggregateValues();
				end = System.currentTimeMillis();
				LOG.info("[==>Clock<==] <Encapsulate aggregate values> used "
						+ (end - start) / 1000f + " seconds");
				/** Clock */
				/** Clock */
				start = end;
				// Set the aggregate values into the super step report
				// container.

				/*
				 * Review suggestion: allow user to determine whether to use
				 * load balance Zhicheng Liu 2013/10/9
				 */

				ssrc.setAggValues(aggValues);

				ssrc.setLocalBarrierNum(this.localBarrierNum);
				ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.SECOND_STAGE);

				// to here
				LOG.info("[WorkerManagerNum]" + this.workerMangerNum);
				ssrc.setCheckNum(this.workerMangerNum + 1);
				ssrc.setJudgeFlag(this.activeCounter
						+ this.communicator.getIncomedQueuesSize()
						+ this.communicator.getOutgoingQueuesSize());

				// add by chen
				sssc.setCounters(this.counters);
				this.counters.clearCounters();
				// this.counters.log(LOG);

				/* Zhicheng Liu added */
				if (this.openMigrateMode) { // report migrate information to
					// controller
					this.migrateCost = (this.rwCheckPointT != 0 ? this.rwCheckPointT
							: this.loadDataT)
							* (this.graphBytes + this.messageBytes)
							/ this.graphBytes;
					ssrc.setStaffRunTime(this.staffRunTime);
					ssrc.setStaffID(this.getSid().getStaffID().getId());
					ssrc.setCurrentSuperStep(this.currentSuperStepCounter);
					ssrc.setMigrateCost(migrateCost);
					LOG.info("start second barrier");
					LOG.info("staffRunTime is " + this.staffRunTime);
					LOG.info("staffID is " + this.getSid());
					LOG.info("currentSuperStepCounter is "
							+ this.currentSuperStepCounter);
					LOG.info("migrateCost is " + this.migrateCost);
				}

				this.ssc = sssc.secondStageSuperStepBarrier(
						this.currentSuperStepCounter, ssrc);

				LOG.info("[==>Clock<==] <StaffSSController's rebuild session> used "
						+ StaffSSController.rebuildTime / 1000f + " seconds");

				StaffSSController.rebuildTime = 0;

				/* Zhicheng Liu added */
				// Judge whether the staff is slow
				if (this.openMigrateMode) {
					String migrateStaffIDs = ssc.getMigrateStaffIDs();

					if (!migrateStaffIDs.equals("")) {
						LOG.info("Get the superstep command, and shows that the"
								+ " migate staff id is " + migrateStaffIDs);

						this.hasMigrateStaff = true;

						String[] ids = migrateStaffIDs.split(":");
						for (int i = 0; i < ids.length; i++) {
							if (Integer.parseInt(ids[i]) == this.getSid()
									.getStaffID().getId()) {
								this.migrateFlag = true;
								LOG.info("This staff should migrate!");
								break;
							}
						}
					}
				}

				if (ssc.getCommandType() == Constants.COMMAND_TYPE.START_AND_RECOVERY) {
					LOG.info("[Command]--[routeTableSize]"
							+ ssc.getPartitionToWorkerManagerNameAndPort()
									.size());
					this.setPartitionToWorkerManagerNameAndPort(ssc
							.getPartitionToWorkerManagerNameAndPort());
					ArrayList<String> tmp = new ArrayList<String>();
					for (String str : this
							.getPartitionToWorkerManagerNameAndPort().values()) {
						if (!tmp.contains(str)) {
							tmp.add(str);
						}
					}
					this.localBarrierNum = getLocalBarrierNumber(hostName);
					workerAgent.setNumberWorkers(this.getJobId(),
							this.getSid(), tmp.size());
					tmp.clear();
					this.workerMangerNum = workerAgent.getNumberWorkers(
							this.getJobId(), this.getSid());

					displayFirstRoute();
				}
				end = System.currentTimeMillis();
				/** Clock */
				LOG.info("[==>Clock<==] <SuperStep sync> used " + (end - start)
						/ 1000f + " seconds");

				/** Clock */
				start = end;
				// Get the aggregate values from the super step command.
				// Decapsulate the aggregate values from String[].
				aggValues = this.ssc.getAggValues();
				if (aggValues != null) {
					decapsulateAggregateValues(aggValues);
				}
			end = System.currentTimeMillis();
				/** Clock */
				LOG.info("[==>Clock<==] <Decapsulate aggregate values> used "
						+ (end - start) / 1000f + " seconds");

				/** Clock */
				start = end;

				switch (ssc.getCommandType()) {
				case Constants.COMMAND_TYPE.START:
					LOG.info("Get the CommandType is : START");

					/* Zhicheng Liu added */
					if (openMigrateMode && migrateFlag) {
						// Wrtie graph checkpoint
						BSPConfiguration bspConf = new BSPConfiguration();
						String uri = bspConf.get(Constants.BC_BSP_HDFS_NAME)
								+ this.getJobId() + "/" + this.getSid()
								+ "/migrate/graph.cp";

						boolean success = cp.writeCheckPoint(this.graphData,
								new BSPHdfsImpl().newPath(uri), job, staff);

						if (success) {
							LOG.info("Has Write graphData checkPoint success!");
						} else {
							LOG.info("Can not write graphData checkPiont to hdfs");
						}

						// Write incoming message checkpoint
						uri = bspConf.get(Constants.BC_BSP_HDFS_NAME)
								+ this.getJobId() + "/" + this.getSid()
								+ "/migrate/message.cp";
						LOG.info("writeMessages size! "
								+ this.migrateMessagesString.size());
						success = cp.writeMessages(new BSPHdfsImpl().newPath(uri), job, this,
								staff, this.migrateMessagesString);

						if (success) {
							// LOG.info("messages path "+uri);
							LOG.info("Has Write incoming messages into hdfs");
						} else {
							LOG.info("Can not write messages checkpoint to hdfs");
						}

						this.flag = false;
						break;
					}

					this.currentSuperStepCounter = ssc.getNextSuperStepNum();
					this.flag = true;
					break;
				case Constants.COMMAND_TYPE.START_AND_CHECKPOINT:
					LOG.info("Get the CommandTye is : START_AND_CHECKPOINT");

					/* Zhicheng Liu added */
					// Migrate staff
					if (openMigrateMode && migrateFlag) {
						// Wrtie graph checkpoint
						BSPConfiguration bspConf = new BSPConfiguration();
						String uri = bspConf.get(Constants.BC_BSP_HDFS_NAME)
								+ this.getJobId() + "/" + this.getSid()
								+ "/migrate/graph.cp";

						boolean success = cp.writeCheckPoint(this.graphData,
								new BSPHdfsImpl().newPath(uri), job, staff);

						if (success) {
							LOG.info("Has Write graphData checkPoint success!");
						} else {
							LOG.info("Can not write graphData checkPiont to hdfs");
						}

						// Write incoming message checkpoint
						uri = bspConf.get(Constants.BC_BSP_HDFS_NAME)
								+ this.getJobId() + "/" + this.getSid()
								+ "/migrate/message.cp";
						success = cp.writeMessages(new BSPHdfsImpl().newPath(uri), job, this,
								staff, this.migrateMessagesString);

						if (success) {
							LOG.info("Has Write incoming messages into hdfs");
						} else {
							LOG.info("Can not write messages checkpoint to hdfs");
						}

					}

/*					boolean success = cp.writeCheckPoint(this.graphData,
							new BSPHdfsImpl().newPath(ssc.getInitWritePath()),
							job, staff);
					if (success) {
						deleteOldCheckpoint(ssc.getOldCheckPoint(), job);
					}*/
					
					if (aggCpFlag) {
						//String aggValuesCp = aggValues.toString();
						BSPConfiguration bspAggConf = new BSPConfiguration();
						String agguri = bspAggConf
								.get(Constants.BC_BSP_HDFS_NAME)
								+ this.getJobId()
								+ "/"
								+ this.getSid()
								+ "/assCheckpoint";

						boolean aggsuccess = asscp.writeAssCheckPoint(
								new BSPHdfsImpl().newPath(agguri),
								job, staff);

						if (aggsuccess) {
							LOG.info("assistcheckpoint work!");
						} else {
							LOG.info("Fail to write assistCheckpoint!");
						} // Feng added end-else
					} // end-if
					this.setCheckPointFrequency();
					String oripath = conf.get(Constants.BC_BSP_CHECKPOINT_WRITEPATH) +
					          "/" +
					          this.getJobId() + "/" + this.currentSuperStepCounter;
					/*Biyahui added*/
				    if(this.checkPointFrequency==this.currentSuperStepCounter){
				    	LOG.info("First time write normal checkpoint!");
						boolean successCP = cp.writeCheckPoint(this.graphData,
								new BSPHdfsImpl().newPath(ssc.getInitWritePath()),
								job, staff);
						/*Biyahui added for writing message on hdfs checkpoint*/
						backupMessages(staff,this.currentSuperStepCounter,job,this,cp);
				    }else{
				    	LOG.info("Write increment part!");
					boolean successInc = cp.writeIncrementCheckPoint(this.graphData,
							new BSPHdfsImpl().newPath(ssc.getInitWritePath()),
							job, staff);
					/*Biyahui added*/
					backupMessages(staff,this.currentSuperStepCounter,job,this,cp);
					int currentCP = ssc.getOldCheckPoint();			
					if (successInc&&currentCP!=this.checkPointFrequency) {
						deleteOldCheckpoint(ssc.getOldCheckPoint(), job);
					}
					}

					ssrc.setLocalBarrierNum(this.localBarrierNum);
					ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.WRITE_CHECKPOINT_SATGE);
					ssrc.setDirFlag(new String[] { "write" });
					ssrc.setCheckNum(this.workerMangerNum * 3);
					sssc.checkPointStageSuperStepBarrier(
							this.currentSuperStepCounter, ssrc);

					/* Zhicheng Liu added */
					if (openMigrateMode && migrateFlag) {
						this.flag = false;
					} else {
						this.currentSuperStepCounter = ssc
								.getNextSuperStepNum();
						this.flag = true;
					}

					break;
				case Constants.COMMAND_TYPE.START_AND_RECOVERY:
					LOG.info("Get the CommandTye is : START_AND_RECOVERY");
					this.currentSuperStepCounter = ssc.getAbleCheckPoint();

					// clean first
					int version = job.getGraphDataVersion();
					this.graphData = this.getGraphDataFactory()
							.createGraphData(version, this);
					this.graphData.clean();
					// this.graphData = cp.readCheckPoint(
					// new BSPHdfsImpl().newPath(ssc.getInitWritePath()), job,
					// staff);
					// ljn modefied 20140701
					String oripathTest = conf.get(Constants.BC_BSP_CHECKPOINT_WRITEPATH) +
					          "/" +
					          this.getJobId() + "/" + this.checkPointFrequency;
					/*Biyahui revised*/
					if(this.checkPointFrequency==ssc.getAbleCheckPoint()){
						this.graphData = cp.readCheckPoint(new BSPHdfsImpl().newPath(oripathTest),job, staff);
					}else{
						this.graphData = cp.readIncCheckPoint(new BSPHdfsImpl().newPath(oripathTest),
								new BSPHdfsImpl().newPath(ssc.getInitReadPath()),
								job, staff);
					}
					if (aggCpFlag){
						BSPConfiguration bspAggConf = new BSPConfiguration();
						String agguri = bspAggConf
								.get(Constants.BC_BSP_HDFS_NAME)
								+ this.getJobId()
								+ "/"
								+ this.getSid()
								+ "/assCheckpoint";
					        assCKinfo = asscp.readAssCheckPoint(new BSPHdfsImpl().newPath(agguri), job, this);
					}
					ssrc.setPartitionId(this.getPartition());
					ssrc.setLocalBarrierNum(this.localBarrierNum);
					ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE);
					ssrc.setDirFlag(new String[] { "read" });
					ssrc.setCheckNum(this.workerMangerNum * 1);

					ssrc.setPort2(this.activeMQPort);
					LOG.info("[BSPStaff] Get the port for ActiveMQ Broker is : "
							+ this.activeMQPort + "!");
					this.setPartitionToWorkerManagerNameAndPort(sssc
							.checkPointStageSuperStepBarrier(
									this.currentSuperStepCounter, ssrc));
					displayFirstRoute();
					/*
					 * Feng added for new version staff recovery reinitialize
					 * communicator for send messages
					 */
					//Biyahui noted
					//this.communicator = new CommunicatorNew(this.getJobId(),
							//job, this.getPartition(), partitioner);
					this.communicator.initialize(this.routerparameter,
							this.getPartitionToWorkerManagerNameAndPort(),
							this.graphData);
					this.communicator.start(hostName, this);
					this.communicator.setPartitionToWorkerManagerNamePort(this
							.getPartitionToWorkerManagerNameAndPort());
					this.communicator.setPartitionToWorkerManagerNamePort(this
					 .getPartitionToWorkerManagerNameAndPort());
					context.setCommHandler(communicator);
					this.currentSuperStepCounter = ssc.getNextSuperStepNum();

					this.communicator.clearOutgoingQueues();
					this.communicator.clearIncomedQueues();

					recovery = true;
					this.flag = true;
					break;
				case Constants.COMMAND_TYPE.STOP:
					LOG.info("Get the CommandTye is : STOP");
					LOG.info("Staff will save the computation result and then quit!");
					this.currentSuperStepCounter = ssc.getNextSuperStepNum();
					this.flag = false;
					break;
				default:
					LOG.error("ERROR! "
							+ ssc.getCommandType()
							+ " is not a valid CommandType, so the staff will save the "
							+ "computation result and quit!");
					flag = false;
				}
				// Report the status at every superstep.
				workerAgent.setStaffStatus(this.getSid(),
						Constants.SATAFF_STATUS.RUNNING, null, 1);
			} // 本地计算循环结束

			this.communicator.complete();

		} catch (IOException ioe) { // try 结束
			LOG.error("Exception has been catched in BSPStaff--run--during"
					+ " local computing !", ioe);
			workerAgent.setStaffStatus(
					this.getSid(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.DISK, Fault.Level.CRITICAL,
							workerAgent.getWorkerManagerName(job.getJobID(),
									this.getSid()), ioe.toString(), job
									.getJobID().toString(), this.getSid()
									.toString()), 1);
			LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*="
					+ "*=*=*=*=*=*=*=*=*");
			LOG.error("IO Exception has happened and been catched, "
					+ "the exception will be reported to WorkerManager", ioe);
			LOG.error("Staff will quit abnormally");
			return;
		} catch (Exception e) {
			LOG.error("Exception has been catched in BSPStaff--run--during"
					+ " local computing !", e);
			workerAgent.setStaffStatus(
					this.getSid(),
					Constants.SATAFF_STATUS.FAULT,
					new Fault(Fault.Type.SYSTEMSERVICE,
							Fault.Level.INDETERMINATE, workerAgent
									.getWorkerManagerName(job.getJobID(),
											this.getSid()), e.toString(), job
									.getJobID().toString(), this.getSid()
									.toString()), 1);
			LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*="
					+ "*=*=*=*=*=*=*=*");
			LOG.error("Other Exception has happened and been catched, "
					+ "the exception will be reported to WorkerManager", e);
			LOG.error("Staff will quit abnormally");
			return;
		}

		// Zhicheng Liu Test
		if (!this.migrateFlag) {
			// save the computation result

			try {
				saveResult(job, staff, workerAgent);

				ssrc.setLocalBarrierNum(this.localBarrierNum);
				ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.SAVE_RESULT_STAGE);
				ssrc.setDirFlag(new String[] { "1", "2", "write", "read" });
				sssc.saveResultStageSuperStepBarrier(
						this.currentSuperStepCounter, ssrc);
				// cleanup after local computation
				bsp.cleanup(staff);
				stopActiveMQBroker();
				done(workerAgent);
				workerAgent.setStaffStatus(this.getSid(),
						Constants.SATAFF_STATUS.SUCCEED, null, 1);
				LOG.info("The max SuperStep num is " + this.maxSuperStepNum);
				LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*"
						+ "=*=*=*=*=*=*=*=*");
				LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*:the send and receive time"
						+ " accumulation is  " + mssgTime / 1000f);
				LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*:the synchronization time"
						+ " accumulation is  " + syncTime / 1000f);
				LOG.info("Staff is completed successfully");
			} catch (Exception e) {
				LOG.error("Exception has been catched in BSPStaff--run--after"
						+ " local computing !", e);
				workerAgent.setStaffStatus(
						this.getSid(),
						Constants.SATAFF_STATUS.FAULT,
						new Fault(Fault.Type.SYSTEMSERVICE,
								Fault.Level.INDETERMINATE, workerAgent
										.getWorkerManagerName(job.getJobID(),
												this.getSid()), e.toString(),
								job.getJobID().toString(), this.getSid()
										.toString()), 2);
				LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*"
						+ "=*=*=*=*=*=*=*");
				LOG.error("Other Exception has happened and been catched, "
						+ "the exception will be reported to WorkerManager", e);
			}

		} else { // This staff should be migrated
			LOG.info("This staff stop running and migrate!");

			bsp.cleanup(staff);
			stopActiveMQBroker();
			try {
				done(workerAgent);
				LOG.info("setStaffStatus before! recovery " + recovery);
				workerAgent.setStaffStatus(this.getSid(),
						Constants.SATAFF_STATUS.SUCCEED, null, 1);

				boolean succ = workerAgent.updateWorkerJobState(this.getSid());
				LOG.info("setStaffStatus after! recovery " + recovery);
				if (succ) {
					LOG.info("Update the infomation of staffs successfully!");
				} else {
					LOG.info("Can not update the infomation of staffs successfully!");
				}

				LOG.info("The max SuperStep num is " + this.maxSuperStepNum);
				LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*="
						+ "*=*=*=*=*=*=*=*");
				LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*:the send and receive time"
						+ " accumulation is  " + mssgTime / 1000f);
				LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*:the synchronization time"
						+ " accumulation is  " + syncTime / 1000f);
				LOG.info("Staff is completed successfully");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * For partition compute.
	 */
	  @Override
	  public void runPartition(BSPJob job, Staff staff,
	      WorkerAgentProtocol workerAgent, boolean recovery,
	      boolean changeWorkerState, int migrateSuperStep, int failCounter,
	      String hostName) {
	    WritePartition writePartition = (WritePartition) ReflectionUtils
	        .newInstance(
	            job.getConf().getClass(
	                Constants.USER_BC_BSP_JOB_WRITEPARTITION_CLASS,
	                HashWritePartition.class), job.getConf());
	    // record the number of failures of this staff
	    LOG.info("BSPStaff---run()--changeWorkerState: " + changeWorkerState
	        + "[HostName] " + hostName);
	    staff.setFailCounter(failCounter);
	    // Note Memory Deploy 20140312
	    MemoryAllocator ma = new MemoryAllocator(job);
	    ma.PrintMemoryInfo(LOG);
	    ma.setupBeforeLoadGraph(LOG);
	    initializeBefore(job, workerAgent);
	    // instalize the staff.
	    SuperStepReportContainer ssrc = newInstanceSSRC();
	    this.sssc = new StaffSSController(this.getJobId(), this.getSid(),
	        workerAgent);
	    Checkpoint cp = new Checkpoint(job);
	    AggValueCheckpoint aggcp = new AggValueCheckpoint(job);
	    /* Zhicheng Liu added */
	    this.openMigrateMode = this.bspJob.getConf()
	        .get("bcbsp.loadbalance", "false").equals("true") ? true : false;
	    /* Feng added */
	    this.aggCpFlag = this.bspJob.getConf()
	        .get("bcbsp.aggValuesCheckpoint", "false").equals("true") ? true
	        : false;
	    if (this.getGraphDataFactory() == null) {
	      this.setGraphDataFactory(new GraphDataFactory(job.getConf()));
	    }
	    // prepare for staff
	    try {
	      this.counters = new Counters();
	      // for staff migtare
	      if (openMigrateMode && migrateSuperStep != 0) {
	        boolean staffMigrateFlag = true;// flag to judge a staff migrate
	        // to set the init vertex path
	        LOG.info("Migrate new staff " + this.getSid());
	        this.currentSuperStepCounter = migrateSuperStep - 1;
	        prepareForMigrate(workerAgent, hostName);
	        prepareMigrateGraphdata(job, staff, cp);
	        intializePartitionForRecovery(job, writePartition);
	      } else if (!recovery) {
	        ssrc.setCheckNum(this.staffNum);
	        int runpartitionRPCPort = workerAgent.getFreePort();
	        /* Zhicheng Liu added */
	        this.partitionRPCPort = runpartitionRPCPort;
	        ssrc.setPort1(runpartitionRPCPort);
	        this.activeMQPort = workerAgent.getFreePort();
	        ssrc.setPort2(this.activeMQPort);
	        LOG.info("[BSPStaff] Get the port for partitioning RPC is : "
	            + runpartitionRPCPort + "!");
	        LOG.info("[BSPSr Ataff] Get the port foctiveMQ Broker is : "
	            + this.activeMQPort + "!");
	        initializeAfter(job, workerAgent);
	        recordWorkerManagerNameAndPort(ssrc);
	        this.staffAgent = new WorkerAgentForStaff(job.getConf());
	        workerAgent.setStaffAgentAddress(this.getSid(),
	            this.staffAgent.address());
	        initStaffNum(hostName);
	        initWorkerNum(workerAgent);
	        /** Clock */
	        long start = System.currentTimeMillis();
	        try {
	          loadData(job, workerAgent, this.staffAgent);
	        } catch (Exception e) {
	          throw new RuntimeException(
	              "Load data Exception in BSP staff runPartition", e);
	        }
	        long end = System.currentTimeMillis();
	        LOG.info("[==>Clock<==] <BSP Partition compute load Data> used "
	            + (end - start) / 1000f + " seconds");
	        if (this.openMigrateMode) {
	          this.loadDataT = (end - start) * 2;
	          updateMigrateStatistics();
	        }
	      } else {
	        // for recovery
	        LOG.info("The recoveried staff begins to read checkpoint");
	        LOG.info("The fault SuperStepCounter is : "
	            + job.getInt("staff.fault.superstep", 0));
	        prepareRecoverySchedule(job, workerAgent, hostName);
	        prepareRecoveryGraphdata(job, staff, cp);
	        resetRecoverySuperStepReportContainer(ssrc);
	        freshRecoveryPort(ssrc, workerAgent);
	        this.currentSuperStepCounter = ssc.getNextSuperStepNum();
	        intializePartitionForRecovery(job, writePartition);
	      }
	    } catch (ClassNotFoundException cnfE) {
	      faultProcessLocalCompute(job, staff, workerAgent, cnfE, 0);
	      return;
	    } catch (IOException ioE) {
	      faultProcessLocalCompute(job, staff, workerAgent, ioE, 0);
	      return;
	      // } catch (InterruptedException iE) {
	      // faultProcessLocalCompute(job, staff, workerAgent, iE, 0);
	      // return;
	    }
	    this.bsp = (BSP) ReflectionUtils
	        .newInstance(
	            job.getConf().getClass(Constants.USER_BC_BSP_JOB_WORK_CLASS,
	                BSP.class), job.getConf());
	    prpareAggregate(job, migrateSuperStep);
	    /* Zhicheng Liu added */
	    if (openMigrateMode && migrateSuperStep != 0) {
	      decapsulateAggForMigrate();
	    }
	    String commOption = job.getCommucationOption();
	    try {
	      // configuration before local computation
	      bsp.setup(staff);
	      this.icomMess = null;
	      if (openMigrateMode && migrateSuperStep != 0) { // read message from
	        // hdfs for recovery
	        processStaffMigrate(job, staff, cp, ssrc, workerAgent);
	        // this.currentSuperStepCounter++;
	      }
	      prepareRoute(writePartition);
	      this.issueCommunicator(commOption, hostName, 0, job, icomMess);
	      BSPStaffContext context = new BSPStaffContext(job,
	          this.currentSuperStepCounter);
	      context.setCommHandler(communicator);
	      // Begin local computation for the partition compute.
	      while (this.flag) { 
	        /* Zhicheng Liu added waiting migrate staff for */
	        if (this.openMigrateMode && this.hasMigrateStaff
	            && migrateSuperStep == 0) {
	          LOG.info("Having staff needed to migrate, so update the"
	              + " globle routing");
	          this.hasMigrateStaff = false;
	          resetPortsMigrate(ssrc, workerAgent, hostName);
	          rebuildCommunicator(job, hostName);
	        }
	        staffStartTime = System.currentTimeMillis();
	        this.activeCounter = 0;
	        if (openMigrateMode && migrateSuperStep != 0) {
	          recovery = false;
	          LOG.info("Test migrateSuperStep counter! " + migrateSuperStep);
	          migrateSuperStep = 0;
	          this.migratedStaffFlag = true;
	          LOG.info("this.currentSuperStepCounter is "
	              + this.currentSuperStepCounter);
	        } else if (!recovery) {
	          this.graphData.setRecovryFlag(recovery);
	        } else {// for recovery
	          recovery = false;
	        }
	        prepareForOneSuperstep(staff, context, bsp, job);
	        long start = System.currentTimeMillis();
	        // graph data processing and BSPPeer compute.
	        if (this.openMigrateMode == true && this.migrateMessagesString != null) {
	          this.migrateMessagesString.clear();
	        }
	        this.graphData.setMigratedStaffFlag(migratedStaffFlag);
	        this.graphData.processingByBucket(this, bsp, job,
	            currentSuperStepCounter, context);
	        long end = System.currentTimeMillis();
	        reportTimeOneStep(start, end);
	        start = System.currentTimeMillis();
	        this.communicator.noMoreMessagesForSending();
	        while (true) {
	          if (this.communicator.isSendingOver()) {
	            break;
	          }
	        }
	        start = System.currentTimeMillis();
	        staffEndTime = start;
	        staffRunTime = staffEndTime - staffStartTime;
	        processSendSSRC(ssrc);
	        end = System.currentTimeMillis();
	        LOG.info("[==>Clock<==] <Sending over sync> used " + (end - start)
	            / 1000f + " seconds");
	        start = end;
	        this.communicator.noMoreMessagesForReceiving();
	        while (true) {
	          if (this.communicator.isReceivingOver()) {
	            break;
	          }
	        }
	        if (this.openMigrateMode) {
	          updateMggMigrateStatistics();
	        }
	        processReceiveSSRC(ssrc);
	        updateCounter();
	        this.communicator.exchangeIncomeQueues();
	        reportMessage();
	        if ((this.currentSuperStepCounter + 1) >= this.maxSuperStepNum) {
	          this.communicator.clearOutgoingQueues();
	          this.communicator.clearIncomedQueues();
	          this.activeCounter = 0;
	        } else {
	          this.activeCounter = this.graphData.getActiveCounter();
	        }
	        encapsulateAgg(ssrc);
	        setSecondBarrier(ssrc, context);
	        sssc.setCounters(this.counters);
	        this.counters.clearCounters();
	        if (this.openMigrateMode) {
	          updateMigrateCost(ssrc);
	        }
	        this.ssc = sssc.secondStageSuperStepBarrier(
	            this.currentSuperStepCounter, ssrc);
	        LOG.info("[==>Clock<==] <StaffSSController's rebuild session> used "
	            + StaffSSController.rebuildTime / 1000f + " seconds");
	        StaffSSController.rebuildTime = 0;
	        if (this.openMigrateMode) {
	          confirmMigrateStaff();
	        }
	        if (ssc.getCommandType() == Constants.COMMAND_TYPE.START_AND_RECOVERY) {
	          prepareForRecovery(workerAgent, hostName);
	        }
	        decapsulateAgg(aggcp, job, staff);
	        
	        // command tye to switch.
	        switch (ssc.getCommandType()) {
	        case Constants.COMMAND_TYPE.START:
	          LOG.info("Get the CommandType is : START");
	          if (openMigrateMode && migrateFlag) {
	            writeMigrateData(cp, job, staff);
	            this.flag = false;
	            break;
	          }
	          this.currentSuperStepCounter = ssc.getNextSuperStepNum();
	          this.flag = true;
	        break;
	        case Constants.COMMAND_TYPE.START_AND_CHECKPOINT:
	          LOG.info("Get the CommandTye is : START_AND_CHECKPOINT");
	          if (openMigrateMode && migrateFlag) {
	            writeMigrateData(cp, job, staff);
	          }
	          processCheckpointCommand(cp, job, staff, ssrc);
	          if (openMigrateMode && migrateFlag) {
	            this.flag = false;
	          } else {
	            this.currentSuperStepCounter = ssc.getNextSuperStepNum();
	            this.flag = true;
	          }
	        break;
	        case Constants.COMMAND_TYPE.START_AND_RECOVERY:
	          LOG.info("Get the CommandTye is : START_AND_RECOVERY");
	          this.currentSuperStepCounter = ssc.getAbleCheckPoint();
	          processRecoveryCommand(cp, job, staff, ssrc, hostName);
	          displayFirstRoute();
	          recovery = true;
	          this.flag = true;
	        break;
	        case Constants.COMMAND_TYPE.STOP:
	          LOG.info("Get the CommandTye is : STOP");
	          LOG.info("Staff will save the computation result and then quit!");
	          this.currentSuperStepCounter = ssc.getNextSuperStepNum();
	          this.flag = false;
	        break;
	        default:
	          LOG.error("ERROR! " + ssc.getCommandType()
	              + " is not a valid CommandType, so the staff will save the "
	              + "computation result and quit!");
	          flag = false;
	        }
	        workerAgent.setStaffStatus(this.getSid(),
	            Constants.SATAFF_STATUS.RUNNING, null, 1);
	      }
	      this.communicator.complete();
	    } catch (IOException ioe) { // try over
	      faultProcessLocalCompute(job, staff, workerAgent, ioe, 1);
	      LOG.error("Staff will quit abnormally");
	      return;
	    } catch (Exception e) {
	      faultProcessLocalCompute(job, staff, workerAgent, e, 1);
	      LOG.error("Staff will quit abnormally");
	      return;
	    }
	    if (!this.migrateFlag) {
	      try {
	        finishStaff(job, staff, workerAgent, ssrc);
	        LOG.info("Staff is completed successfully");
	      } catch (Exception e) {
	        reportErrorStaff(workerAgent, job, staff, e);
	      }
	    } else {
	      finishMigrateStaff(bsp, staff, workerAgent, recovery);
	    }
	  }
	  
	  private void reportErrorStaff(WorkerAgentProtocol workerAgent, BSPJob job,
	      Staff staff, Exception e) {
	    faultProcessLocalCompute(job, staff, workerAgent, e, 2);
	  }
	  
	  private void finishStaff(BSPJob job, Staff staff,
	      WorkerAgentProtocol workerAgent, SuperStepReportContainer ssrc) {
	    saveResult(job, staff, workerAgent);
	    
	    ssrc.setLocalBarrierNum(this.localBarrierNum);
	    ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.SAVE_RESULT_STAGE);
	    ssrc.setDirFlag(new String[] {"1","2","write","read"});
	    sssc.saveResultStageSuperStepBarrier(this.currentSuperStepCounter, ssrc);
	    // cleanup after local computation
	    bsp.cleanup(staff);
	    stopActiveMQBroker();
	    try {
	      done(workerAgent);
	    } catch (IOException e) {
	      throw new RuntimeException(e);
	    }
	    workerAgent.setStaffStatus(this.getSid(), Constants.SATAFF_STATUS.SUCCEED,
	        null, 1);
	    LOG.info("The max SuperStep num is " + this.maxSuperStepNum);
	  }
	  
	  private void processRecoveryCommand(Checkpoint cp, BSPJob job, Staff staff,
	      SuperStepReportContainer ssrc, String hostName) {
	    // clean first
	    int version = job.getGraphDataVersion();
	    this.graphData = this.getGraphDataFactory().createGraphData(version, this);
	    this.graphData.clean();
	    // this.graphData = cp.readCheckPoint(
	    // new BSPHdfsImpl().newPath(ssc.getInitWritePath()), job,
	    // staff);
	    // ljn modefied 20140701
	    this.graphData = cp.readCheckPoint(
	        new BSPHdfsImpl().newPath(ssc.getInitReadPath()), job, staff);
	    ssrc.setPartitionId(this.getPartition());
	    ssrc.setLocalBarrierNum(this.localBarrierNum);
	    ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE);
	    ssrc.setDirFlag(new String[] {"read"});
	    ssrc.setCheckNum(this.workerMangerNum * 1);
	    
	    ssrc.setPort2(this.activeMQPort);
	    LOG.info("[BSPStaff] Get the port for ActiveMQ Broker is : "
	        + this.activeMQPort + "!");
	    this.setPartitionToWorkerManagerNameAndPort(sssc
	        .checkPointStageSuperStepBarrier(this.currentSuperStepCounter, ssrc));
	    displayFirstRoute();
	    /*
	     * Feng added for new version staff recovery reinitialize communicator for
	     * send messages
	     */
	    //Biyahui noted
	    //this.communicator = new CommunicatorNew(this.getJobId(), job,
	        //this.getPartition(), partitioner);
	    this.communicator.initialize(this.routerparameter,
	        this.getPartitionToWorkerManagerNameAndPort(), this.graphData);
	    this.communicator.start(hostName, this);
	    this.communicator.setPartitionToWorkerManagerNamePort(this
	        .getPartitionToWorkerManagerNameAndPort());
	    // this.communicator.setPartitionToWorkerManagerNamePort(this
	    // .getPartitionToWorkerManagerNameAndPort());
	    
	    this.currentSuperStepCounter = ssc.getNextSuperStepNum();
	    
	    this.communicator.clearOutgoingQueues();
	    this.communicator.clearIncomedQueues();
	    
	  }
	  
	  /**
	   * Initialize the staff state.
	   */
	  public void initializeBefore(BSPJob job, WorkerAgentProtocol workerAgent) {
	    this.bspJob = job;
	    this.maxSuperStepNum = job.getNumSuperStep();
	    this.staffNum = job.getNumBspStaff();
	  }
	  
	  public void initializeAfter(BSPJob job, WorkerAgentProtocol workerAgent) {
	    // ***note for putHeadNode Error @author Liu Jinpeng 2013/06/26
	    {
	      this.recordParse = (RecordParse) ReflectionUtils.newInstance(
	          job.getConf().getClass(Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
	              RecordParseDefault.class), job.getConf());
	      this.recordParse.init(job);
	      
	      int version = job.getGraphDataVersion();
	      this.graphData = this.getGraphDataFactory()
	          .createGraphData(version, this);
	    }
	    
	    // this.counters = new Counters();
	  }
	  
	  /**
	   * Return a new SuperStepReportContainer for staff.
	   * @return ssrc
	   */
	  public SuperStepReportContainer newInstanceSSRC() {
	    SuperStepReportContainer ssrc = new SuperStepReportContainer();
	    ssrc.setPartitionId(this.getPartition());
	    return ssrc;
	  }
	  
	  public void recordWorkerManagerNameAndPort(SuperStepReportContainer ssrc) {
	    this.partitionToWorkerManagerHostWithPorts = sssc.scheduleBarrier(ssrc);
	    // record the map from partitions to workermanagers
	    for (Integer e : this.partitionToWorkerManagerHostWithPorts.keySet()) {
	      String[] nameAndPorts = this.partitionToWorkerManagerHostWithPorts.get(e)
	          .split(":");
	      String[] ports = nameAndPorts[1].split("-");
	      this.getPartitionToWorkerManagerNameAndPort().put(e,
	          nameAndPorts[0] + ":" + ports[1]);
	    }
	  }
	  
	  // /**
	  // * Set the state of ssrc in no recorvy staff.
	  // * @param ssrc
	  // */
	  // public void setSSRCState(SuperStepReportContainer ssrc) {
	  // ssrc.setCheckNum(this.staffNum);
	  // ssrc.setPort1(partitionRPCPort);
	  // ssrc.setPort2(this.activeMQPort);
	  // }
	  
	  /**
	   * For partition and for WorkerManager to invoke rpc method of Staff.
	   * @param job
	   * @param workerAgent
	   */
	  public void initCommunicatePort(BSPJob job, WorkerAgentProtocol workerAgent) {
	    /* Zhicheng Liu added */
	    this.partitionRPCPort = workerAgent.getFreePort();
	    this.activeMQPort = workerAgent.getFreePort();
	    LOG.info("[BSPStaff] Get the port for partitioning RPC is : "
	        + partitionRPCPort + "!");
	    
	  }
	  
	  // initialize the number of local staffs and the number of
	  // workers of the same job
	  /**
	   * Initialize the number of local staffs.
	   * @param hostName
	   */
	  public void initStaffNum(String hostName) {
	    this.localBarrierNum = getLocalBarrierNumber(hostName);
	  }
	  
	  /**
	   * Initialize the number of workers of the same job.
	   * @param hostName
	   */
	  public void initWorkerNum(WorkerAgentProtocol workerAgent) {
	    this.workerMangerNum = workerAgent.getNumberWorkers(this.getJobId(),
	        this.getSid());
	    displayFirstRoute();
	  }
	  
	  /**
	   * Prepare the aggregator and aggretevalue before compute.
	   * @param job
	   */
	  public void prpareAggregate(BSPJob job, int migrateSuperStep) {
	    /** Clock */
	    long start = System.currentTimeMillis();
	    // load aggregators and aggregate values.
	    loadAggregators(job);
	    long end = System.currentTimeMillis();
	    LOG.info("[==>Clock<==] <loadAggregators> used " + (end - start) / 1000f
	        + " seconds");
	    /* Zhicheng Liu added */
	    /*
	     * if (openMigrateMode && migrateSuperStep != 0) { // String[] aggValues =
	     * this.ssc.getAggValues(); // if (aggValues != null) { //
	     * decapsulateAggregateValues(aggValues); // } // }
	     */
	    if (openMigrateMode && migrateSuperStep != 0) {
	      String[] aggValues = this.ssc.getAggValues();
	      if (aggValues != null) {
	        decapsulateAggregateValues(aggValues);
	      }
	    }
	  }
	  
	  /**
	   * Initialize the route of bsp staff for partition cumpute.
	   * @param writePartition
	   */
	  public void prepareRoute(WritePartition writePartition) {
	    this.routerparameter.setPartitioner(partitioner);
	    if (writePartition instanceof HashWithBalancerWritePartition) {
	      this.routerparameter.setHashBucketToPartition(this
	          .getHashBucketToPartition());
	      
	    } else {
	      if (writePartition instanceof RangeWritePartition) {
	        this.routerparameter.setRangeRouter(this.getRangeRouter());
	      }
	    }
	  }
	  
	  /**
	   * Prepare the local compute for this superstep.
	   * @param staff
	   * @param context
	   * @param bsp
	   * @param job
	   */
	  public void prepareForOneSuperstep(Staff staff, BSPStaffContext context,
	      BSP bsp, BSPJob job) {
	    this.communicator.setStaffId(staff.getStaffID().toString());
	    this.communicator.begin(this.currentSuperStepCounter);
	    context.refreshSuperStep(this.currentSuperStepCounter);
	    SuperStepContext ssContext = new SuperStepContext(job,
	        currentSuperStepCounter);
	    publishAggregateValues(ssContext);
	    bsp.initBeforeSuperStep(ssContext);
	    initBeforeSuperStepForAggregateValues(ssContext);
	  }
	  
	  /**
	   * Report the time information for one superstep.
	   */
	  public void reportTimeOneStep(long start, long end) {
	    /** Clocks */
	    LOG.info("[BSPStaff] Vertex computing is over for the super step <"
	        + this.currentSuperStepCounter + ">");
	    LOG.info("[==>Clock<==] [Local computing for partition ] used "
	        + (end - start) / 1000f + " seconds" + " in superstep "
	        + currentSuperStepCounter);
	    LOG.info("[==>Clock<==] ...(Load Graph Data Time) used " + loadGraphTime
	        / 1000f + " seconds" + " in superstep " + currentSuperStepCounter);
	    this.loadGraphTime = 0;
	    LOG.info("[==>Clock<==] ...(Aggregate Time) used " + aggregateTime / 1000f
	        + " seconds" + " in superstep " + currentSuperStepCounter);
	    this.aggregateTime = 0;
	    LOG.info("[==>Clock<==] ...(Compute Time) used " + computeTime / 1000f
	        + " seconds" + " in superstep " + currentSuperStepCounter);
	    LOG.info("[==>Clock<==] ...(Collect Messages Time) used " + collectMsgsTime
	        / 1000f + " seconds");
	    this.collectMsgsTime = 0;
	  }
	  
	  /**
	   * After send update ssrc.
	   * @param ssrc
	   */
	  public void processSendSSRC(SuperStepReportContainer ssrc) {
	    ssrc.setLocalBarrierNum(this.localBarrierNum);
	    ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
	    ssrc.setDirFlag(new String[] {"1"});
	    ssrc.setCheckNum(this.workerMangerNum);
	    sssc.firstStageSuperStepBarrier(this.currentSuperStepCounter, ssrc);
	    LOG.info("debug:incomingQueueSize is "
	        + this.communicator.getIncomingQueuesSize());
	  }
	  
	  /**
	   * After receive update ssrc.
	   * @param ssrc
	   */
	  public void processReceiveSSRC(SuperStepReportContainer ssrc) {
	    ssrc.setLocalBarrierNum(this.localBarrierNum);
	    ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
	    ssrc.setDirFlag(new String[] {"2"});
	    ssrc.setCheckNum(this.workerMangerNum * 2);
	    sssc.firstStageSuperStepBarrier(this.currentSuperStepCounter, ssrc);
	  }
	  
	  /**
	   * Update the counter after one superstep.
	   */
	  public void updateCounter() {
	    this.counters.findCounter(BspCounters.MESSAGES_NUM_SENT).increment(
	        this.communicator.getCombineOutgoMessageCounter());
	    this.counters.findCounter(BspCounters.MESSAGES_NUM_RECEIVED).increment(
	        this.communicator.getReceivedMessageCounter());
	    this.counters.findCounter(BspCounters.MESSAGE_BYTES_SENT).increment(
	        this.communicator.getCombineOutgoMessageBytesCounter());
	    this.counters.findCounter(BspCounters.MESSAGE_BYTES_RECEIVED).increment(
	        this.communicator.getReceivedMessageBytesCounter());
		//add by lvs
		//count the vertexNum and edgeNum of the GraphData
		if(this.currentSuperStepCounter == 0){
			this.counters.findCounter(BspCounters.VERTEXES_NUM_OF_GRAPHDATA)
			.increment(MetaDataOfGraph.BCBSP_GRAPH_VERTEXNUM);
			this.counters.findCounter(BspCounters.EDGES_NUM_OF_GRAPHDATA)
			.increment(MetaDataOfGraph.BCBSP_GRAPH_EDGENUM);
		}
	  }
	  
	  public void reportMessage() {
	    this.messageBytes = communicator.getIncomedQueuesSize()
	        * this.messagePerLength;
	    LOG.info("[BSPStaff] Communicator has received "
	        + this.communicator.getIncomedQueuesSize()
	        + " messages totally for the super step <"
	        + this.currentSuperStepCounter + ">");
	  }
	  
	  /**
	   * Encapsulate the aggregate values into String[].
	   */
	  public void encapsulateAgg(SuperStepReportContainer ssrc) {
	    String[] aggValues = encapsulateAggregateValues();
	    ssrc.setAggValues(aggValues);
	  }
	  
	  /**
	   * Set the state of ssrc for senond barrier.
	   * @param ssrc
	   */
	  public void setSecondBarrier(SuperStepReportContainer ssrc,
	      BSPStaffContext context) {
	    ssrc.setAggValues(ssrc.getAggValues());
	    ssrc.setLocalBarrierNum(this.localBarrierNum);
	    ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.SECOND_STAGE);
	    // to here
	    LOG.info("[WorkerManagerNum]" + this.workerMangerNum);
	    ssrc.setCheckNum(this.workerMangerNum + 1);
	    if (context.getCurrentSuperStepCounter() > 0 && !(context.getActiveFLag())) {
	      ssrc.setJudgeFlag(0);
	    } else {
	      ssrc.setJudgeFlag(this.activeCounter
	          + this.communicator.getIncomedQueuesSize()
	          + this.communicator.getOutgoingQueuesSize());
	    }
	  }
	  
	  /**
	   * Prepare the network, checknumber and superstep for recovery.
	   * @param workerAgent
	   * @param hostName
	   */
	  private void prepareForRecovery(WorkerAgentProtocol workerAgent,
	      String hostName) {
	    LOG.info("[Command]--[routeTableSize]"
	        + ssc.getPartitionToWorkerManagerNameAndPort().size());
	    this.setPartitionToWorkerManagerNameAndPort(ssc
	        .getPartitionToWorkerManagerNameAndPort());
	    ArrayList<String> tmp = new ArrayList<String>();
	    for (String str : this.getPartitionToWorkerManagerNameAndPort().values()) {
	      if (!tmp.contains(str)) {
	        tmp.add(str);
	      }
	    }
	    this.localBarrierNum = getLocalBarrierNumber(hostName);
	    workerAgent.setNumberWorkers(this.getJobId(), this.getSid(), tmp.size());
	    tmp.clear();
	    this.workerMangerNum = workerAgent.getNumberWorkers(this.getJobId(),
	        this.getSid());
	    
	    displayFirstRoute();
	  }
	  
	  /**
	   * Decapsulate the aggregate values from String[].
	   * @param aggcp
	   * @param job
	   * @param staff
	   */
	  public void decapsulateAgg(AggValueCheckpoint aggcp, BSPJob job, Staff staff) {
	    String[] aggValues = this.ssc.getAggValues();
	    if (aggValues != null) {
	      decapsulateAggregateValues(aggValues);
	      
	      /* for checkpoint the aggvalues */
	      if (aggCpFlag) {
	        String aggValuesCp = aggValues.toString();
	        BSPConfiguration bspAggConf = new BSPConfiguration();
	        String agguri = bspAggConf.get(Constants.BC_BSP_HDFS_NAME)
	            + this.getJobId() + "/" + this.getSid()
	            + "/aggValueCheckpoint/aggCheckpoint.cp";
	        boolean aggsuccess;
	        try {
	          aggsuccess = aggcp.writeAggCheckPoint(aggValuesCp,
	              new BSPHdfsImpl().newPath(agguri), job, staff);
	          if (aggsuccess) {
	            LOG.info("AggValues have been writen into aggCheckpoint!");
	          } else {
	            LOG.info("Fail to write aggValues into aggCheckpoint!");
	          }
	        } catch (IOException e) {
	          new RuntimeException(e);
	        }// Feng added end-else
	      } // end-if
	    } // end-if
	  }
	  
	  private void processCheckpointCommand(Checkpoint cp, BSPJob job, Staff staff,
	      SuperStepReportContainer ssrc) {
	    boolean success;
	    try {
	      success = cp.writeCheckPoint(this.graphData,
	          new BSPHdfsImpl().newPath(ssc.getInitWritePath()), job, staff);
	      if (success) {
	        deleteOldCheckpoint(ssc.getOldCheckPoint(), job);
	      }
	      
	      ssrc.setLocalBarrierNum(this.localBarrierNum);
	      ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.WRITE_CHECKPOINT_SATGE);
	      ssrc.setDirFlag(new String[] {"write"});
	      ssrc.setCheckNum(this.workerMangerNum * 3);
	      sssc.checkPointStageSuperStepBarrier(this.currentSuperStepCounter, ssrc);
	    } catch (Exception e) {
	      throw new RuntimeException(e);
	    }
	  }
	  
	  /**
	   * Prepare for recovery , reset the parameters.
	   * @param job
	   * @param workerAgent
	   * @param hostName
	   */
	  private void prepareRecoverySchedule(BSPJob job,
	      WorkerAgentProtocol workerAgent, String hostName) {
	    this.ssc = sssc.secondStageSuperStepBarrierForRecovery(job.getInt(
	        "staff.fault.superstep", 0));
	    this.setPartitionToWorkerManagerNameAndPort(ssc
	        .getPartitionToWorkerManagerNameAndPort());
	    this.localBarrierNum = getLocalBarrierNumber(hostName);
	    ArrayList<String> tmp = new ArrayList<String>();
	    for (String str : this.getPartitionToWorkerManagerNameAndPort().values()) {
	      if (!tmp.contains(str)) {
	        tmp.add(str);
	      }
	    }
	    workerAgent.setNumberWorkers(this.getJobId(), this.getSid(), tmp.size());
	    tmp.clear();
	    this.workerMangerNum = workerAgent.getNumberWorkers(this.getJobId(),
	        this.getSid());
	    this.currentSuperStepCounter = ssc.getAbleCheckPoint();
	  }
	  
	  /**
	   * Prepare the graphdata from checkpoint for recovery.
	   * @param job
	   * @param staff
	   */
	  private void prepareRecoveryGraphdata(BSPJob job, Staff staff, Checkpoint cp) {
	    int version = job.getGraphDataVersion();
	    this.graphData = this.getGraphDataFactory().createGraphData(version, this);
	    this.graphData.clean();
	    
	    /* Zhicheng Liu added */
	    long tmpTS = System.currentTimeMillis();
	    
	    this.graphData=cp.readCheckPoint(new BSPHdfsImpl().newPath(ssc.getInitReadPath()), job, staff);
	    
	    /* Zhicheng Liu added */
	    long tmpTE = System.currentTimeMillis();
	    this.rwCheckPointT = (tmpTE - tmpTS) * 2;
	  }
	  
	  /**
	   * Reset the ssrc for recovery staff.
	   * @param ssrc
	   */
	  private void resetRecoverySuperStepReportContainer(
	      SuperStepReportContainer ssrc) {
	    ssrc.setLocalBarrierNum(this.localBarrierNum);
	    ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE);
	    ssrc.setDirFlag(new String[] {"read"});
	    ssrc.setCheckNum(this.workerMangerNum * 1);
	  }
	  
	  /**
	   * Get the new port of ActiveMQ.
	   * @param ssrc
	   * @param workerAgent
	   */
	  private void freshRecoveryPort(SuperStepReportContainer ssrc,
	      WorkerAgentProtocol workerAgent) {
	    this.activeMQPort = workerAgent.getFreePort();
	    ssrc.setPort2(this.activeMQPort);
	    LOG.info("[BSPStaff] ReGet the port for ActiveMQ Broker is : "
	        + this.activeMQPort + "!");
	    
	    this.setPartitionToWorkerManagerNameAndPort(sssc
	        .checkPointStageSuperStepBarrier(this.currentSuperStepCounter, ssrc));
	    displayFirstRoute();
	  }
	  
	  /**
	   * Rebuid the partition and read data from checkpoint for intializing.
	   * @param job
	   * @param writePartition
	   * @throws ClassNotFoundException
	   * @throws IOException
	   */
	  private void intializePartitionForRecovery(BSPJob job,
	      WritePartition writePartition) throws ClassNotFoundException, IOException {
	    this.currentSuperStepCounter = ssc.getNextSuperStepNum();
	    LOG.info("Now, this super step count is " + this.currentSuperStepCounter);
	    this.partitioner = (Partitioner<Text>) ReflectionUtils.newInstance(
	        job.getConf().getClass(Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
	            HashPartitioner.class), job.getConf());
	    
	    if (writePartition instanceof HashWithBalancerWritePartition) {
	      this.partitioner.setNumPartition(this.staffNum * numCopy);
	    } else {
	      this.partitioner.setNumPartition(this.staffNum);
	    }
	    org.apache.hadoop.mapreduce.InputSplit split = null;
	    if (rawSplitClass.equals("no")) {
	      
	    } else {
	      
	      DataInputBuffer splitBuffer = new DataInputBuffer();
	      splitBuffer.reset(rawSplit.getBytes(), 0, rawSplit.getLength());
	      SerializationFactory factory = new SerializationFactory(job.getConf());
	      Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deserializer = (Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>) factory
	          .getDeserializer(job.getConf().getClassByName(rawSplitClass));
	      deserializer.open(splitBuffer);
	      split = deserializer.deserialize(null);
	    }
	    this.partitioner.intialize(job, split);
	    displayFirstRoute();
	  }
	  
	  private void faultProcessLocalCompute(BSPJob job, Staff staff,
	      WorkerAgentProtocol workerAgent, Exception e, int faultNumber) {
	    LOG.error("Exception has been catched in BSPStaff--run--before"
	        + " local computing !", e);
	    Type type;
	    Level level;
	    if (e instanceof ClassNotFoundException) {
	      type = Fault.Type.SYSTEMSERVICE;
	      level = Fault.Level.CRITICAL;
	    } else if (e instanceof IOException) {
	      type = Fault.Type.DISK;
	      level = Fault.Level.INDETERMINATE;
	    } else if (e instanceof InterruptedException) {
	      type = Fault.Type.SYSTEMSERVICE;
	      level = Fault.Level.CRITICAL;
	    } else {
	      type = Fault.Type.SYSTEMSERVICE;
	      level = Fault.Level.INDETERMINATE;
	    }
	    workerAgent.setStaffStatus(
	        staff.getStaffAttemptId(),
	        Constants.SATAFF_STATUS.FAULT,
	        new Fault(type, level, workerAgent.getWorkerManagerName(job.getJobID(),
	            staff.getStaffAttemptId()), e.toString(),
	            job.getJobID().toString(), staff.getStaffAttemptId().toString()),
	        faultNumber);
	    LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*="
	        + "*=*=*=*=*=*=*=*=*");
	    LOG.error("Exception has happened and been catched, "
	        + "the exception will be reported to WorkerManager", e);
	  }
	  
	  /**
	   * Prepare the network and checknumber for Migrate.
	   * @param workerAgent
	   * @param hostName
	   */
	  private void prepareForMigrate(WorkerAgentProtocol workerAgent,
	      String hostName) {
	    this.ssc = sssc
	        .secondStageSuperStepBarrierForRecovery(this.currentSuperStepCounter);
	    this.setPartitionToWorkerManagerNameAndPort(ssc
	        .getPartitionToWorkerManagerNameAndPort());
	    this.localBarrierNum = getLocalBarrierNumber(hostName);
	    ArrayList<String> tmp = new ArrayList<String>();
	    for (String str : this.getPartitionToWorkerManagerNameAndPort().values()) {
	      if (!tmp.contains(str)) {
	        tmp.add(str);
	      }
	    }
	    // Update the worker information
	    workerAgent.setNumberWorkers(this.getJobId(), this.getSid(), tmp.size());
	    tmp.clear();
	    this.workerMangerNum = workerAgent.getNumberWorkers(this.getJobId(),
	        this.getSid());
	  }
	  
	  /**
	   * Prepare the graphdata from checkpoint for migrate.
	   * @param job
	   * @param staff
	   * @throws IOException
	   */
	  private void prepareMigrateGraphdata(BSPJob job, Staff staff, Checkpoint cp)
	      throws IOException {
	    int version = job.getGraphDataVersion();
	    this.graphData = this.getGraphDataFactory().createGraphData(version, this);
	    this.graphData.clean();
	    // this.graphData.setMigratedStaffFlag(true);
	    BSPConfiguration bspConf = new BSPConfiguration();
	    String uri = bspConf.get(Constants.BC_BSP_HDFS_NAME) + this.getJobId()
	        + "/" + this.getSid() + "/migrate/graph.cp";
	    long tmpTS = System.currentTimeMillis();
	    this.graphData = cp.readCheckPoint(new BSPHdfsImpl().newPath(uri), job,
	        staff);
	    long tmpTE = System.currentTimeMillis();
	    this.rwCheckPointT = (tmpTE - tmpTS) * 2;
	    if (graphData.getVertexSize() == 0) {
	      vertexSize = 10;
	    }
	    this.graphBytes = graphData.sizeForAll() * vertexSize;
	    LOG.info("readGraph from checkpoint: this.graphBytes is " + this.graphBytes);
	    Configuration conf = new Configuration();
	    BSPFileSystem bspfs = new BSPFileSystemImpl(URI.create(uri), conf);
	    if (bspfs.exists(new BSPHdfsImpl().newPath(uri))) {
	      bspfs.delete(new BSPHdfsImpl().newPath(uri), true);
	      LOG.info("Has deleted the checkpoint of graphData on hdfs");
	    }
	  }
	  
	  /**
	   * Update the the graph Statistics for Migrate.
	   */
	  private void updateMigrateStatistics() {
	    if (graphData.getVertexSize() == 0) {
	      vertexSize = 8;
	    }
	    this.graphBytes = graphData.sizeForAll() * vertexSize;
	  }
	  
	  /**
	   * Update the the message Statistics for Migrate.
	   */
	  private void updateMggMigrateStatistics() {
	    if (communicator.getIncomingQueuesSize() == 0) {
	      // ljn test motify
	      // this.messagePerLength = 0;
	      this.messagePerLength = 10;
	      // LOG.info("incoming message is null, so this.messagePerLength = 0");
	      LOG.info("incoming message is null, so this.messagePerLength = 10");
	    } else {
	      while (true) {
	        BSPMessage msg = (BSPMessage) communicator.checkAMessage();
	        if (msg != null) {
	          this.messagePerLength = msg.intoString().getBytes().length;
	          break;
	        }
	      } // while
	    }
	  }
	  
	  /**
	   * Decapsulate aggregate value for migrate.
	   */
	  private void decapsulateAggForMigrate() {
	    String[] aggValues = this.ssc.getAggValues();
	    if (aggValues != null) {
	      decapsulateAggregateValues(aggValues);
	    }
	  }
	  
	  /**
	   * Process the Migrate work for Staff.
	   * @param job
	   * @param staff
	   * @param cp
	   * @param ssrc
	   * @param workerAgent
	   * @throws IOException
	   */
	  private void processStaffMigrate(BSPJob job, Staff staff, Checkpoint cp,
	      SuperStepReportContainer ssrc, WorkerAgentProtocol workerAgent)
	      throws IOException {
	    LOG.info("read message checkpoint from hdfs for migrate");
	    BSPConfiguration bspConf = new BSPConfiguration();
	    String uri = bspConf.get(Constants.BC_BSP_HDFS_NAME) + this.getJobId()
	        + "/" + this.getSid() + "/migrate/message.cp";
	    icomMess = cp.readMessages(new BSPHdfsImpl().newPath(uri), job, staff);
	    LOG.info("Migrate messages size! " + icomMess.size());
	    // Delete from hdfs
	    Configuration conf = new Configuration();
	    // FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    BSPFileSystem bspfs = new BSPFileSystemImpl(URI.create(uri), conf);
	    // if (fs.exists(new Path(uri))) {
	    // LOG.info("Has delete message checkpoint from hdfs");
	    // fs.delete(new Path(uri), true);
	    // }
	    if (bspfs.exists(new BSPHdfsImpl().newPath(uri))) {
	      LOG.info("Has delete message checkpoint from hdfs");
	      bspfs.delete(new BSPHdfsImpl().newPath(uri), true);
	    }
	    ssrc.setCheckNum(this.staffNum);
	    int runpartitionRPCPort = workerAgent.getFreePort();
	    this.partitionRPCPort = runpartitionRPCPort;
	    ssrc.setPort1(runpartitionRPCPort);
	    this.activeMQPort = workerAgent.getFreePort();
	    ssrc.setPort2(this.activeMQPort);
	    LOG.info("[BSPStaff] Get the port for partitioning RPC is : "
	        + runpartitionRPCPort + "!");
	    LOG.info("[BSPStaff] Get the port for ActiveMQ Broker is : "
	        + this.activeMQPort + "!");
	    this.partitionToWorkerManagerHostWithPorts = sssc
	        .scheduleBarrierForMigrate(ssrc);
	    // Get the globle information of route
	    
	    // record the map from partitions to workermanagers
	    for (Integer e : this.partitionToWorkerManagerHostWithPorts.keySet()) {
	      String[] nameAndPorts = this.partitionToWorkerManagerHostWithPorts.get(e)
	          .split(":");
	      String[] ports = nameAndPorts[1].split("-");
	      this.getPartitionToWorkerManagerNameAndPort().put(e,
	          nameAndPorts[0] + ":" + ports[1]);
	    }
	    
	    // Added later
	    this.staffAgent = new WorkerAgentForStaff(job.getConf());
	    workerAgent.setStaffAgentAddress(this.getSid(), this.staffAgent.address());
	  }
	  
	  /**
	   * Reset the localcheck number and communicator(RPC,ActiveMQ) for Migrating.
	   * @param ssrc
	   * @param workerAgent
	   */
	  private void resetPortsMigrate(SuperStepReportContainer ssrc,
	      WorkerAgentProtocol workerAgent, String hostName) {
	    ssrc.setCheckNum(this.staffNum);
	    ssrc.setPort1(this.partitionRPCPort);
	    ssrc.setPort2(this.activeMQPort);
	    LOG.info("[BSPStaff migrate] Get the port for partitioning RPC is : "
	        + this.partitionRPCPort + "!");
	    LOG.info("[BSPStaff migrate] Get the port for ActiveMQ Broker is : "
	        + this.activeMQPort + "!");
	    this.partitionToWorkerManagerHostWithPorts = sssc
	        .scheduleBarrierForMigrate(ssrc);
	    
	    // record the map from partitions to workermanagers
	    for (Integer e : this.partitionToWorkerManagerHostWithPorts.keySet()) {
	      String[] nameAndPorts = this.partitionToWorkerManagerHostWithPorts.get(e)
	          .split(":");
	      String[] ports = nameAndPorts[1].split("-");
	      this.getPartitionToWorkerManagerNameAndPort().put(e,
	          nameAndPorts[0] + ":" + ports[1]);
	    }
	    
	    ArrayList<String> tmp = new ArrayList<String>();
	    
	    for (String str : this.getPartitionToWorkerManagerNameAndPort().values()) {
	      String workerName = str.split(":")[0];
	      if (!tmp.contains(workerName)) {
	        tmp.add(workerName);
	      }
	    }
	    
	    workerAgent.setNumberWorkers(this.getJobId(), this.getSid(), tmp.size());
	    tmp.clear();
	    this.workerMangerNum = workerAgent.getNumberWorkers(this.getJobId(),
	        this.getSid());
	    LOG.info("get globle partitiontoWorkerNanagerNameAndPort is "
	        + this.getPartitionToWorkerManagerNameAndPort());
	    this.localBarrierNum = getLocalBarrierNumber(hostName);
	  }
	  
	  /**
	   * Rebuild the communicator for migrate staff.
	   * @param job
	   * @param hostName
	   */
	  private void rebuildCommunicator(BSPJob job, String hostName) {
	    /*
	     * Feng added for new version loadbalance reinitialize communicator for send
	     * messages
	     */
	    this.communicator = new CommunicatorNew(this.getJobId(), job,
	        this.getPartition(), partitioner);
	    this.communicator.initialize(this.routerparameter,
	        this.getPartitionToWorkerManagerNameAndPort(), this.graphData);
	    this.communicator.start(hostName, this);
	    this.communicator.setPartitionToWorkerManagerNamePort(this
	        .getPartitionToWorkerManagerNameAndPort());
	  }
	  
	  /**
	   * Compute the migrate cost and report migrate information to bspcontroller.
	   * @param ssrc
	   */
	  private void updateMigrateCost(SuperStepReportContainer ssrc) {
	    if (this.graphBytes == 0) {
	      this.graphBytes = 1;
	    }
	    this.migrateCost = (this.rwCheckPointT != 0 ? this.rwCheckPointT
	        : this.loadDataT)
	        * (this.graphBytes + this.messageBytes)
	        / this.graphBytes;
	    ssrc.setStaffRunTime(this.staffRunTime);
	    ssrc.setStaffID(this.getSid().getStaffID().getId());
	    ssrc.setCurrentSuperStep(this.currentSuperStepCounter);
	    ssrc.setMigrateCost(migrateCost);
	    LOG.info("start second barrier");
	    LOG.info("staffRunTime is " + this.staffRunTime);
	    LOG.info("staffID is " + this.getSid());
	    LOG.info("currentSuperStepCounter is " + this.currentSuperStepCounter);
	    LOG.info("migrateCost is " + this.migrateCost);
	  }
	  
	  /**
	   * Judge whether the staff is slow.
	   */
	  private void confirmMigrateStaff() {
	    String migrateStaffIDs = ssc.getMigrateStaffIDs();
	    if (!migrateStaffIDs.equals("")) {
	      LOG.info("Get the superstep command, and shows that the"
	          + " migate staff id is " + migrateStaffIDs);
	      
	      this.hasMigrateStaff = true;
	      
	      String[] ids = migrateStaffIDs.split(":");
	      for (int i = 0; i < ids.length; i++) {
	        if (Integer.parseInt(ids[i]) == this.getSid().getStaffID().getId()) {
	          this.migrateFlag = true;
	          LOG.info("This staff should migrate!");
	          break;
	        }
	      }
	    }
	  }
	  
	  /**
	   * Write the graph data and message into Checkpoint for migrate staff.
	   * @param cp
	   * @param job
	   * @param staff
	   * @throws IOException
	   * @throws InterruptedException
	   */
	  private void writeMigrateData(Checkpoint cp, BSPJob job, Staff staff)
	      throws IOException, InterruptedException {
	    // Wrtie graph checkpoint
	    BSPConfiguration bspConf = new BSPConfiguration();
	    String uri = bspConf.get(Constants.BC_BSP_HDFS_NAME) + this.getJobId()
	        + "/" + this.getSid() + "/migrate/graph.cp";
	    boolean success = cp.writeCheckPoint(this.graphData,
	        new BSPHdfsImpl().newPath(uri), job, staff);
	    if (success) {
	      LOG.info("Has Write graphData checkPoint success for migrate staff!");
	    } else {
	      LOG.info("Can not write graphData checkPiont to hdfs");
	    }
	    // Write incoming message checkpoint
	    uri = bspConf.get(Constants.BC_BSP_HDFS_NAME) + this.getJobId() + "/"
	        + this.getSid() + "/migrate/message.cp";
	    LOG.info("writeMessages size! " + this.migrateMessagesString.size());
	    success = cp.writeMessages(new BSPHdfsImpl().newPath(uri), job, this, staff,
	        this.migrateMessagesString);
	    if (success) {
	      LOG.info("Has Write incoming messages into hdfs for migrate staff");
	    } else {
	      LOG.info("Can not write messages checkpoint to hdfs");
	    }
	  }
	  
	  /**
	   * Finish the staff of one step for migrate staff.
	   * @param bsp
	   * @param staff
	   * @param workerAgent
	   */
	  private void finishMigrateStaff(BSP bsp, Staff staff,
	      WorkerAgentProtocol workerAgent, boolean recovery) {
	    LOG.info("This staff stop running and migrate!");
	    bsp.cleanup(staff);
	    stopActiveMQBroker();
	    try {
	      done(workerAgent);
	      LOG.info("setStaffStatus before! recovery " + recovery);
	      workerAgent.setStaffStatus(this.getSid(),
	          Constants.SATAFF_STATUS.SUCCEED, null, 1);
	      
	      boolean succ = workerAgent.updateWorkerJobState(this.getSid());
	      LOG.info("setStaffStatus after! recovery " + recovery);
	      if (succ) {
	        LOG.info("Update the infomation of staffs successfully!");
	      } else {
	        LOG.info("Can not update the infomation of staffs successfully!");
	      }
	    } catch (IOException e) {
	      e.printStackTrace();
	    }
	  }
	
	/**
	 * Get BSP job configuration.
	 */
	public BSPJob getConf() {
		return this.bspJob;
	}

	/**
	 * Set BSP job configuration.
	 */
	public void setConf(BSPJob bspJob) {
		this.bspJob = bspJob;
	}

	/** Write and read split info to WorkerManager. */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		Text.writeString(out, rawSplitClass);
		rawSplit.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		rawSplitClass = Text.readString(in);
		rawSplit.readFields(in);
	}

	/**
	 * Get partition to worker manager name and port.
	 */
	/*
	 * public HashMap<Integer, String> getPartitionToWorkerManagerNameAndPort()
	 * { try{ return this.getPartitionToWorkerManagerNameAndPort();
	 * }catch(Exception e){ LOG.info("getPartitionToworkerManager error is:"+e);
	 * return null; } }
	 */

	@SuppressWarnings("unchecked")
	private void loadAggregators(BSPJob job) {
		int aggregateNum = job.getAggregateNum();
		String[] aggregateNames = job.getAggregateNames();
		for (int i = 0; i < aggregateNum; i++) {
			String name = aggregateNames[i];
			this.nameToAggregator.put(name, job.getAggregatorClass(name));
			this.nameToAggregateValue.put(name,
					job.getAggregateValueClass(name));
		}
		try {
			// Instanciate each aggregate values.
			for (Entry<String, Class<? extends AggregateValue<?, ?>>> entry : this.nameToAggregateValue
					.entrySet()) {
				String aggName = entry.getKey();
				AggregateValue aggValue;
				aggValue = entry.getValue().newInstance();
				this.aggregateValuesCurrent.put(aggName, aggValue);
			}
		} catch (InstantiationException e) {
			LOG.error("[BSPStaff:loadAggregators]", e);
		} catch (IllegalAccessException e) {
			LOG.error("[BSPStaff:loadAggregators]", e);
		}
	}

	@SuppressWarnings("unchecked")
	private void initBeforeSuperStepForAggregateValues(
			SuperStepContext ssContext) {
		for (Entry<String, AggregateValue> entry : this.aggregateValuesCurrent
				.entrySet()) {
			AggregateValue aggValue = entry.getValue();
			aggValue.initBeforeSuperStep(ssContext);
		}
	}

	@SuppressWarnings("unchecked")
	private void aggregate(ConcurrentLinkedQueue<IMessage> messages,
			BSPJob job, Vertex vertex, int superStepCount) {
		try {
			for (Entry<String, Class<? extends AggregateValue<?, ?>>> entry : this.nameToAggregateValue
					.entrySet()) {

				String aggName = entry.getKey();

				// Init the aggregate value for this head node.
				AggregateValue aggValue1 = this.aggregateValuesCurrent
						.get(aggName);
				AggregationContext aggContext = new AggregationContext(job,
						vertex, superStepCount);
				publishAggregateValues(aggContext);
				aggValue1.initValue(messages.iterator(), aggContext);

				// Get the current aggregate value.
				AggregateValue aggValue0;
				aggValue0 = this.aggregateValues.get(aggName);

				// Get the aggregator for this kind of aggregate value.
				Aggregator<AggregateValue> aggregator;
				aggregator = (Aggregator<AggregateValue>) this.nameToAggregator
						.get(aggName).newInstance();

				// Aggregate
				if (aggValue0 == null) { // the first time aggregate.
					aggValue0 = (AggregateValue) aggValue1.clone();
					this.aggregateValues.put(aggName, aggValue0);
				} else {
					ArrayList<AggregateValue> tmpValues = new ArrayList<AggregateValue>();
					tmpValues.add(aggValue0);
					tmpValues.add(aggValue1);
					AggregateValue aggValue = aggregator.aggregate(tmpValues);
					this.aggregateValues.put(aggName, aggValue);
				}
			}
		} catch (InstantiationException e) {
			LOG.error("[BSPStaff:aggregate]", e);
		} catch (IllegalAccessException e) {
			LOG.error("[BSPStaff:aggregate]", e);
		}
	}

	/**
	 * To encapsulate the aggregation values to the String[]. The aggValues
	 * should be in form as follows: [ AggregateName \t
	 * AggregateValue.toString() ]
	 * 
	 * @return String[]
	 */
	@SuppressWarnings("unchecked")
	private String[] encapsulateAggregateValues() {

		int aggSize = this.aggregateValues.size();

		String[] aggValues = new String[aggSize];

		int ia = 0;
		for (Entry<String, AggregateValue> entry : this.aggregateValues
				.entrySet()) {
			aggValues[ia] = entry.getKey() + Constants.KV_SPLIT_FLAG
					+ entry.getValue().toString();
			ia++;
		}
		// The cache for this super step should be cleared for next super step.
		this.aggregateValues.clear();

		return aggValues;
	}

	/**
	 * To decapsulate the aggregation values from the String[]. The aggValues
	 * should be in form as follows: [ AggregateName \t
	 * AggregateValue.toString() ]
	 * 
	 * @param aggValues
	 *            String[]
	 */
	@SuppressWarnings("unchecked")
	private void decapsulateAggregateValues(String[] aggValues) {

		for (int i = 0; i < aggValues.length; i++) {
			String[] aggValueRecord = aggValues[i]
					.split(Constants.KV_SPLIT_FLAG);
			String aggName = aggValueRecord[0];
			String aggValueString = aggValueRecord[1];
			AggregateValue aggValue = null;
			try {
				aggValue = this.nameToAggregateValue.get(aggName).newInstance();
				aggValue.initValue(aggValueString); // init the aggValue from
				// its string form.
			} catch (InstantiationException e1) {
				LOG.error("ERROR", e1);
			} catch (IllegalAccessException e1) {
				LOG.error("ERROR", e1);
			} // end-try
			if (aggValue != null) {
				this.aggregateResults.put(aggName, aggValue);
			} // end-if
		} // end-for
	}

	/**
	 * To publish the aggregate values into the bsp's cache for user's accession
	 * for the next super step.
	 * 
	 * @param context
	 *            BSPStaffContext
	 */
	@SuppressWarnings("unchecked")
	private void publishAggregateValues(BSPStaffContext context) {
		for (Entry<String, AggregateValue> entry : this.aggregateResults
				.entrySet()) {
			context.addAggregateValues(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * To publish the aggregate values into the super step context for the
	 * bsp.initBeforeSuperStep for the next super step.
	 * 
	 * @param context
	 *            SuperStepContext
	 */
	@SuppressWarnings("unchecked")
	private void publishAggregateValues(SuperStepContext context) {
		for (Entry<String, AggregateValue> entry : this.aggregateResults
				.entrySet()) {
			context.addAggregateValues(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * To publish the aggregate values into the aggregation context for the
	 * aggregation value's init of each vertex.
	 * 
	 * @param context
	 *            AggregationContext
	 */
	@SuppressWarnings("unchecked")
	private void publishAggregateValues(AggregationContext context) {
		for (Entry<String, AggregateValue> entry : this.aggregateResults
				.entrySet()) {
			context.addAggregateValues(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * WorkerAgentForStaffInterface.java.
	 */
	public interface WorkerAgentForStaffInterface extends VersionedProtocol {
		public static final long versionID = 0L;

		/**
		 * This method is used to worker which this worker's partition id equals
		 * belongPartition.
		 * 
		 * @param jobId
		 *            the current BSP job id
		 * @param staffId
		 *            the current Staff id
		 * @param belongPartition
		 *            the current partition
		 * @return worker manager
		 */
		WorkerAgentForStaffInterface getWorker(BSPJobID jobId,
				StaffAttemptID staffId, int belongPartition);

		/**
		 * This method is used to put the HeadNode to WorkerAgentForJob's map.
		 * 
		 * @param jobId
		 *            the current BSP job id
		 * @param staffId
		 *            the current Staff id
		 * @param belongPartition
		 *            the partitionID which the HeadNode belongs to
		 */
		void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
				int belongPartition, BytesWritable data, String type);

		/**
		 * This method is used to put the HeadNode to WorkerAgentForJob's map.
		 * 
		 * @param jobId
		 *            the current BSP job id
		 * @param staffId
		 *            the current Staff id
		 * @param belongPartition
		 *            the partitionID which the HeadNode belongs to
		 */
		void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
				int belongPartition, BytesWritable data);

		/**
		 * Get the address of this WorkerAgentForStaff.
		 * 
		 * @return address
		 */
		String address();

		/**
		 * This method will be invoked before the staff be killed, to notice the
		 * staff to do some cleaning operations.
		 */
		void onKillStaff();

	}

	/**
	 * WorkerAgentForStaff.java.
	 * 
	 * @author root
	 */
	public class WorkerAgentForStaff implements WorkerAgentForStaffInterface {

		/** <partitionID, hostName:port1-port2> */
		private HashMap<Integer, String> partitionToWorkerManagerHostWithPorts = new HashMap<Integer, String>();
		/**
		 * The workers.
		 */
		private final Map<InetSocketAddress, WorkerAgentForStaffInterface> workers = new ConcurrentHashMap<InetSocketAddress, WorkerAgentForStaffInterface>();
		/**
		 * This class implements an IP Socket Address (IP address + port
		 * number).
		 */
		private InetSocketAddress workAddress;
		/**
		 * .hadoop.ipc.RPC.Server.
		 */
		private Server server = null;
		/**
		 * BSP job configruation.
		 */
		private Configuration conf;

		/**
		 * Constructor of WorkerAgentForStaff.
		 * 
		 * @param conf
		 *            BSP job configuration
		 */
		public WorkerAgentForStaff(Configuration conf) {
			this.partitionToWorkerManagerHostWithPorts = BSPStaff.this.partitionToWorkerManagerHostWithPorts;
			this.conf = conf;
			String[] hostandports = this.partitionToWorkerManagerHostWithPorts
					.get(BSPStaff.this.getPartition()).split(":");
			LOG.info(this.partitionToWorkerManagerHostWithPorts
					.get(BSPStaff.this.getPartition()));
			String[] ports = hostandports[1].split("-");
			workAddress = new InetSocketAddress(hostandports[0],
					Integer.parseInt(ports[0]));
			reinitialize();
		}

		/**
		 * Reinitialize the Staff.
		 */
		private void reinitialize() {

			try {
				LOG.info("reinitialize() the WorkerAgentForStaff: "
						+ getJobId().toString());

				server = RPC.getServer(this, workAddress.getHostName(),
						workAddress.getPort(), conf);
				server.start();
				LOG.info("WorkerAgentForStaff address:"
						+ workAddress.getHostName() + " port:"
						+ workAddress.getPort());

			} catch (IOException e) {
				LOG.error("[reinitialize]", e);
			}
		}

		/**
		 * Get WorkerAgentConnection
		 * 
		 * @param addr
		 *            IP address + port number
		 * @return The workerAgentConnection
		 */
		protected WorkerAgentForStaffInterface getWorkerAgentConnection(
				InetSocketAddress addr) {
			WorkerAgentForStaffInterface worker;
			synchronized (this.workers) {
				worker = workers.get(addr);

				if (worker == null) {
					try {
						worker = (WorkerAgentForStaffInterface) RPC.getProxy(
								WorkerAgentForStaffInterface.class,
								WorkerAgentForStaffInterface.versionID, addr,
								this.conf);
					} catch (IOException e) {
						LOG.error("[getWorkerAgentConnection]", e);
					}
					this.workers.put(addr, worker);
				}
			}

			return worker;
		}

		/**
		 * Get Address.
		 * 
		 * @param peerName
		 * @return IP address + port number
		 */
		private InetSocketAddress getAddress(String peerName) {
			String[] workerAddrParts = peerName.split(":");
			return new InetSocketAddress(workerAddrParts[0],
					Integer.parseInt(workerAddrParts[1]));
		}

		/**
		 * This method is used to get worker.
		 * 
		 * @param jobId
		 * @param staffId
		 * @param belongPartition
		 * @return
		 */
		public WorkerAgentForStaffInterface getWorker(BSPJobID jobId,
				StaffAttemptID staffId, int belongPartition) {

			String dstworkerName = null;
			dstworkerName = this.partitionToWorkerManagerHostWithPorts
					.get(belongPartition); // hostName:port1-port2

			String[] hostAndPorts = dstworkerName.split(":");
			String[] ports = hostAndPorts[1].split("-");
			dstworkerName = hostAndPorts[0] + ":" + ports[0];

			WorkerAgentForStaffInterface work = workers
					.get(getAddress(dstworkerName));
			if (work == null) {
				work = getWorkerAgentConnection(getAddress(dstworkerName));
			}
			return work;
		}

		/**
		 * This method is used to put the HeadNode to WorkerAgentForJob's map.
		 * 
		 * @param jobId
		 *            BSP job id
		 * @param staffId
		 *            BSP Staff id
		 * @param belongPartition
		 *            the partitionID which the HeadNode belongs to
		 */
		// hash重划分
		@SuppressWarnings("unchecked")
		public void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
				int belongPartition, BytesWritable data) {
			DataInputStream in = new DataInputStream(new BufferedInputStream(
					new ByteArrayInputStream(data.getBytes())));

			try {

				while (true) {
					Text key = new Text();
					key.readFields(in);
					Text value = new Text();
					value.readFields(in);
					if (key.getLength() > 0 && value.getLength() > 0) {

						if (BSPStaff.this.recordParse == null) {
							LOG.error("Test Null: BSPStaff.this.recordParse is NULL");

						}
						Vertex vertex = BSPStaff.this.recordParse.recordParse(
								key.toString(), value.toString());
						if (vertex == null) {
							BSPStaff.this.lost++;
							continue;
						}
						BSPStaff.this.graphData.addForAll(vertex);
					} else {
						break;
					}

				}
			} catch (IOException e) {
				LOG.error("ERROR", e);
			}

		}

		/**
		 * This method is used to put the HeadNode to WorkerAgentForJob's map.
		 * 
		 * @param jobId
		 * @param staffId
		 * @param belongPartition
		 *            the partitionID which the HeadNode belongs to
		 */
		// hash重划分
		@SuppressWarnings("unchecked")
		public void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
				int belongPartition, BytesWritable data, String type) {
			DataInputStream in = new DataInputStream(new BufferedInputStream(
					new ByteArrayInputStream(data.getBytes())));

			try {

				while (true) {
					Text key = new Text();
					key.readFields(in);
					Text value = new Text();
					value.readFields(in);
					if (key.getLength() > 0 && value.getLength() > 0) {
						application.getDownlink().sendKeyValue(key.toString(),
								value.toString());
						//
						// if (vertex == null) {
						// BSPStaff.this.lost++;
						// continue;
						// }
						// BSPStaff.this.graphData.addForAll(vertex);
					} else {
						break;
					}
				}
			} catch (IOException e) {
				LOG.error("ERROR", e);
			}
		}

		@Override
		public long getProtocolVersion(String arg0, long arg1)
				throws IOException {
			return WorkerAgentForStaffInterface.versionID;
		}

		@Override
		public String address() {
			String hostName = this.workAddress.getHostName();
			int port = this.workAddress.getPort();
			return new String(hostName + ":" + port);
		}

		@Override
		public void onKillStaff() {
			BSPStaff.this.stopActiveMQBroker();
		}
	}

	/**
	 * Start the ActiveMQ.
	 * 
	 * @param hostName
	 *            the compute nome name
	 */
	public void startActiveMQBroker(String hostName) {
		// brokerName = "hostName-partitionID"
		this.activeMQBroker = new ActiveMQBroker(hostName + "-"
				+ this.getPartition());
		try {
			this.activeMQBroker.startBroker(this.activeMQPort);
			LOG.info("[BSPStaff] starts ActiveMQ Broker successfully!");
		} catch (Exception e) {
			LOG.error("[BSPStaff] caught: ", e);
		}
	}

	/**
	 * Stop the ActiveMQ.
	 */
	public void stopActiveMQBroker() {

		if (this.activeMQBroker != null) {
			try {
				this.activeMQBroker.stopBroker();
				LOG.info("[BSPStaff] stops ActiveMQ Broker successfully!");
			} catch (Exception e) {
				LOG.error("[BSPStaff] caught: ", e);
			}
		}
	}

	/**
	 * Start the rpc server.
	 * 
	 * @param hostName
	 *            the local compute node name
	 */
	public void startRPCServer(String hostName) {
		try {
			server = RPC.getServer(this.communicator, hostName,
					this.activeMQPort, new Configuration());

			server.start();
			LOG.info("[BSPStaff] starts RPC Communication Server successfully!");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stop RPC server.
	 */
	public void stopRPCSever() {
		if (this.server == null) {
			LOG.info("[BSPStaff] stops RPC Communication Server successfully!");
			return;
		}
		this.server.stop();

	}

	/**
	 * Get the recovery times.
	 */
	public int getRecoveryTimes() {
		return recoveryTimes;
	}

	long loadGraphTime = 0;
	long aggregateTime = 0;
	long computeTime = 0;
	long collectMsgsTime = 0;

	// ============ Note Add 20140310 For Adjusting Local Computation ========
	@Override
	public void vertexProcessing(Vertex v, BSP bsp, BSPJob job,
			int superStepCounter, BSPStaffContext context, boolean activeFlag)
			throws IOException {
		// /**Feng added for migrate staff messages*/
		// ConcurrentLinkedQueue<IMessage> migrateMessages;
		String migrateMessages = null;
		String checkPointMessages=null;
		StringBuffer sb = new StringBuffer();
		StringBuffer strbuf = new StringBuffer();
		/** Clock */
		long tmpStart = System.currentTimeMillis();
		if (v == null) {
			LOG.error("Fail to get the HeadNode of index[" + "] "
					+ "and the system will skip the record");
			return;
		}
		loadGraphTime = loadGraphTime + (System.currentTimeMillis() - tmpStart);
		// Get the incomed message queue for this vertex.
		ConcurrentLinkedQueue<IMessage> messages = this.communicator
				.getMessageQueue(String.valueOf(v.getVertexID()));
		
		if (this.migratedStaffFlag == true) {

			messages = this.communicator// Feng test use the migrated messages
					.getMigrateMessageQueue(String.valueOf(v.getVertexID()));
		}
		/*Biyahui added*/
		if(this.recoveryFlag==true){
			messages = this.communicator.getRecoveryMessageQueue(String.valueOf(v.getVertexID()));
		}
		
		// Aggregate the new values for each vertex. Clock the time
		// cost.
		tmpStart = System.currentTimeMillis();
		aggregate(messages, job, v, this.currentSuperStepCounter);
		aggregateTime = aggregateTime + (System.currentTimeMillis() - tmpStart);
		// Note Ever Fault.
		// Note Ever Edit /\2014-01-23
		context.refreshVertex(v);
		if (superStepCounter > 0) {
			if (!activeFlag && (messages.size() == 0)) {
				return;
			}
		}
		
		if (this.openMigrateMode == true && messages.size() > 0) {
			Iterator<IMessage> iterator = messages.iterator();
			sb.append(v.getVertexID().toString() + Constants.MESSAGE_SPLIT);
			while (iterator.hasNext()) {
				IMessage msg = iterator.next();
				String msgID = msg.getMessageId().toString();
				String msgValue = msg.getContent().toString();
				LOG.info("Record migrate messagesID! " + msgID + "messageVale"
						+ msgValue);
				if (msgID != null) {
					sb.append(msgID + Constants.SPLIT_FLAG + msgValue
							+ Constants.SPACE_SPLIT_FLAG);
				}
			}
			migrateMessages = sb.toString();
			this.migrateMessagesString.add(migrateMessages);
		}
		Iterator<IMessage> messagesIter = messages.iterator();
		// Call the compute function for local computation.
		/*
		 * Publish the total result aggregate values into the bsp's cache for
		 * the user's function's accession in the next super step.
		 */
		publishAggregateValues(context);
		/** Clock */
		tmpStart = System.currentTimeMillis();
		try {
			//LOG.info("bsp.compute vertex message!");
			bsp.compute(messagesIter, context);
		} catch (Exception e) {
			throw new RuntimeException("catch exception", e);
		}
		computeTime = computeTime + (System.currentTimeMillis() - tmpStart);
		/** Clock */
		messages.clear();
		/** Clock */
		tmpStart = System.currentTimeMillis();
		collectMsgsTime = collectMsgsTime
				+ (System.currentTimeMillis() - tmpStart);
		/** Clock */

	}
	
	/***
	 * Biyahui added for writing messages on HDFS by bucket
	 * @param staff
	 * @param superStepCounter
	 * @param job
	 * @param graphStaffHandler
	 * @param cp
	 */
	public void backupMessages(Staff staff,int superStepCounter,BSPJob job,
			GraphStaffHandler graphStaffHandler,Checkpoint cp){
		try {
			ConcurrentLinkedQueue<IMessage> messages = null;
			vertexClass = staff.getConf().getVertexClass();
			Vertex v = vertexClass.newInstance();
			this.vManager = new VertexManager();
			for (int i = (MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER - 1); i >= 0; i--) {
		        int counter = MetaDataOfGraph.VERTEX_NUM_PERBUCKET[i];
		        if (counter == 0) {
		        	continue;
		        }
		        this.communicator.preBucketMessagesNew(i, superStepCounter);
		        this.vManager.prepareBucket(i, superStepCounter);
		        for (int j = 0; j < counter; j++) {
		        	StringBuffer strbuf = new StringBuffer();
		        	String checkPointMessages=null;
		        	vManager.processVertexLoad(v);
		        	try {
		        		messages=communicator.getMessageQueue(String.valueOf(v.getVertexID()));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		        	if(messages.size()>0){
		        		Iterator<IMessage> iterator = messages.iterator();
		    			strbuf.append(v.getVertexID().toString() + Constants.MESSAGE_SPLIT);
		    			while (iterator.hasNext()) {
		    				IMessage msg = iterator.next();
		    				String msgID = msg.getMessageId().toString();
		    				String msgValue = msg.getContent().toString();
		    				if (msgID != null) {
		    					strbuf.append(msgID + Constants.SPLIT_FLAG + msgValue
		    							+ Constants.SPACE_SPLIT_FLAG);
		    				}
		    			}
		    			checkPointMessages = strbuf.toString();
		    			this.checkPointMessages.add(checkPointMessages);
		    		}
		        }
		        //write message to HDFS by bucket
		        String oripath = conf.get(Constants.BC_BSP_CHECKPOINT_WRITEPATH) +"/" +
				          this.getJobId() + "/" + this.currentSuperStepCounter;
				String messagePath=oripath+"/"+staff.getStaffAttemptId()+"/message/"+"Bucket-"+i;
				cp.writeMessages(new Path(messagePath), job, this, staff, checkPointMessages);
				checkPointMessages.clear();
		    }
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
	@Override
	public void preBucket(int bucket, int superstep) {
		// LOG.info("++++++++++++++++++++++++++"+superstep);
		if (superstep == 0) {
			return;
		}
		this.communicator.preBucketMessages(bucket, superstep - 1);
	}
	
	/***
	 * Biyahui added for loading message for recovery staff by bucket
	 * @param bucket
	 * @param context
	 */
	public void preBucketForRecovery(int bucket,BSPJob job,BSPStaffContext context) {
		Map<String, LinkedList<IMessage>> icomRecoveryMess = null;
		if (this.recoveryFlag == true) {
			String uri = conf.get(Constants.BC_BSP_HDFS_NAME)+"tmp/hadoop-root/bcbsp/checkpoint/"
					+ this.getJobId() + "/" +ssc.getAbleCheckPoint()+"/"+ this.getStaffAttemptId()+"/message/"+"Bucket-"+bucket;
			LOG.info("read message from" + uri);
			Checkpoint cp=new Checkpoint(job);
			icomRecoveryMess = cp.readMessages(new BSPHdfsImpl().newPath(uri), job,
					this);
			this.communicator.recoveryForFault(icomRecoveryMess);
			context.setCommHandler(communicator);
		}
		
	}
	
	@Override
	public void saveResultOfVertex(Vertex v, RecordWriter output)
			throws IOException, InterruptedException {
		output.write(new Text(v.intoString()));

	}

	// ==============Add End======
	private void issueCommunicator(String commOption, String hostName,
			int migrateSuperStep, BSPJob job,
			Map<String, LinkedList<IMessage>> icomMess) {
		// Start an ActiveMQ Broker, create a communicator, initialize it,
		// and start it
		// Note TAG The Option Content
		LOG.info("[Comm Server Str Is ] ---->>>>   " + commOption);
		if (commOption.equals(Constants.ACTIVEMQ_VERSION)) {
			LOG.info("Start  Communicator[ACTIVEMQ]");
			this.communicator = new Communicator(this.getJobId(), job,
					this.getPartition(), partitioner);

			/* Zhicheng Liu added */
			if (openMigrateMode && migrateSuperStep != 0) {
				communicator.recoveryForMigrate(icomMess);
			}

		}
		// 构造RPC communicator 而且启动RPC Server
		else if (commOption.equals(Constants.RPC_VERSION)) {
			LOG.info("Start Communicator[RPC]");
			this.communicator = new RPCCommunicator(this.getJobId(), job,
					this.getPartition(), partitioner);

			/* Zhicheng Liu added */
			if (openMigrateMode && migrateSuperStep != 0) {
				communicator.recoveryForMigrate(icomMess);
				LOG.info("Migrate staff: incomed messages size is "
						+ communicator.getIncomedQueuesSize());
			}

		} else if (commOption.equals(Constants.RPC_BYTEARRAY_VERSION)) {
			LOG.info("Start  Communicator[ByteArray]");
			this.communicator = new CommunicatorNew(this.getJobId(), job,
					this.getPartition(), partitioner);
			/* Feng added for new version loadbalance */
			if (openMigrateMode && migrateSuperStep != 0) {
				LOG.info("Recovery for migrate!");
				communicator.recoveryForMigrate(icomMess);
			}
		}

		// Note Do Sth Setting Up.
		this.communicator.initialize(this.routerparameter,
				this.getPartitionToWorkerManagerNameAndPort(), this.graphData);
		this.communicator.start(hostName, this);
	}
	/**
	 * feng added to get the checkpoint frequency
	 */
	  public void setCheckPointFrequency() {
		  this.conf=this.bspJob.getConf();
	    int defaultF = conf.getInt(
	        Constants.DEFAULT_BC_BSP_JOB_CHECKPOINT_FREQUENCY, 0);
	    if (defaultF == 0) {
	      this.checkPointFrequency = defaultF;
	    } else {
	      this.checkPointFrequency = conf.getInt(
	          Constants.USER_BC_BSP_JOB_CHECKPOINT_FREQUENCY, defaultF);
	    }
	  }

	  @Override
	  public void peerProcessing(BSPPeer peer, BSP bsp, BSPJob job,
	      int superStepCounter, BSPStaffContext context, boolean activeFlag) {
	    // /**Feng added for migrate staff messages*/
	    // ConcurrentLinkedQueue<IMessage> migrateMessages;
	    String migrateMessages = null;
	    StringBuffer sb = new StringBuffer();
	    /** Clock */
	    long tmpStart = System.currentTimeMillis();
	    if (peer == null) {
	      throw new RuntimeException("No key-value to compute for staff "
	          + this.getSid());
	    }
	    loadGraphTime = loadGraphTime + (System.currentTimeMillis() - tmpStart);
	    // Get the incomed message queue for this peer(partition).
	    try {
	      ConcurrentLinkedQueue<IMessage> messages = this.communicator
	          .getMessageQueue(String
	              .valueOf(Constants.DEFAULT_PEER_DST_MESSSAGE_ID));
	      // Aggregate the new values for each vertex. Clock the time
	      // cost.
	      publishAggregateValues(context);
	      tmpStart = System.currentTimeMillis();
	      aggregate(messages, job, peer, this.currentSuperStepCounter);
	      aggregateTime = aggregateTime + (System.currentTimeMillis() - tmpStart);
	      context.updatePeer(peer);
	      if (superStepCounter > 0) {
	        if (!activeFlag && (messages.size() == 0)) {
	          return;
	        }
	      }
	      Iterator<IMessage> messagesIter = messages.iterator();
	      // Call the compute function for local computation.
	      /*
	       * Publish the total result aggregate values into the bsp's cache for the
	       * user's function's accession in the next super step.
	       */
	      // publishAggregateValues(context);
	      /** Clock */
	      tmpStart = System.currentTimeMillis();
	      try {
	        // LOG.info("bsp.compute vertex message!");
	        bsp.compute(messagesIter, context);
	      } catch (Exception e) {
	        throw new RuntimeException("catch exception", e);
	      }
	      computeTime = computeTime + (System.currentTimeMillis() - tmpStart);
	      /** Clock */
	      messages.clear();
	      peer.resetPair();
	      /** Clock */
	      tmpStart = System.currentTimeMillis();
	      collectMsgsTime = collectMsgsTime
	          + (System.currentTimeMillis() - tmpStart);
	    } catch (Exception e) {
	      throw new RuntimeException("catch exception on peer compute", e);
	    }
	    /** Clock */
	  }
	  
	  /**
	   * Aggregate for peer compute.
	   * @param messages
	   * @param job
	   * @param peer
	   * @param currentSuperStepCounter
	   */
	  private void aggregate(ConcurrentLinkedQueue<IMessage> messages, BSPJob job,
	      BSPPeer peer, int currentSuperStepCounter) {
	    
	    try {
	      for (Entry<String, Class<? extends AggregateValue<?, ?>>> entry : this.nameToAggregateValue
	          .entrySet()) {
	        
	        String aggName = entry.getKey();
	        
	        // Init the aggregate value for this head node.
	        AggregateValue aggValue1 = this.aggregateValuesCurrent.get(aggName);
	        AggregationContext aggContext = new AggregationContext(job, peer,
	            currentSuperStepCounter);
	        publishAggregateValues(aggContext);
	        aggValue1.initValue(messages.iterator(), aggContext);
	        
	        // Get the current aggregate value.
	        AggregateValue aggValue0;
	        aggValue0 = this.aggregateValues.get(aggName);
	        
	        // Get the aggregator for this kind of aggregate value.
	        Aggregator<AggregateValue> aggregator;
	        aggregator = (Aggregator<AggregateValue>) this.nameToAggregator.get(
	            aggName).newInstance();
	        
	        // Aggregate
	        if (aggValue0 == null) { // the first time aggregate.
	          aggValue0 = (AggregateValue) aggValue1.clone();
	          this.aggregateValues.put(aggName, aggValue0);
	        } else {
	          ArrayList<AggregateValue> tmpValues = new ArrayList<AggregateValue>();
	          tmpValues.add(aggValue0);
	          tmpValues.add(aggValue1);
	          AggregateValue aggValue = aggregator.aggregate(tmpValues);
	          this.aggregateValues.put(aggName, aggValue);
	        }
	      }
	    } catch (InstantiationException e) {
	      LOG.error("[BSPStaff:aggregate]", e);
	    } catch (IllegalAccessException e) {
	      LOG.error("[BSPStaff:aggregate]", e);
	    }
	    
	  }
	/** For JUnit test. */
	public int getActiveMQPort() {
		return activeMQPort;
	}

	public void setActiveMQPort(int activeMQPort) {
		this.activeMQPort = activeMQPort;
	}

	public CommunicatorInterface getCommunicator() {
		return communicator;
	}

	public void setCommunicator(CommunicatorInterface communicator) {
		this.communicator = communicator;
	}
}
