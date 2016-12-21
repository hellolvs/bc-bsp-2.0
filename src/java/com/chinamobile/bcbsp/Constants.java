/**
 * CopyRight by Chinamobile
 *
 * Constants.java
 */

package com.chinamobile.bcbsp;

/**
 * Constants This class maintains all relative global variables.
 */
public interface Constants {

  /**
   *  communication policy
   */

  /** State RPC version */
  String RPC_VERSION =
      "bcbsp.communication.version.rpc";
  /** State ActiveMQ version */
  String ACTIVEMQ_VERSION =
      "bcbsp.communication.version.activemq";
  /** State RPC bytearray version */
  String RPC_BYTEARRAY_VERSION =
      "bcbsp.communication.version.rpc.bytearray";
  /** State option version */
  String COMMUNICATION_OPTION =
      "bcbsp.communication.version.option";




  /**
   * Constants for BC-BSP ZooKeeper
   */

  /** State Zookeeper quorum,
   * Comma separated list of servers in the ZooKeeper Quorum. */
  String ZOOKEEPER_QUORUM = "bcbsp.zookeeper.quorum";
  /** State Zookeeper config name */
  String ZOOKEEPER_CONFIG_NAME = "zoo.cfg";
  /** State Zookeeper client port,
   * The port at which the clients will connect. */
  String ZOOKEPER_CLIENT_PORT = "bcbsp.zookeeper.clientPort";
  /** State default client port */
  int DEFAULT_ZOOKEPER_CLIENT_PORT = 2181;
  /** State bspjob zookeeper directory root */
  String BSPJOB_ZOOKEEPER_DIR_ROOT = "/bspRoot";
  /** State command name */
  String COMMAND_NAME = "command";
  /** State session time out */
  int SESSION_TIME_OUT = 10000;
  /** State bspcontroller leader */
  String BSPCONTROLLER_LEADER = "/leader";
  /** State bspcontroller standby leader */
  String BSPCONTROLLER_STANDBY_LEADER = "/standbyLeader";

  /**
   * Constants for HA
   */

  /** enumerate bspcontroller role */
  public static enum BspControllerRole {
    /** active */
    /** standby */
    /** neutral */
    ACTIVE, STANDBY, NEUTRAL
  }

  /** State bcbsp HA log directory */
  String BC_BSP_HA_LOG_DIR = "bcbsp.halog.dir";
  /** State bcbsp HA summit log */
  String BC_BSP_HA_SUBMIT_LOG = "submitLog";
  /** State bcbsp HA schedule log */
  String BC_BSP_HA_SCHEDULE_LOG = "scheduleLog";
  /** State bcbsp HA queue operate log */
  String BC_BSP_HA_QUEUE_OPERATE_LOG = "queueOperateLog";
  /** State bcbsp HA flag */
  String BC_BSP_HA_FLAG = "haFlag";

  /**
   * Constants for Counters
   */

  public static enum BspCounters {
    /** receive message bytes */
    /** send message bytes */
    /** send message number */
    MESSAGE_BYTES_RECEIVED, MESSAGE_BYTES_SENT, MESSAGES_NUM_SENT,
    /** receive message number */
    /** the counter of receiving message number */
    /** The global synchronization time */
    MESSAGES_NUM_RECEIVED, Messages_num_received_counter, TIME_IN_SYNC_MS,
    //add by lvs
    /** the vertexNum of the graphdata */
    /** the edgeNum of the graphdata */
    VERTEXES_NUM_OF_GRAPHDATA,EDGES_NUM_OF_GRAPHDATA
  }


  /**
   *  Constants for BC-BSP Framework
   */

  /** State bcbsp controller address ,
   * the address of the bcbsp controller server.*/
  String BC_BSP_CONTROLLER_ADDRESS = "bcbsp.controller.address";
  /** State bcbsp workmanager RPC hostname */
  String BC_BSP_WORKERMANAGER_RPC_HOST = "bcbsp.workermanager.rpc.hostname";
  /** State bcbsp default workmanager RPC hostname */
  String DEFAULT_BC_BSP_WORKERMANAGER_RPC_HOST = "0.0.0.0";
  /** State bcbsp workmanager RPC port,
   * The port an worker manager server binds to. */
  String BC_BSP_WORKERMANAGER_RPC_PORT = "bcbsp.workermanager.rpc.port";
  /** State bcbsp workmanager report address,
   * The interface and port that groom server listens on.  */
  String BC_BSP_WORKERMANAGER_REPORT_ADDRESS =
      "bcbsp.workermanager.report.address";
  /** State bcbsp worker agent host */
  String BC_BSP_WORKERAGENT_HOST = "bcbsp.workeragent.host";
  /** State bcbsp default worker agent host */
  String DEFAULT_BC_BSP_WORKERAGENT_HOST = "0.0.0.0";
  /** State bcbsp worker agent port,
   * The port an worker agent server binds to. */
  String BC_BSP_WORKERAGENT_PORT = "bcbsp.workeragent.port";
  /** State bcbsp default worker agent port */
  int DEFAULT_BC_BSP_WORKERAGENT_PORT = 61000;
  /** State bcbsp workermanager max staff,
   * The max number of staffs running on one worker manager. */
  String BC_BSP_WORKERMANAGER_MAXSTAFFS = "bcbsp.workermanager.staff.max";
  /** State bcbsp share directory,
   * The shared directory where BSP stores control files. */
  String BC_BSP_SHARE_DIRECTORY = "bcbsp.share.dir";
  /** State bcbsp checkpoint directory */
  String BC_BSP_CHECKPOINT_DIRECTORY = "bcbsp.checkpoint.dir";
  /** State bcbsp local directory,
   * local directory for temporal store */
  String BC_BSP_LOCAL_DIRECTORY = "bcbsp.local.dir";
  /** State bcbsp local subdir controller */
  String BC_BSP_LOCAL_SUBDIR_CONTROLLER = "controller";
  /** State bcbsp local subdir workermanager */
  String BC_BSP_LOCAL_SUBDIR_WORKERMANAGER = "workerManager";
  /** State heart beat interval,
   * The interval of heart beat in millsecond. */
  String HEART_BEAT_INTERVAL = "bcbsp.heartbeat.interval";
  /** State heart beat timeout,
   *  The threshold of time out for heart beat in millsecond.*/
  String HEART_BEAT_TIMEOUT = "bcbsp.heartbeat.timeout";
  /** State sleep timeout,
   * The maximum of sleeping time for the gray worker */
  String SLEEP_TIMEOUT = "bcbsp.worker.sleep.timeout";
  /** State bcbsp per worker failed job number,
   * The maximum of failed job on one worker. */
  String BC_BSP_FAILED_JOB_PER_WORKER = "bcbsp.max.faied.job.worker";
  /** State bcbsp jvm version */
  String BC_BSP_JVM_VERSION = "bcbsp.jvm.bits";
  /** State bcbsp hdfs name,
   * The name of the default file system. */
  String BC_BSP_HDFS_NAME = "fs.default.name";


  /**
   * Constants for BC-BSP Job
   */

  /** State bcbsp job default checkpoint frequency,
   * The default frequency of checkpoint. */
  String DEFAULT_BC_BSP_JOB_CHECKPOINT_FREQUENCY = "bcbsp.checkpoint.frequency";
  
  String DEFAULT_BC_BSP_JOB_CHECKPOINT_USER_DEFINE = "job.assistCheckpoint.class";
  /** State bcbsp job max attempt recovery,
   *  If a job is fault, the cluster will attempt to recovery it.
   *  However, if the number of attempt is up to the threshold,
   *  then stop attempting to recovery it and fail it. */
  String BC_BSP_JOB_RECOVERY_ATTEMPT_MAX = "bcbsp.recovery.attempt.max";
  /** State bcbsp max staff attempt recovery */
  String BC_BSP_STAFF_RECOVERY_ATTEMPT_MAX = "bcbsp.staff.recovery.attempt.max";
  /** State bcbsp checkpoint type. */
  String BC_BSP_CHECKPOINT_TYPE = "bcbsp.checkpoint.type";
  /** State bcbsp checkpoint write path,
   * The directory used for checkpoint. */
  String BC_BSP_CHECKPOINT_WRITEPATH = "bcbsp.checkpoint.dir";
  /** State bcbsp recovery read path */
  String BC_BSP_RECOVERY_READPATH = "bcbsp.checkpoint.dir";
  /** Sttae bcbsp memory data percent */
  String BC_BSP_MEMORY_DATA_PERCENT = "bcbsp.memory.data.percent";
  /** State bcbsp job writePartition class */
  String USER_BC_BSP_JOB_WRITEPARTITION_CLASS = "job.writepartition.class";
  /** State bcbsp job partitioner class */
  String USER_BC_BSP_JOB_PARTITIONER_CLASS = "job.partitioner.class";
  /** State bcbsp job record parse class */
  String USER_BC_BSP_JOB_RECORDPARSE_CLASS = "job.recordparse.class";
  /** State Whether or not bcbsp job is divided */
  String USER_BC_BSP_JOB_ISDIVIDE = "job.isdivide";
  /** State bcbsp job send thread number */
  String USER_BC_BSP_JOB_SENDTHREADNUMBER = "job.partition.sendthreadnumber";
  /** State bcbsp job default send thread number */
  int USER_BC_BSP_JOB_SENDTHREADNUMBER_DEFAULT = 10;
  /** State bcbsp job the size of total cache */
  String USER_BC_BSP_JOB_TOTALCACHE_SIZE = "job.writepartition.totalcache.size";
  /** State bcbsp job the default size of total cache */
  int USER_BC_BSP_JOB_TOTALCACHE_SIZE_DEFAULT = 10;
  /** State bcbsp job balance factor */
  String USER_BC_BSP_JOB_BALANCE_FACTOR = "job.writepartition.balance.factor";
  /** State bcbsp job default balance factor */
  float USER_BC_BSP_JOB_BALANCE_FACTOR_DEFAULT = 0.01f;
  /** State bcbsp job jar */
  String USER_BC_BSP_JOB_JAR = "job.jar";
  /** State bcbsp job name */
  String USER_BC_BSP_JOB_NAME = "job.name";
  /** State bcbsp job user name */
  String USER_BC_BSP_JOB_USER_NAME = "job.user.name";
  /** State bcbsp job work class */
  String USER_BC_BSP_JOB_WORK_CLASS = "job.work.class";
  /** State bcbsp job input format class */
  String USER_BC_BSP_JOB_INPUT_FORMAT_CLASS = "job.inputformat.class";
  /** State bcbsp job the size of spilt */
  String USER_BC_BSP_JOB_SPLIT_SIZE = "job.input.split.size";
  /** State bcbsp job output format class */
  String USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS = "job.outputformat.class";
  /** State bcbsp job the names of aggregate */
  String USER_BC_BSP_JOB_AGGREGATE_NAMES = "job.aggregate.names";
  /** State bcbsp job the number of aggregate */
  String USER_BC_BSP_JOB_AGGREGATE_NUM = "job.aggregate.num";
  /** State bcbsp job working directory */
  String USER_BC_BSP_JOB_WORKING_DIR = "job.working.dir";
  /** State bcbsp job the frequency of checkpoint */
  String USER_BC_BSP_JOB_CHECKPOINT_FREQUENCY = "job.user.checkpoint.frequency";
  /** State bcbsp job the max of superstep */
  String USER_BC_BSP_JOB_SUPERSTEP_MAX = "job.user.superstep.max";
  /** State bcbsp job the number of staff */
  String USER_BC_BSP_JOB_STAFF_NUM = "job.staff.num";
  /** State bcbsp job priority */
  String USER_BC_BSP_JOB_PRIORITY = "job.priority";
  /** State bcbsp job partition type */
  String USER_BC_BSP_JOB_PARTITION_TYPE = "job.partition.type";
  /** State bcbsp job the number of partition */
  String USER_BC_BSP_JOB_PARTITION_NUM = "job.partition.num";
  /** State bcbsp job input directory */
  String USER_BC_BSP_JOB_INPUT_DIR = "job.input.dir";
  /** State bcbsp job output directory */
  String USER_BC_BSP_JOB_OUTPUT_DIR = "job.output.dir";
  /** State bcbsp job spilt file */
  String USER_BC_BSP_JOB_SPLIT_FILE = "job.split.file";
  /** State bcbsp job spilt factor */
  String USER_BC_BSP_JOB_SPLIT_FACTOR = "job.split.factor";
  /** State bcbsp job combiner class */
  String USER_BC_BSP_JOB_COMBINER_CLASS = "job.combiner.class";
  /** State bcbsp job graphdata class */
  String USER_BC_BSP_JOB_GRAPHDATA_CLASS = "job.graphdata.class";
  /** State bcbsp job combiner define flag */
  String USER_BC_BSP_JOB_COMBINER_DEFINE_FLAG = "job.combiner.define.flag";
  /** State bcbsp job combiner receive flag */
  String USER_BC_BSP_JOB_COMBINER_RECEIVE_FLAG = "job.combiner.receive.flag";
  /** State bcbsp job send threshold */
  String USER_BC_BSP_JOB_SEND_THRESHOLD = "job.send.threshold";
  /** State bcbsp job send combine threshold */
  String USER_BC_BSP_JOB_SEND_COMBINE_THRESHOLD = "job.send.combine.threshold";
  /** State bcbsp receive combine threshold */
  String USER_BC_BSP_JOB_RECEIVE_COMBINE_THRESHOLD =
      "job.receive.combine.threshold";
  /** State bcbsp job the size of message pack */
  String USER_BC_BSP_JOB_MESSAGE_PACK_SIZE = "job.message.pack.size";
  /** State bcbsp job the max number of producer */
  String USER_BC_BSP_JOB_MAX_PRODUCER_NUM = "job.max.producer.number";
  /** State bcbsp job the max number of consumer */
  String USER_BC_BSP_JOB_MAX_CONSUMER_NUM = "job.max.consumer.number";
  /** State bcbsp job the percent of memory data */
  String USER_BC_BSP_JOB_MEMORY_DATA_PERCENT = "job.memory.data.percent";
  /** State bcbsp job memory beta */
  String USER_BC_BSP_JOB_MEMORY_BETA = "job.memory.beta";
  /** State bcbsp job the NO. of memory hashbucket */
  String USER_BC_BSP_JOB_MEMORY_HASHBUCKET_NO = "job.memory.hashbucket.number";
  /** State bcbsp job the version of graph data */
  String USER_BC_BSP_JOB_GRAPH_DATA_VERSION = "job.graph.data.version";
  /** State bcbsp job the version of message queue */
  String USER_BC_BSP_JOB_MESSAGE_QUEUES_VERSION = "job.message.queues.version";
  /** State bcbsp job vertex class */
  String USER_BC_BSP_JOB_VERTEX_CLASS = "job.vertex.class";
  /** State bcbsp job edge class */
  String USER_BC_BSP_JOB_EDGE_CLASS = "job.edge.class";
  /** State bcbsp job message class */
  String USER_BC_BSP_JOB_MESSAGE_CLASS = "job.message.class";

  /** State the name of input table. */
  String USER_BC_BSP_JOB_HBASE_INPUT_TABLE_NAME = "hbase.mapreduce.inputtable";
  /** State the name of output table. */
  String USER_BC_BSP_JOB_HBASE_OUTPUT_TABLE_NAME = "hbase.outputtable";
  /** State bcbsp job the name of titan table name */
  String USER_BC_BSP_JOB_TITAN_INPUT_TABLE_NAME = "titan.input.table.name";
  /** State bcbsp job the name of titan hbase input table */
  String USER_BC_BSP_JOB_TITAN_HBASE_INPUT_TABLE_NAME = "hbase.table.name";
  /** State bcbsp job the name of titan output table name */
  String USER_BC_BSP_JOB_TITAN_OUTPUT_TABLE_NAME = "titan.output.table.name";
  /** State bcbsp job the name of titan hbase output table */
  String USER_BC_BSP_JOB_TITAN_HBASE_OUTPUT_TABLE_NAME = "hbase.table.name";
  /** State bcbsp job the number of outgoing edge */
  String USER_BC_BSP_JOB_OUTGOINGEDGE_NUM = "job.outgoingedge.num";
  /** State the default number of outgoing edge */
  long USER_BC_BSP_JOB_OUTGOINGEDGE_NUM_DEFAULT = Long.MAX_VALUE;
  /** State the value of adjust threshold0 */
  String DEFAULT_BC_BSP_JOB_ADJUST_THRESHOLD0 = "bcbsp.adjust.threshold0";
  /** State the value of adjust threshold1 */
  String DEFAULT_BC_BSP_JOB_ADJUST_THRESHOLD1 = "bcbsp.adjust.threshold1";
  /** State the value of adjust threshold2 */
  String DEFAULT_BC_BSP_JOB_ADJUST_THRESHOLD2 = "bcbsp.adjust.threshold2";
  /** State the value of adjust threshold3 */
  String DEFAULT_BC_BSP_JOB_ADJUST_THRESHOLD3 = "bcbsp.adjust.threshold3";
  /**Administrator email address*/
  String DEFAULT_BC_BSP_JOB_EMAILSENDER_ADDRESS = "bcbsp.email.address";
  /**Administrator email password*/
  String DEFAULT_BC_BSP_JOB_EMAIL_PASSWORD = "bcbsp.email.password";
  /**User Email Address*/
  String DEFAULT_BC_BSP_JOB_USER_EMAIL_ADDRESS = "bcbsp.useremail.address";
  /**email send open flag*/
  String DEFAULT_BC_BSP_JOB_EMAIL_SEND_FLAG = "bcbsp.emailsend.flag" ;
  /** State the type of bcbsp job */
  String USER_BC_BSP_JOB_TYPE = "job.type";
  /** State the C type of bcbsp job */
  String USER_BC_BSP_JOB_TYPE_C = "C++";
  /** State bcbsp job exe file */
  String USER_BC_BSP_JOB_EXE = "job.exe";

  /**
   *  State of vertex or partition bcbsp job.
   *  "0" means vertex-centre compute for graph; 
   *  "1" means partition-centre compute for machine-learning.
   */
  String USER_BC_BSP_COMPUTE_TYPE = "bcbsp.compute.type";

  String USER_BC_BSP_JOB_KVPAIR_CLASS = "job.kvpair.class";
  String USER_BC_BSP_JOB_KEY_CLASS = "job.key.class";
  String USER_BC_BSP_JOB_VALUE_CLASS = "job.value.class";

  int PEER_COMPUTE_BUCK_ID = 0;
  int DEFAULT_PEER_DST_MESSSAGE_ID = 0;
  int PEER_COMPUTE = 1;
  
  /**
   * Constants for BC-BSP Utility
   */

  /** State UTF-8 encoding */
  String UTF8_ENCODING = "UTF-8";
  /** State spilt flag */
  String SPLIT_FLAG = ":";
  /** State key_value spilt flag */
  String KV_SPLIT_FLAG = "\t";
  /** State space split  flag */
  String SPACE_SPLIT_FLAG = " ";
  /** State super step command split flag */
  String SSC_SPLIT_FLAG = "#";
  /** State empty byte array */
  byte[] EMPTY_BYTE_ARRAY = new byte[0];

  /**
   *  added for loadBalance
   */

  /** State message split flag */
  String MESSAGE_SPLIT = "@";
  /** State migrate split flag */
  String MIGRATE_SPLIT = "/";



  /**
   * Constants for BC-BSP Disk Cache
   */

  /** State the number of graph bitmap bucket bytes  */
  int GRAPH_BITMAP_BUCKET_NUM_BYTES = 320;
  /** State the header file of graph bucket */
  String GRAPH_BUCKET_FILE_HEADER = "[Graph data hash bucket]";
  /** State the header file of message bucket */
  String MSG_BUCKET_FILE_HEADER = "[Message data hash bucket]";
  /** State the header file of message queue */
  String MSG_QUEUE_FILE_HEADER = "[Message data queue]";
  /** State default bcbsp job estimate process factor value */
  long USER_BC_BSP_JOB_ESTIMATEPROCESS_FACTOR_DEFAULT = 7L;

  /**
   * Complicated Constants
   *
   *
   */


  public static class PRIORITY {
    /** State priority ---lower */
    public static final String LOWER = "5";
    /** State priority ---low */
    public static final String LOW = "4";
    /** State priority ---normal */
    public static final String NORMAL = "3";
    /** State priority ---high */
    public static final String HIGH = "2";
    /** State priority ---higher */
    public static final String HIGHER = "1";
  }

  /**
   *
   * State partition type
   *
   */
  public static class PARTITION_TYPE {
    /** State partition type --hash */
    public static final String HASH = "hash";
    /** State partition type --range */
    public static final String RANGE = "range";
  }

/**
 *
 * State command type
 *
 */
  public static class COMMAND_TYPE {
    /** State command type --start */
    public static final int START = 1;
    /** State command type --stop */
    public static final int STOP = 2;
    /** State command type --start and checkpoint */
    public static final int START_AND_CHECKPOINT = 3;
    /** State command type --start and recovery */
    public static final int START_AND_RECOVERY = 4;
  }

  /**
   *
   * State superstep stage
   *
   */
  public static class SUPERSTEP_STAGE {
    /** State superstep stage -- schedule stage */
    public static final int SCHEDULE_STAGE = 1;
    /** State superstep stage -- load data stage */
    public static final int LOAD_DATA_STAGE = 2;
    /** State superstep stage -- first stage */
    public static final int FIRST_STAGE = 3;
    /** State superstep stage -- second stage */
    public static final int SECOND_STAGE = 4;
    /** State superstep stage -- write checkpoint stage */
    public static final int WRITE_CHECKPOINT_SATGE = 5;
    /** State superstep stage -- read checkpoint stage */
    public static final int READ_CHECKPOINT_STAGE = 6;
    /** State superstep stage -- save result stage */
    public static final int SAVE_RESULT_STAGE = 7;
  }

  /**
   *
   * State staff status
   *
   */
  public static class SATAFF_STATUS {
    /** State staff status --running */
    public static final int RUNNING = 1;
    /** State staff status --fault */
    public static final int FAULT = 2;
    /** State staff status --succeed */
    public static final int SUCCEED = 3;
  }

}
