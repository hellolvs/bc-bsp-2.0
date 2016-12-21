/**
 * CopyRight by Chinamobile
 *
 * WorkerManagerRunner.java
 */

package com.chinamobile.bcbsp;

import com.chinamobile.bcbsp.util.SystemInfo;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WorkerManagerRunner This class starts and runs the WorkerManager. It is
 * relative with the shell command.
 *
 *
 *
 */
public class WorkerManagerRunner extends Configured implements Tool {

  /**
   * StartupShutdownPretreatment This class is used to do some prepare work
   * before starting WorkerManager and cleanup after shutting down
   * WorkerManager. For an example, kill all Staff Process before shutting down
   * the damean process.
   *
   *
   *
   */
  public class StartupShutdownPretreatment extends Thread {

    /** State Log */
    private final Log log;
    /** State host name */
    private final String hostName;
    /** State class name */
    private final String className;
    /** State workerManager */
    private WorkerManager workerManager;

    /**
     * constructor
     * @param clazz
     *        Class<?>
     * @param log
     *        org.apache.commons.logging.Log
     */
    public StartupShutdownPretreatment(Class<?> clazz,
        final org.apache.commons.logging.Log log) {
      this.log = log;
      this.hostName = getHostname();
      this.className = clazz.getSimpleName();

      this.log.info(toStartupShutdownMessage("STARTUP_MSG: ", new String[] {
        "Starting " + this.className, "  host = " + this.hostName,
        "  version = " + SystemInfo.getVersionInfo(),
        "  source = " + SystemInfo.getSourceCodeInfo(),
        "  compiler = " + SystemInfo.getCompilerInfo()}));
    }

    public void setHandler(WorkerManager workerManager) {
      this.workerManager = workerManager;
    }

    @Override
    public void run() {
      try {
        this.workerManager.shutdown();

        this.log.info(toStartupShutdownMessage("SHUTDOWN_MSG: ",
            new String[] {"Shutting down " + this.className + " at " +
                    this.hostName}));
      } catch (Exception e) {
      //  this.log.error("Shutdown Abnormally", e);
    	  throw new RuntimeException("Shutdown Abnormally", e);
      }
    }

    /**
     * get host name
     * @return
     *        host name
     */
    private String getHostname() {
      try {
        return "" + InetAddress.getLocalHost();
      } catch (UnknownHostException uhe) {
        return "" + uhe;
      }
    }

    /**
     * toString
     * @param prefix
     *         the initial contents of the buffer.
     * @param msg
     *         String[]
     * @return
     *        String
     */
    private String toStartupShutdownMessage(String prefix, String[] msg) {
      StringBuffer b = new StringBuffer(prefix);

      b.append("\n/*****************************" +
            "*******************************");
      for (String s : msg) {
        b.append("\n" + prefix + s);
        b.append("\n********************************" +
             "****************************/");
      }
      return b.toString();
    }
  }

  /** Define Log variable output messages */
  public static final Log LOG = LogFactory.getLog(WorkerManagerRunner.class);

  @Override
  public int run(String[] args) throws Exception {

    StartupShutdownPretreatment pretreatment = new StartupShutdownPretreatment(
        WorkerManager.class, LOG);

    if (args.length > 1) {
      System .out .println("usage: WorkerManagerRunner");
      System.exit(-1);
    }
    try {
      Configuration conf = new BSPConfiguration(getConf());
      if (args.length == 1) {
        conf.set(Constants.BC_BSP_HA_FLAG, args[0]);
      }
      WorkerManager workerManager = WorkerManager.constructWorkerManager(
          WorkerManager.class, conf);

      pretreatment.setHandler(workerManager);
      Runtime.getRuntime().addShutdownHook(pretreatment);
      WorkerManager.startWorkerManager(workerManager).join();
    } catch (Exception e) {
      LOG.fatal("Start Abnormally", e);
      return -1;
    }

    return 0;
  }

  /**
   * The main function
   * @param args
   *        a
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new WorkerManagerRunner(), args);
    System.exit(exitCode);
  }
}
