/**
 * CopyRight by Chinamobile
 *
 * BSPControllerRunner.java
 */

package com.chinamobile.bcbsp;

import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.util.SystemInfo;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * BSPControllerRunner This class starts and runs the BSPController. It is
 * relative with the shell command.
 *
 *
 *
 */
public class BSPControllerRunner extends Configured implements Tool {

  /**
   * StartupShutdownPretreatment This class is used to do some prepare work
   * before starting BSPController and cleanup after shutting down
   * BSPController.
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
    /** State BSPController */
    private BSPController bspController;

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

    public void setHandler(BSPController bspController) {
      this.bspController = bspController;
    }

    @Override
    public void run() {
      try {
        this.bspController.shutdown();

        this.log.info(toStartupShutdownMessage("SHUTDOWN_MSG: ",
            new String[] {"Shutting down " + this.className + " at " +
                   this.hostName}));
      } catch (Exception e) {
       // this.log.error("Shutdown Abnormally", e);
    	  throw new RuntimeException("Shutdown Abnormally", e);
      }
    }

    /**
     * get the host name
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
     *        the initial contents of the buffer.
     * @param msg
     *        String[]
     * @return
     *        String
     */
    private String toStartupShutdownMessage(String prefix, String[] msg) {
      StringBuffer b = new StringBuffer(prefix);

      b.append("\n/****************************************" +
              "********************");
      for (String s : msg) {
        b.append("\n" + prefix + s);
        b.append("\n******************************" +
                "******************************/");
      }
      return b.toString();
    }

  }

  /** Define Log variable output messages */
  public static final Log LOG = LogFactory.getLog(BSPControllerRunner.class);

  @Override
  public int run(String[] args) throws Exception {
    StartupShutdownPretreatment pretreatment = new StartupShutdownPretreatment(
        BSPController.class, LOG);

    if (args.length != 0) {
      System .out .println("usage: BSPControllerRunner");
      System.exit(-1);
    }

    try {
      BSPConfiguration conf = new BSPConfiguration(getConf());
      BSPController bspController = BSPController.startMaster(conf);
      pretreatment.setHandler(bspController);
      Runtime.getRuntime().addShutdownHook(pretreatment);
      bspController.offerService();
    } catch (Exception e) {
      LOG.fatal("Start Abnormally", e);
      return -1;
    }

    return 0;
  }

  /**
   * @param args
   *        a
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new BSPControllerRunner(), args);
    System.exit(exitCode);
  }
}
