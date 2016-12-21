
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPPath;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPPathImpl A concrete class that implements interface BSPPath.
 *
 */
public class BSPPathImpl implements BSPPath {
  /** State BSPController */
  private final BSPController controller;
  /** State BSPJob */
  private BSPJob jobConf;
  /** State path　*/
  private Path localJobFile = null;
  /** State path */
  private Path localJarFile = null;
  /** State path */
  private Path systemDir = null;
  /** State path */
  private Path path = null;

  /**
   *  constructor
   * @param controller
   *        BSPController
   * @param pathString
   *        String
   * @param i
   *        flag bit
   * @throws IOException
   */
  public BSPPathImpl(BSPController controller, String pathString, int i)
      throws IOException {
    this.controller = controller;
    if (i == 1) {
      this.localJobFile = controller.getLocalPath(pathString);
    } else {
      this.localJarFile = controller.getLocalPath(pathString);
    }
  }

  /**
   * constructor
   * @param conf
   *        Configuration
   * @param pathString
   *        String
   * @param staff
   *        Staff
   * @throws IOException
   */
  public BSPPathImpl(Configuration conf, String pathString, Staff staff)
      throws IOException {
    this.controller = null;

    BSPJob defaultJobConf = new BSPJob((BSPConfiguration) conf);
    localJarFile = defaultJobConf.getLocalPath(pathString);
  }

  /**
   * constructor
   * @param jobConf
   *        BSPJob
   * @param pathString
   *        String
   * @param task
   *        Staff
   * @throws IOException
   */
  public BSPPathImpl(BSPJob jobConf, String pathString, Staff task)
      throws IOException {
    this.controller = null;
    localJarFile = this.jobConf
        .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/" +
      task.getStaffID() + "/jobC");
  }

  /**
   * constructor
   * @param pathString
   *        String
   */
  public BSPPathImpl(String pathString) {
    this.controller = null;
    systemDir = new Path(pathString);

  }

  @Override
  public Path getSystemDir() {
    return systemDir;
  }

  @Override
  public Path makeQualified(FileSystem fs) {
    // TODO Auto-generated method stub
    return path.makeQualified(fs);
  }
}
