
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFsPermission;

import org.apache.hadoop.fs.permission.FsPermission;

/**
 *
 * BSPFspermissionImpl
 *  A concrete class that implements interface BSPFsPermission.
 *
 */
public class BSPFspermissionImpl implements BSPFsPermission {
  /** system directories are world-wide readable and owner readable rwx-wx-wx */
  static final FsPermission SYSTEM_DIR_PERMISSION = FsPermission
      .createImmutable((short) 0733);
  /** system files should have 700 permission rwx------ */
  static final FsPermission SYSTEM_FILE_PERMISSION = FsPermission
      .createImmutable((short) 0700);
  /** job submission directory is world readable/writable/executable
   * rwx-rwx-rwx */
  static final FsPermission JOB_DIR_PERMISSION = FsPermission
      .createImmutable((short) 0777);
  /** job files are world-wide readable and owner writable rw-r--r-- */
  private static final FsPermission JOB_FILE_PERMISSION = FsPermission
      .createImmutable((short) 0644);
  /** State a FsPermission type of variable fp */
  private FsPermission fp = null;


  /**
   * constructor
   * @param i
   *        flag bit
   */
  public BSPFspermissionImpl(int i) {
    if (i == 1) {
      fp = new FsPermission(SYSTEM_DIR_PERMISSION);
    } else if (i == 0) {
      fp = new FsPermission(JOB_FILE_PERMISSION);
    } else {
      fp = new FsPermission(JOB_DIR_PERMISSION);
    }
  }

  @Override
  public FsPermission getFp() {
    return fp;
  }


}
