
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import org.apache.hadoop.fs.permission.FsPermission;

/**
 *
 * BSPFsPermission An interface that encapsulates FsPermission
 *
 */
public interface BSPFsPermission {

  /**
   * getter method
   * @return
   *       FsPermission
   */
  FsPermission getFp();
}
