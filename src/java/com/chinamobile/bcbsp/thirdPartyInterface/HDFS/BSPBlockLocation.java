
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import org.apache.hadoop.fs.BlockLocation;
/**
 *
 * BSPBlockLocation An interface that encapsulates BlockLocation.
 *
 */
public interface BSPBlockLocation {

  /**
   * getter method
   * @return
   *       BlockLocation[]
   */
  BlockLocation[] getBlkLocations();

  /**
   * get the Block Index
   * @param blkLocations
   *        BlockLocation array
   * @param offset
   *        offset length
   * @return
   *        the Block Index
   */
  int getBlockIndex(BlockLocation[] blkLocations, long offset);

  /**
   * get the Block Location
   * @param i
   *        int
   * @return
   *        Block Location
   */
  BlockLocation getBlkLocations(int i);

}
