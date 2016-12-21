
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPBlockLocation;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * BSPBlockLocationImpl A concrete class that implements
 *  interface BSPBlockLocation.
 *
 */
public class BSPBlockLocationImpl implements BSPBlockLocation {
  /**State a BlockLocation[] type of variable blkLocations */
  private BlockLocation[] blkLocations;

  /**
   * constructor
   * @param file
   *         the given file
   * @param job
   *        BSPJob
   * @param start
   *        offset
   * @param length
   *        size of portions of the given file
   * @throws IOException
   */
  public BSPBlockLocationImpl(FileStatus file, BSPJob job, long start,
      long length) throws IOException {
    Path path = file.getPath();
    FileSystem fs = path.getFileSystem(job.getConf());
    blkLocations = fs.getFileBlockLocations(file, start, length);
  }

  @Override
  public BlockLocation[] getBlkLocations() {
    return blkLocations;
  }

  @Override
  public BlockLocation getBlkLocations(int i) {
    return blkLocations[i];
  }

  @Override
  public int getBlockIndex(BlockLocation[] blkLocations, long offset) {
    for (int i = 0; i < blkLocations.length; i++) {

      if ((blkLocations[i].getOffset() <= offset) && (offset < blkLocations[i]
          .getOffset() + blkLocations[i].getLength())) {
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length - 1];
    long fileLength = last.getOffset() + last.getLength() - 1;
    throw new IllegalArgumentException("Offset " + offset +
        " is outside of file (0.." + fileLength + ")");
  }

}
