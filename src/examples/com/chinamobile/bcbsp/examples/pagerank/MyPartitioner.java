
package com.chinamobile.bcbsp.examples.pagerank;

import com.chinamobile.bcbsp.api.Partitioner;

import java.util.zip.CRC32;

import org.apache.hadoop.io.Text;

/**
 *
 * A class extends Partitioner<Text>.
 *
 */
class MyPartitioner extends Partitioner<Text> {

  /**
   * constructor
   */
  public MyPartitioner() {
  }

  /**
   * constructor
   * @param numPartition
   *        partition number
   */
  public MyPartitioner(int numPartition) {
    this.numPartition = numPartition;
  }

  @Override
  public int getPartitionID(Text url) {
    // TODO Auto-generated method stub
    CRC32 checksum = new CRC32();
    checksum.update(url.toString().getBytes());
    int crc = (int) checksum.getValue();
    long hashcode = (crc >> 16) & 0x7fff;
    return (int) (hashcode % this.numPartition);
  }
}
