/**
 * CopyRight by Chinamobile
 * 
 * WritePartition.java
 */

package com.chinamobile.bcbsp.partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.pipes.Application;
import com.chinamobile.bcbsp.sync.StaffSSControllerInterface;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;

/**
 * WritePartition This abstract class is the primary interface for users to
 * define their own partition method.The user must provide a no-argument
 * constructor.
 * @author
 * @version 2.0
 */
public abstract class WritePartition {
  /** The WorkerAgentForStaff.*/
  protected WorkerAgentForStaffInterface workerAgent = null;
  /** The staff of the writepartition.*/
  protected BSPStaff staff = null;
  /** The partitioner of the writepartition.*/
  protected Partitioner<Text> partitioner = null;
  /** The recordparse of the writepartition.*/
  protected RecordParse recordParse = null;
  /** The separator of the vertexid and value.*/
  protected String separator = ":";
  /** The staff controller of the writepartition.*/
  protected StaffSSControllerInterface sssc;
  /** The SuperStepReportContainer of the writepartition.*/
  protected SuperStepReportContainer ssrc;
  /** cache size of the writepartition.*/
  protected int TotalCacheSize = 0;
  /** The Thread Num of the writepartition.*/
  protected int sendThreadNum = 0;
  /** The SerializationFactory of the writepartition.*/
  protected SerializationFactory serializationFactory =
      new SerializationFactory(new Configuration());
  /** The Key Serialization of the writepartition.*/
  protected Serializer<Text> keyserializer = serializationFactory
      .getSerializer(Text.class);
  /** The Value Serialization of the writepartition.*/
  protected Serializer<Text> valueserializer = serializationFactory
      .getSerializer(Text.class);
  /**
   * @param workAgent the workagent of the staff
   */
  public void setWorkerAgent(WorkerAgentForStaffInterface workAgent) {
    this.workerAgent = workAgent;
  }
  /**
   * @param bspstaff the staff owns the writepartition
   */
  public void setStaff(BSPStaff bspstaff) {
    this.staff = bspstaff;
  }
  /**
   * @param partition the partitioner of the writepartition
   */
  public void setPartitioner(Partitioner<Text> partition) {
    this.partitioner = partition;
  }
  /**
   * @param aSssc the staffscontroller for the writepartition
   */
  public void setSssc(StaffSSControllerInterface aSssc) {
    this.sssc = aSssc;
  }
  /**
   * @param aSsrc the SuperStepReportContainer of the writepartition
   */
  public void setSsrc(SuperStepReportContainer aSsrc) {
    this.ssrc = aSsrc;
  }
  /**
   * @param aSeparator the separator of the vertexid and value
   */
  public void setSeparator(String aSeparator) {
    this.separator = aSeparator;
  }
  /**
   * @param rp the recordparse of the writepartition
   */
  public void setRecordParse(RecordParse rp) {
    this.recordParse = rp;
  }
  /**
   * @param catchsize cache size of the writepartition
   */
  public void setTotalCatchSize(int catchsize) {
    this.TotalCacheSize = catchsize;
  }
  /**
   * @param sThreadNum the Thread Num of the writepartition
   */
  public void setSendThreadNum(int sThreadNum) {
    this.sendThreadNum = sThreadNum;
  }
  /**
   * This method is used to partition graph vertexes. Writing Each vertex to the
   * corresponding partition. In this method calls recordParse method to create
   * an HeadNode object. The last call partitioner's getPartitionId method to
   * calculate the HeadNode belongs to partition's id. If the HeadNode belongs
   * local partition then written to the local partition or send it to the
   * appropriate partition.
   * @param recordReader the recordreader of the split
   * @throws IOException the IO exception
   * @throws InterruptedException Interruputed exception
   */
  @SuppressWarnings("rawtypes")
  public abstract void write(RecordReader recordReader) throws IOException,
      InterruptedException;
  /**
   * This method is used to partition graph vertexes. Writing Each vertex to the
   * corresponding partition. In this method calls recordParse method to create
   * an HeadNode object. The last call partitioner's getPartitionId method to
   * calculate the HeadNode belongs to partition's id. If the HeadNode belongs
   * local partition then written to the local partition or send it to the
   * appropriate partition.(for c++)
   * @param recordReader the recordreader of the split
   * @param application the application of the writepartition
   * @param staffnum the number of the staff
   * @throws IOException the IO exception
   * @throws InterruptedException Interruputed exception
   */
  @SuppressWarnings("rawtypes")
  public abstract void write(RecordReader recordReader,
      Application application, int staffnum) throws IOException,
      InterruptedException;
}
