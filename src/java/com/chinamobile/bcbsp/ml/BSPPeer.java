
package com.chinamobile.bcbsp.ml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspcontroller.Counters.Counter;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContext;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.GraphStaffHandler;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.partition.HashWritePartition;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

public class BSPPeer<K extends KeyInterface, V extends KeyInterface> implements
    GraphDataInterface, BSPPeerInterface {
  
  public static final Log LOG = LogFactory.getLog(BSPPeer.class);
  private Staff staff;
  /** vertex class. */
  private Class<? extends KeyValuePair<?, ?>> keyValuePairClass;
  /** last superstep counter. */
  private int currentSuperStepCounter = 0;
  /** pair size */
  private int totalPairNum = 0;
  /** Key Value data manager for this peer. */
  private ArrayList<KeyValuePair> keyValueManager = new ArrayList<KeyValuePair>();
  /** the ID for this peer. */
  private int peerID = -1;
  /***/
  private int index = 0;
  
  @Override
  public void initialize() {
    // Note Initialize The Job Working Directory For Vertices.
    String jobId = staff.getJobID().toString();
    int partitionId = staff.getPartition();
    keyValuePairClass = (Class<? extends KeyValuePair<?, ?>>) staff.getConf()
        .getKeyValuePairClass();
    peerID = staff.getPartition();
  }
  
  /*
   * Method for Graph compute interface.
   */
  
  @Override
  public final synchronized void addForAll(Vertex vertex) {
    KeyValuePair pair;
    try {
      pair = keyValuePairClass.newInstance();
      pair.setKey(vertex.getVertexID());
      pair.setEdgeToValue(vertex.getAllEdges());
      this.keyValueManager.add(pair);
      totalPairNum = keyValueManager.size();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void setStaff(Staff staff) {
    this.staff = staff;
  }
  
  @Override
  public int size() {
    // TODO Auto-generated method stub
    return totalPairNum;
  }
  
  @Override
  public int sizeForAll() {
    return totalPairNum;
  }
  
  /*
   * Method for BSP Peer compute.
   */
  
  @Override
  public final void processingByBucket(GraphStaffHandler graphStaffHandler,
      BSP bsp, BSPJob job, int superStepCounter, BSPStaffContext context)
      throws IOException {
    // Note Record Super Step For Final Saving.
    this.currentSuperStepCounter = superStepCounter;
    // process by vector
    graphStaffHandler.preBucket(Constants.PEER_COMPUTE_BUCK_ID,
        superStepCounter);// this will combine the bucket messages
    graphStaffHandler.peerProcessing(this, bsp, job, superStepCounter, context,
        true);
  }
  
  @Override
  public void resetPair() {
    this.index = 0;
  }
  
  @Override
  public final void saveAllVertices(GraphStaffHandler graphStaffHandler,
      RecordWriter output) throws IOException, InterruptedException {
    for (KeyValuePair pair : keyValueManager) {
      output.write(new Text(pair.getKey().toString()), new Text(pair.getValue()
          .toString()));
    }
    
  }
  
  @Override
  public final void saveAllVertices(RecordWriter output) throws IOException,
      InterruptedException {
    for (KeyValuePair pair : keyValueManager) {
      output.write(new Text(pair.getKey().toString()), new Text(pair.getValue()
          .toString()));
    }
    
    
  }
  @Override
  public int getSuperstepCount() {
    return currentSuperStepCounter;
  }
  
  @Override
  public String getPeerName() {
    return staff.getStaffAttemptId().toString();
  }
   
  @Override
  public String[] getAllPeerNames() {
    // TODO Auto-generated method stub
    return null;
  }
  
  public int getNumPeers() {
    return this.totalPairNum;
  }
 
  @Override
  public KeyValuePair<KeyInterface, KeyInterface> readNext()
      throws IOException, InstantiationException, IllegalAccessException {
    if (index >= totalPairNum) {
      throw new RuntimeException(new ArrayIndexOutOfBoundsException());
    }
    KeyValuePair currentKeyValuePair = this.keyValueManager.get(index);
    index++;
    return currentKeyValuePair;
  }
  
  @Override
  public BSPJob getConfiguration() {
    return staff.getConf();
  }
  
  @Override
  public StaffAttemptID getStaffId() {
    return staff.getStaffAttemptId();
  }
  
  public boolean hasMorePair() {
    if (totalPairNum <= 0) {
      LOG.error("NO key value pair for computing.");
    }
    if (this.index < this.totalPairNum) {
      return true;
    } else {
      return false;
    }
  }
  /*
   * Methods not implements in BSPPeerInterface (non-Javadoc)
   * 
   */
  @Override
  public void reopenInput() throws IOException {
    // TODO Auto-generated method stub
    
  }
  @Override
  public Counter getCounter(Enum<?> name) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public Counter getCounter(String group, String name) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void incrementCounter(Enum<?> key, long amount) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void incrementCounter(String group, String counter, long amount) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public long getSplitSize() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public long getPos() throws IOException {
    // TODO Auto-generated method stub
    return index;
  }
  
  @Override
  public int getNumCurrentMessages() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public int getEdgeSize() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public int getVertexSize() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public void setMigratedStaffFlag(boolean flag) {
    
  }
  
  @Override
  public void setRecovryFlag(boolean recovery) {
    
  }
  
  /*
   * Methods not implements in GraphdataInterface(non-Javadoc)
   */
  @Override
  public void getAllVertex(GraphStaffHandler graphStaffHandler,
      CommunicatorInterface communicator, RecordWriter output)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    
  } 
  @Override
  public Vertex getForAll(int index) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public boolean getActiveFlagForAll(int index) {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public void finishAdd() {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void clean() {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public long getActiveCounter() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public void showMemoryInfo() {
    // TODO Auto-generated method stub
  }
  
  @Override
  public Vertex get(int index) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void set(int index, Vertex vertex, boolean activeFlag) {
  }

  @Override
  public void saveAllVertices(RecordWriter output, boolean writeEdgeFlag)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    
  }  
}
