/**
 * CopyRight by Chinamobile
 * 
 * GraphDataForBDB.java
 */

package com.chinamobile.bcbsp.graph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContext;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.comm.BDBMap;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.GraphStaffHandler;
import com.chinamobile.bcbsp.examples.PRVertex;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * GraphDataForBDB implements GraphDataInterface for storage of the graph
 * data.Graph data manager for BerkeleyDB JE supported.
 * 
 * @author
 * @version
 */
public class GraphDataForBDB implements GraphDataInterface {
  
  private static final Log LOG = LogFactory.getLog(GraphDataForBDB.class);
  private File fileRoot;
  private File graphDataFile;
  @SuppressWarnings("unused")
  private BDBMap<Integer, String> headMap = null;
  private BDBList graphdata = null;
  private List<Boolean> activeFlags = new ArrayList<Boolean>();
  private int totalHeadNodes = 0;
  private int globalIndex = 0;
  private int edgeSize = 0;
  
  private BSPJobID jobID;
  private int partitionID;
  
  private Class<? extends Vertex<?, ?, ?>> vertexClass;
  @SuppressWarnings("unused")
  private Class<? extends Edge<?, ?>> edgeClass;
  
  private Staff staff;
  
  public void setStaff(Staff staff) {
    this.staff = staff;
  }
  
  public void initialize() {
    int partitionID = this.staff.getPartition();
    BSPJob job = staff.getConf();
    initialize(job, partitionID);
  }
  
  /**
   * @param job
   */
  public void initialize(BSPJob job, int partitionID) {
    
    this.jobID = job.getJobID();
    this.partitionID = partitionID;
    
    this.fileRoot = new File("/tmp/bcbsp/graphdata" + this.jobID.toString()
        + "/" + "partition-" + this.partitionID);
    this.graphDataFile = new File(this.fileRoot + "/" + "GraphData");
    if (!fileRoot.exists()) {
      fileRoot.mkdirs();
      
    }
    if (!graphDataFile.exists()) {
      graphDataFile.mkdirs();
    }
    
    graphdata = new BDBList(graphDataFile.toString(), "headnodes");
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void addForAll(Vertex vertex) {
    try {
      edgeSize += vertex.getEdgesNum();
      graphdata.push(vertex.intoString());
      activeFlags.add(true);
    } catch (IOException e) {
      LOG.error("[addForAll]", e);
    }
    
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Vertex get(int index) {// index's arrange is [0, headMap.size)
  
    @SuppressWarnings("unused")
    Vertex<?, ?, ?> tmpVertex = null;
    try {
      tmpVertex = this.vertexClass.newInstance();
    } catch (InstantiationException e) {
      LOG.error("[GraphDataForBDB] caught: ", e);
    } catch (IllegalAccessException e) {
      LOG.error("[GraphDataForBDB] caught: ", e);
    }
    PRVertex vertex = new PRVertex();
    String vertexValue;
    
    do {
      if (activeFlags.get(globalIndex)) {
        try {
          vertexValue = graphdata.getElement(index);
          vertex.fromString(vertexValue);
          break;
        } catch (Exception e) {
          LOG.error("[get]", e);
        }
        
      }
      globalIndex++;
    } while (globalIndex < totalHeadNodes);
    return vertex;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Vertex getForAll(int index) {// index's arrange is [0, headMap.size)
    PRVertex vertex = new PRVertex();
    try {
      vertex.fromString(graphdata.getElement(index));
    } catch (Exception e) {
      LOG.error("[getForAll]", e);
    }
    return vertex;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void set(int index, Vertex vertex, boolean activeFlag) {
    graphdata.setEelment(index, vertex.intoString());
    activeFlags.set(index, activeFlag);
    globalIndex++;
    if (globalIndex >= totalHeadNodes) {
      resetGlobalIndex();
    }
    
  }
  
  @Override
  public int size() {
    updateTotalHeadNodes();
    return (int) getActiveCounter();
  }
  
  @Override
  public int sizeForAll() {
    updateTotalHeadNodes();
    return totalHeadNodes;
  }
  
  @Override
  public void clean() {
    if (this.totalHeadNodes != 0)
      graphdata.clean();
  }
  
  @Override
  public void finishAdd() {
    // For memory-only version, do nothing.
  }
  
  @Override
  public long getActiveCounter() {
    long counter = 0;
    for (boolean flag : activeFlags) {
      if (flag) {
        counter++;
      }
    }
    
    return counter;
  }
  
  private void resetGlobalIndex() {
    globalIndex = 0;
  }
  
  private void updateTotalHeadNodes() {
    totalHeadNodes = (int) graphdata.size();
  }
  
  @Override
  public boolean getActiveFlagForAll(int index) {
    
    return activeFlags.get(index);
  }
  
  @Override
  public void showMemoryInfo() {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public int getEdgeSize() {
    return edgeSize;
  }
  
  @Override
  public void processingByBucket(GraphStaffHandler graphStaffHandler, BSP bsp,
      BSPJob job, int superStepCounter, BSPStaffContext context)
      throws IOException {
    int tmpCounter = sizeForAll();
    Vertex vertex;
    boolean activeFlag;
    for (int i = 0; i < tmpCounter; i++) {
      /** Clock */
      long tmpStart = System.currentTimeMillis();
      vertex = getForAll(i);
      if (vertex == null) {
        org.mortbay.log.Log.info("[ERROR]Fail to get the HeadNode of index["
            + i + "] " + "and the system will skip the record");
        continue;
      }
      activeFlag = this.getActiveFlagForAll(i);
      graphStaffHandler.vertexProcessing(vertex, bsp, job, superStepCounter,
          context, activeFlag);
      
      this.set(i, context.getVertex(), context.getActiveFLag());
    }
  }
  
  @Override
  public void saveAllVertices(GraphStaffHandler graphStaffHandler,
      RecordWriter output) throws IOException, InterruptedException {
    int tmpCounter = sizeForAll();
    Vertex vertex;
    for (int i = 0; i < tmpCounter; i++) {
      /** Clock */
      long tmpStart = System.currentTimeMillis();
      vertex = getForAll(i);
      if (vertex == null) {
        org.mortbay.log.Log.info("[ERROR]Fail to save the HeadNode of index["
            + i + "] " + "and the system will skip the record");
        continue;
      }
      graphStaffHandler.saveResultOfVertex(vertex, output);
    }
    
  }
  
  @Override
  public void saveAllVertices(RecordWriter output) throws IOException,
      InterruptedException {
    for (int i = 0; i < sizeForAll(); i++) {
      Vertex<?, ?, Edge> vertex = getForAll(i);
      StringBuffer outEdges = new StringBuffer();
      for (Edge edge : vertex.getAllEdges()) {
        outEdges.append(edge.getVertexID() + Constants.SPLIT_FLAG
            + edge.getEdgeValue() + Constants.SPACE_SPLIT_FLAG);
      }
      if (outEdges.length() > 0) {
        int j = outEdges.length();
        outEdges.delete(j - 1, j - 1);
      }
      output.write(new Text(vertex.getVertexID() + Constants.SPLIT_FLAG
          + vertex.getVertexValue()), new Text(outEdges.toString()));
    }
    
  }
  
  @Override
  public int getVertexSize() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public void getAllVertex(GraphStaffHandler graphStaffHandler,
      CommunicatorInterface communicator, RecordWriter output)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void setMigratedStaffFlag(boolean flag) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void setRecovryFlag(boolean recovery) {
    // TODO Auto-generated method stub
    
  }
  
  /** For JUnit test. */
  public void setActiveFlags(List<Boolean> activeFlags) {
    this.activeFlags = activeFlags;
  }
  
  public List<Boolean> getActiveFlags() {
    return activeFlags;
  }
  
  public void setEdgeSize(int edgeSize) {
    this.edgeSize = edgeSize;
  }
  
  public int getTotalHeadNodes() {
    return totalHeadNodes;
  }
  
  public void setTotalHeadNodes(int totalHeadNodes) {
    this.totalHeadNodes = totalHeadNodes;
  }
  
  public void saveAllVertices(RecordWriter output,boolean writeEdgeFlag) throws IOException,
  InterruptedException {

  }
  
}
