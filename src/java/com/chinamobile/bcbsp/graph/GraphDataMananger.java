/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chinamobile.bcbsp.graph;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContext;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.GraphStaffHandler;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * Manage Graph data, include vertex and edge data with their managers.
 * @author Liu Jinpeng.
 */
public class GraphDataMananger implements GraphDataInterface {
 
  /** class logger. */
  public static final Log LOG = LogFactory.getLog(GraphDataMananger.class);
  /** handle of staff. */
  private Staff staff;
  /** vertex manager. */
  private VertexManager vManager;
  /** edge manager. */
  private EdgeManager eManager;
  /** vertex class. */
  private Class<? extends Vertex<?, ?, ?>> vertexClass;
  /** edge class. */
  private Class<? extends Edge<?, ?>> edgeClass;
  /** last superstep counter. */
  private int lastSuperstepCounter = 0;
  /** vertex size*/
  private int vertexSize = 0;
  /** edge list. */
  private ArrayList<Edge> edgelist;
  /**flag for get loaded graphdata*/
  private boolean migrateStaffFlag = false;
  /**flag for get recovery checkpoint vertex*/
  private boolean recoveryStaffFlag = false;
  //biyahui added
  private CommunicatorInterface communicator;
  //ljn add 1011
 
  //ljn add 1011
  private long activeCounter = 0;
//  /** vertex list*/
//  private ArrayList<Vertex> vertexlist;
// /** Edge list */
//  private List<PREdgeLiteNew> edgesList = new ArrayList<PREdgeLiteNew>();

  
  @Override
  public void initialize() {
    // Note Initialize The Job Working Directory For Vertices.
    String jobId = staff.getJobID().toString();
    int partitionId = staff.getPartition();
    MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER = staff.getConf()
        .getHashBucketNumber();
    MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR = MetaDataOfGraph.BCBSP_DISKGRAPH_BASEDIR
        + "/" + jobId + "/Partition-" + partitionId;
    vertexClass = staff.getConf().getVertexClass();
    edgeClass = staff.getConf().getEdgeClass();
    // Note Initialize VertexManager And EdgeManager
    this.vManager = new VertexManager();
    this.eManager = new EdgeManager(this.edgeClass);
    // Initialize The Statistic Of Graph, Namely Count Of Vertices And Edges.
    MetaDataOfGraph.BCBSP_GRAPH_VERTEXNUM = 0;
    MetaDataOfGraph.BCBSP_GRAPH_EDGENUM = 0;
    MetaDataOfGraph.initStatisticsPerBucket();
  }
  
  // Note Multiple Thread Used Method While Loading Data.
  @Override
  public synchronized void addForAll(Vertex vertex) {
    // Note Handle Vertex Information And Its Edges Information Independently.
    MetaDataOfGraph.BCBSP_GRAPH_VERTEXNUM++;
    MetaDataOfGraph.BCBSP_GRAPH_EDGENUM += vertex.getAllEdges().size();
    this.vManager.processVertexSaveForAll(vertex);
    this.eManager.processEdgeSaveForAll(vertex);
    // ljn add 1011
    activeCounter = 1;
  }
  
  @Override
  public void finishAdd() {
    this.vManager.finishVertexLoad();
    this.eManager.finishEdgesLoad();
    LOG.info("<<<<<<<Finsh Load GraphData>>>>>> <Vetices> "
        + MetaDataOfGraph.BCBSP_GRAPH_VERTEXNUM + "<Edges>"
        + MetaDataOfGraph.BCBSP_GRAPH_EDGENUM);
    printBucketInfo();
  }
  
  /** Show the information of bucket. */
  private void printBucketInfo() {
    LOG.info("==================Bucket MetaInfo Begin==================");
    MetaDataOfGraph.LogStatisticsPerBucket(LOG);
    LOG.info("==================Bucket MetaInfo End==================");
  }
  
  // Note Local Computing Procedure.
  @Override
  public void processingByBucket(GraphStaffHandler graphStaffHandler, BSP bsp,
      BSPJob job, int superStepCounter, BSPStaffContext context)
      throws IOException {
    try {
      // Note Record Super Step For Final Saving.
      this.lastSuperstepCounter = superStepCounter;
      //Vertex v = vertexClass.newInstance();
      // Traverse the buckets and processing every vertex in every bucket. 
      activeCounter = 0;
      //Biyahui added
      BSPConfiguration bspConf=new BSPConfiguration();
      int checkPointFrequency=bspConf.getInt(Constants.DEFAULT_BC_BSP_JOB_CHECKPOINT_FREQUENCY, 0);
      context.setCheckPointFrequency(checkPointFrequency);
      for (int i = (MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER - 1); i >= 0; i--) {
        int counter = MetaDataOfGraph.VERTEX_NUM_PERBUCKET[i];
        if (counter == 0) {
          continue;
        }
        if(this.migrateStaffFlag == false && this.recoveryStaffFlag != true){
        this.prepareBucket(i, superStepCounter);
        }
        if(this.migrateStaffFlag == true){
        this.prepareBucketForMigrate(i, superStepCounter, migrateStaffFlag);
        }
        if(this.recoveryStaffFlag == true){
        	this.prepareBucketForRecovery(i, superStepCounter, recoveryStaffFlag);
        }
        //biyahui revised
        if(this.recoveryStaffFlag==true){
        	graphStaffHandler.preBucketForRecovery(i,job,context);
        }else{
        	graphStaffHandler.preBucket(i, superStepCounter);
        }
        //graphStaffHandler.preBucket(i, superStepCounter);//this will combine the bucket messages
        //ljn activeCounter = 0;
        for (int j = 0; j < counter; j++) {
          Vertex v = vertexClass.newInstance();
          fillVertex(v);
          boolean activeFlag = v.isActive();
          graphStaffHandler.vertexProcessing(v, bsp, job, superStepCounter,
              context, activeFlag);
          v.setActive(context.getActiveFLag());
          this.vManager.processVertexSave(v);
          this.set(j, context.getVertex(), context.getActiveFLag());
        }
        this.finishPreparedBucket();
      }
    } catch (InstantiationException e) {
      throw new RuntimeException("[Graph Data Manager] process by bucket", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("[Graph Data Manager] process by bucket", e);
    }
  }
  
  private void prepareBucketForRecovery(int bucket, int superStepCounter,
		boolean recoveryStaffFlag) {
	// TODO Auto-generated method stub
	    this.vManager.prepareBucketForRecovery(bucket, superStepCounter, recoveryStaffFlag);
	    this.eManager.prepareBucket(bucket, superStepCounter);
	
}

/**
   * Prepare the vertex and edge buckets.
   * @param bucket
   * @param superstep
   */
  private void prepareBucket(int bucket, int superstep) {
    this.vManager.prepareBucket(bucket, superstep);
    this.eManager.prepareBucket(bucket, superstep);
  }
  
  /**
   * Finish preparing the vertex and edge buckets.
   * @param bucket
   * @param superstep
   */
  private void finishPreparedBucket() {
    this.vManager.finishPreparedBucket();
    this.eManager.finishPreparedBucket();
  }
  
  private void fillVertex(Vertex v) {
    this.vManager.processVertexLoad(v);
    /*added by Feng for loadbalance*/
    this.vertexSize = v.intoString().getBytes().length;
    // Note Ever Problemed/**./
    this.eManager.processEdgeLoad(v);
  }
  
  // Note Staff Handler.
  @Override
  public void setStaff(Staff staff) {
    this.staff = staff;
  }
  
  // Note None-Really-Used To Intend not Make Side Effect To Program.
  @Override
  public long getActiveCounter() {
	  return this.activeCounter;
  }
  
  // NOte None-Really-Used
  @Override
  public boolean getActiveFlagForAll(int index) {
    return true;
  }
  
  // Note This Method Is Used While Saving Result At The End Of The Running
  // Staff.Clean Up The Disk File Directory.
  @Override
  public void clean() {
    File tmpWorkDir = new File(MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR);
    if (!tmpWorkDir.exists()) {
      return;
    }
    deleteDirs(tmpWorkDir);
    // Note Add 2014-01-16 For Delete The Top-Level Job Directory
    tmpWorkDir = tmpWorkDir.getParentFile();
    tmpWorkDir.delete();
    activeCounter = 0;
  }
  
  /** Recurse Directory Delete. */
  private void deleteDirs(File dir) {
    if (dir.isFile()) {
      dir.delete();
      return;
    }
    // Note dir Must Be Direcotry.
    File[] childDirs = dir.listFiles();
    int len = childDirs.length;
    for (int i = 0; i < len; i++) {
      deleteDirs(childDirs[i]);
    }
    dir.delete();
  }
  
  @Override
  public int sizeForAll() {
    return MetaDataOfGraph.BCBSP_GRAPH_VERTEXNUM;
  }
  
  @Override
  public void saveAllVertices(GraphStaffHandler graphStaffHandler,
      RecordWriter output) throws IOException, InterruptedException {
    try {
      for (int i = (MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER - 1);
          i >= 0; i--) {
        int counter = MetaDataOfGraph.VERTEX_NUM_PERBUCKET[i];
        if (counter == 0) {
          continue;
        }
        this.prepareBucket(i, this.lastSuperstepCounter + 1);
        for (int j = 0; j < counter; j++) {
	      Vertex v = vertexClass.newInstance();
          fillVertex(v);
          graphStaffHandler.saveResultOfVertex(v, output);
          this.vManager.processVertexSave(v);
        }
        this.finishPreparedBucket();
      }
    } catch (InstantiationException e) {
      throw new RuntimeException("[Graph Data Manager] saveAllVertices", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("[Graph Data Manager] saveAllVertices", e);
    }
  }
  
  // Note For CheckPoint Write Graph Data.
  @Override
  public void saveAllVertices(RecordWriter output) throws IOException,
      InterruptedException {
    try {
      //Edge edge = edgeClass.newInstance();
      for (int i = (MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER - 1);
          i >= 0; i--) {
        int counter = MetaDataOfGraph.VERTEX_NUM_PERBUCKET[i];
        if (counter == 0) {
          continue;
        }
        this.prepareBucket(i, this.lastSuperstepCounter + 1);
        for (int j = 0; j < counter; j++) {
		  Vertex v = vertexClass.newInstance();
          fillVertex(v);
          this.edgelist = (ArrayList<Edge>) v.getAllEdges();
         StringBuffer outEdges = new StringBuffer();
         for (Edge edge : this.edgelist) {
             outEdges.append(edge.getVertexID() + Constants.SPLIT_FLAG
                 + edge.getEdgeValue() + Constants.SPACE_SPLIT_FLAG);
           }
           if (outEdges.length() > 0) {
             int k = outEdges.length();
             outEdges.delete(k - 1, k - 1);
           }
           
          output.write(new Text(v.getVertexID() + Constants.SPLIT_FLAG
                  + v.getVertexValue()), new Text(outEdges.toString()));
          this.vManager.processVertexSave(v);
        }
        this.finishPreparedBucket();
      }
    } catch (InstantiationException e) {
      throw new RuntimeException("[Graph Data Manager] saveAllVertices", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("[Graph Data Manager] saveAllVertices", e);
    }
  }
  
  @Override
  public void saveAllVertices(RecordWriter output,boolean writeEdgeFlag) throws IOException,
      InterruptedException {
    try {
      Vertex v = vertexClass.newInstance();
      //Edge edge = edgeClass.newInstance();
      for (int i = (MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER - 1);
          i >= 0; i--) {
        int counter = MetaDataOfGraph.VERTEX_NUM_PERBUCKET[i];
        if (counter == 0) {
          continue;
        }
        this.prepareBucket(i, this.lastSuperstepCounter + 1);
        for (int j = 0; j < counter; j++) {
          fillVertex(v);
          this.edgelist = (ArrayList<Edge>) v.getAllEdges();
         StringBuffer outEdges = new StringBuffer();
         for (Edge edge : this.edgelist) {
             outEdges.append(edge.getVertexID() + Constants.SPLIT_FLAG
                 + edge.getEdgeValue() + Constants.SPACE_SPLIT_FLAG);
           }
           if (outEdges.length() > 0) {
             int k = outEdges.length();
             outEdges.delete(k - 1, k - 1);
           }
         if(writeEdgeFlag==false){
          output.write(new Text(v.getVertexID() + Constants.SPLIT_FLAG
                  + v.getVertexValue()), new Text(outEdges.toString()));
         }else{
        	 output.write(new Text(v.getVertexID() + Constants.SPLIT_FLAG
                     + v.getVertexValue()));
         }
          this.vManager.processVertexSave(v);
        }
        this.finishPreparedBucket();
      }
    } catch (InstantiationException e) {
      throw new RuntimeException("[Graph Data Manager] saveAllVertices", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("[Graph Data Manager] saveAllVertices", e);
    }
  }
  
  // Note Not Used In New Version

  @Override
  public Vertex get(int index) {
    // TODO Auto-generated method stub
	  return null;
   
  }
  
  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  //ljn add 1011
  public void set(int index, Vertex vertex, boolean activeFlag) {
	if(lastSuperstepCounter == 0){
		activeCounter = 1;
	} else if(activeFlag){
		activeCounter++;
	}
  }
  
  @Override
  public void showMemoryInfo() {
    // TODO Auto-generated method stub
  }
  
  @Override
  public int getEdgeSize() {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public Vertex getForAll(int index) {
	  
//	  Vertex v = vertexClass.newInstance();
//  for (int i = (MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER - 1); i >= 0; i--) {
//      // LOG.info("<<$$$$$Processed Bucket>> " +i);
//      int counter = MetaDataOfGraph.VERTEX_NUM_PERBUCKET[i];
//      if (counter == 0) {
//        continue;
//      }
//      //this.prepareBucket(i, superStepCounter);
//      //graphStaffHandler.preBucket(i, superStepCounter);
//      for (int j = 0; j < counter; j++) {
//        fillVertex(v);
//        this.vertexlist.add(v);
//        //graphStaffHandler.vertexProcessing(v, bsp, job, superStepCounter,
//          //  context, true);
//        this.vManager.processVertexSave(v);
//      }
//      this.finishPreparedBucket();
//    }
//    return this.vertexlist;
    return null;
  }
  public int getVertexSize(){
	  LOG.info("Get VertexSize, vertexSize is "+this.vertexSize);
	  return this.vertexSize;
  }
  @Override
  public void getAllVertex(GraphStaffHandler graphStaffHandler, CommunicatorInterface communicator, 
		  RecordWriter output) throws IOException,
    InterruptedException {
	// TODO Auto-generated method stub
	try{
	Vertex v = vertexClass.newInstance();
  for (int i = (MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER - 1); i >= 0; i--) {
      int counter = MetaDataOfGraph.VERTEX_NUM_PERBUCKET[i];
      if (counter == 0) {
        continue;
      }
      this.prepareBucket(i, this.lastSuperstepCounter + 1);
      graphStaffHandler.preBucket(i,this.lastSuperstepCounter + 1 );
      for (int j = 0; j < counter; j++) {
        fillVertex(v);
        
        //this.vertexlist.add(v);
        //graphStaffHandler.vertexProcessing(v, bsp, job, superStepCounter,
          //  context, true);
//      //Vertex<?, ?, Edge> vertex = graphData.getForAll(i);
      String vertexID = v.getVertexID().toString();
      //Iterator<IMessage> it = communicator.getMessageIterator(vertexID);
      ConcurrentLinkedQueue<IMessage> messages = communicator
      .getMessageQueue(String.valueOf(v.getVertexID()));
      if(messages.size()==0){
 
    	  continue;
      }
      Iterator<IMessage> messagesIter = messages.iterator();
      StringBuffer sb = new StringBuffer();
      //sb.append(vertexID + Constants.MESSAGE_SPLIT);
     while (messagesIter.hasNext()) {
        IMessage msg = messagesIter.next();
        String info = msg.intoString();
        if (info != null) {
         sb.append(info + Constants.SPACE_SPLIT_FLAG);
       }
     }
     if (sb.length() > 0) {
         int k = sb.length();
         sb.delete(k - 1, k - 1);
       }
     //sb.append("\n");
     output.write(new Text(v.getVertexID() + Constants.MESSAGE_SPLIT), new Text(sb.toString()));
    }

  //  OUT.close()
        this.vManager.processVertexSave(v);
      
      this.finishPreparedBucket();
    }
	} catch (InstantiationException e) {
	      throw new RuntimeException("[Graph Data Manageer] saveAllVertices", e);
	} catch (IllegalAccessException e) {
	      throw new RuntimeException("[Graph Data Manageer] saveAllVertices", e);
	    }
}
  @Override
    public void setMigratedStaffFlag(boolean flag){

	this.migrateStaffFlag = flag;
	  LOG.info("Migrated Staff Flag! "+this.migrateStaffFlag);
   }
  /**
   * Prepare the vertex and edge buckets.
   * @param bucket
   * @param superstep
   */
  private void prepareBucketForMigrate(int bucket, int superstep, boolean migratedFlag) {
    this.vManager.prepareBucketForMigrate(bucket, superstep, migratedFlag);
    this.eManager.prepareBucket(bucket, superstep);
  }

@Override
public void setRecovryFlag(boolean recovery) {
	// TODO Auto-generated method stub
	this.recoveryStaffFlag = recovery;
}
}
