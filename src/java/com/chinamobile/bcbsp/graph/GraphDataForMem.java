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

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContext;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.GraphStaffHandler;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.mortbay.log.Log;

/**
 * GraphDataForMem implements GraphDataInterface for only memory storage of the
 * graph data.
 */
public class GraphDataForMem implements GraphDataInterface {
  
  /** the list of vertices in this partition. */
  private List<Vertex> vertexList = new ArrayList<Vertex>();
  /** the list of vertex active flag. */
  private List<Boolean> activeFlags = new ArrayList<Boolean>();
  /** Total number of vertices . */
  private int totalVerticesNum = 0;
  /** Total number of edges. */
  private int totalEdgesNum = 0;
  /** global index of graph data. */
  private int globalIndex = 0;
  
  @Override
  public synchronized void addForAll(Vertex vertex) {
    vertexList.add(vertex);
    activeFlags.add(true);
    this.totalEdgesNum = this.totalEdgesNum + vertex.getEdgesNum();
  }
  
  @Override
  public Vertex get(int index) {
    Vertex vertex = null;
    
    do {
      if (activeFlags.get(globalIndex)) {
        vertex = vertexList.get(globalIndex);
        break;
      }
      globalIndex++;
    } while (globalIndex < totalVerticesNum);
    
    return vertex;
  }
  
  @Override
  public Vertex getForAll(int index) {
    return vertexList.get(index);
  }
  
  @Override
  public void set(int index, Vertex vertex, boolean activeFlag) {
    this.vertexList.set(index, vertex);
    activeFlags.set(index, activeFlag);
    globalIndex++;
    if (globalIndex >= totalVerticesNum) {
      resetGlobalIndex();
    }
  }
  
  @Override
  public int size() {
    updateTotalVertices();
    return (int) getActiveCounter();
  }
  
  @Override
  public int sizeForAll() {
    updateTotalVertices();
    return totalVerticesNum;
  }
  
  @Override
  public void clean() {
    vertexList.clear();
    activeFlags.clear();
    this.totalEdgesNum = 0;
    updateTotalVertices();
    resetGlobalIndex();
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
  
  /** Reset the globalIndex as 0.*/
  private void resetGlobalIndex() {
    globalIndex = 0;
  }
  
  /** Update the total number of vertices.*/
  private void updateTotalVertices() {
    totalVerticesNum = this.vertexList.size();
  }
  
  @Override
  public boolean getActiveFlagForAll(int index) {
    return activeFlags.get(index);
  }
  
  @Override
  public void showMemoryInfo() {
  }
  
  @Override
  public int getEdgeSize() {
    return this.totalEdgesNum;
  }
  
  @Override
  public void processingByBucket(GraphStaffHandler graphStaffHandler, BSP bsp,
      BSPJob job, int superStepCounter, BSPStaffContext context)
      throws IOException {
    int tmpCounter = sizeForAll();
    Vertex vertex;
    boolean flag;
    for (int i = 0; i < tmpCounter; i++) {
      /** Clock */
      long tmpStart = System.currentTimeMillis();
      vertex = getForAll(i);
      if (vertex == null) {
        Log.info("[ERROR]Fail to get the HeadNode of index[" + i + "] "
            + "and the system will skip the record");
        continue;
      }
      flag = this.getActiveFlagForAll(i);
      graphStaffHandler.vertexProcessing(vertex, bsp, job, superStepCounter,
          context, flag);
      this.set(i, context.getVertex(), context.getActiveFLag());
    }
  }
  
  @Override
  public void saveAllVertices(GraphStaffHandler graphStaffHandler,
      RecordWriter output) throws IOException, InterruptedException {
    int tmpCounter = sizeForAll();
    Vertex vertex;
    for (int i = 0; i < tmpCounter; i++) {
      long tmpStart = System.currentTimeMillis();
      vertex = getForAll(i);
      if (vertex == null) {
        Log.info("[ERROR]Fail to save the HeadNode of index[" + i + "] "
            + "and the system will skip the record");
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
  public void setStaff(Staff staff) {
    // TODO Auto-generated method stub
  }
  
  @Override
  public void initialize() {
    // TODO Auto-generated method stub
  }

@Override
public int getVertexSize() {
	// TODO Auto-generated method stub
	return 0;
}

@Override
public void getAllVertex(GraphStaffHandler graphStaffHandler,
		CommunicatorInterface communicator,RecordWriter output) throws IOException,
		InterruptedException {
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

public void saveAllVertices(RecordWriter output,boolean writeEdgeFlag) throws IOException,
InterruptedException {

}
}
