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

import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

/**
 * Edge manager. Edges data default is on disk, with this can load edges data
 * from disk. After compute, edge data save into disk.
 */
public class EdgeManager implements Writable {
  /** class logger. */
  public static final Log LOG = LogFactory.getLog(EdgeManager.class);
  /** edge class. */
  private Class<? extends Edge<?, ?>> edgeClass;
  /** current size of edges. */
  private int currentEdgesLength = -1;
  /** edge list. */
  private ArrayList<Edge> edgelist;
  /** directory of edges on disk. */
  private String edgesDir;
  /** current edge. */
  private Edge currentEdge;
  // Note For Edge Reuse
  /** edge list pool. */
  private ArrayList<Edge> edgePool;
  /** size of edge pool. */
  private int edgePoolSize = 0;
  /** current edge bucket index. */
  private int currentBucketIndex = -1;
  /** BCBSP output Buffer:IO Handler. */
  private BCBSPSpillingOutputBuffer[] spillingBuffers;
  /** BCBSP input Buffer:IO Handler. */
  private BCBSPSpillingInputBuffer currentReader;
  
  /**
   * Constructor of EdgeManager.
   * @param edgeClass
   */
  public EdgeManager(Class<? extends Edge<?, ?>> edgeClass) {
    super();
    this.edgeClass = edgeClass;
    initialize();
    initEdgePool(30);
  }
  
  /** Initialize the edge manager. */
  private void initialize() {
    edgesDir = MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR + "/" + "Edges";
    File tmpFile;
    tmpFile = new File(edgesDir, "init");
    if (!tmpFile.exists()) {
      tmpFile.mkdirs();
    }
    spillingBuffers = new BCBSPSpillingOutputBuffer[MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER];
    for (int i = 0; i < MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER; i++) {
      try {
        spillingBuffers[i] = new BCBSPSpillingOutputBuffer(tmpFile.toString()
            + "/Bucket-" + i, MetaDataOfGraph.BCBSP_GRAPH_LOAD_INIT);
      } catch (IOException e) {
        throw new RuntimeException("[Edge Manageer] initialize :", e);
      }
    }
  }
  
  /**
   * Save all edges.
   * @param v
   */
  public void processEdgeSaveForAll(Vertex v) {
    int hashNumber = MetaDataOfGraph.localPartitionRule(String.valueOf(v
        .getVertexID()));
    this.edgelist = (ArrayList<Edge>) v.getAllEdges();
    this.currentEdgesLength = this.edgelist.size();
    MetaDataOfGraph.EDGE_NUM_PERBUCKET[hashNumber] += this.edgelist.size();
    try {
      this.write(this.spillingBuffers[hashNumber]);
    } catch (IOException e) {
      throw new RuntimeException(
          "[Edge Manageer] process edge and save all edges :", e);
    }
  }
  
  /** Close I/O Channel. */
  public void finishEdgesLoad() {
    try {
      for (int i = 0; i < MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER; i++) {
        spillingBuffers[i].close();
        spillingBuffers[i] = null;
      }
    } catch (IOException e) {
      throw new RuntimeException("[Edge Manageer] finishEdgesLoad :", e);
    }
  }
  
  /** Save all edges and edge is not changed. */
  public void processEdgeSave(Vertex v) {
  }
  
  /** Load the edges from disk. */
  public void processEdgeLoad(Vertex v) {
    this.edgelist = (ArrayList<Edge>) v.getAllEdges();
    if (this.currentReader == null) {
      // Note Not Null
      LOG.warn("I/O Null Pointer While Loading Edges <Bucket>"
          + this.currentBucketIndex);
      return;
    }
    try {
      this.readFields(this.currentReader);
    } catch (IOException e) {
      throw new RuntimeException("[Edge Manageer] processEdgeLoad :", e);
    }
  }
  
  /** Prepare the bucket before compute. */
  public void prepareBucket(int bucket, int superstep) {
    this.currentBucketIndex = bucket;
    File tmpFileIn;
    tmpFileIn = new File(edgesDir, "init");
    if (!tmpFileIn.exists()) {
      LOG.info("Edge Input File Not Exits Bucket NUM Is "
          + this.currentBucketIndex);
      return;
    }
    try {
      this.currentReader = new BCBSPSpillingInputBuffer(tmpFileIn.toString()
          + "/Bucket-" + bucket, MetaDataOfGraph.BCBSP_GRAPH_LOAD_READSIZE);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
          "[Edge Manageer] Prepare the bucket before compute :", e);
    } catch (IOException e) {
      throw new RuntimeException(
          "[Edge Manageer] Prepare the bucket before compute :", e);
    }
  }
  
  /** Finish prepare of bucket. */
  public void finishPreparedBucket() {
    try {
      this.currentReader.close();
      this.currentReader = null;
      this.currentBucketIndex = -1;
    } catch (IOException e) {
      throw new RuntimeException("[Edge Manageer] finish Prepare the bucket :",
          e);
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.currentEdgesLength = in.readInt();
    if (this.currentEdgesLength < 0 || this.edgelist == null) {
      return;
    }
    // Note Clear The Edges Info Of Last Instance.
    this.edgelist.clear();
    for (int i = 0; i < this.currentEdgesLength; i++) {
      this.currentEdge = getEdgeFromPool(i);
      this.currentEdge.readFields(in);
      this.edgelist.add(this.currentEdge);
    }
    this.currentEdgesLength = -1;
    this.edgelist = null;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    if (this.currentEdgesLength == -1 || this.edgelist == null) {
      return;
    }
    out.writeInt(this.currentEdgesLength);
    // LOG.info("EDGE MANAGER SAVE============= length"
    // +this.currentEdgesLength);
    for (int i = 0; i < this.currentEdgesLength; i++) {
      // LOG.info("EDMGE MANAGER SAVE========"
      // +this.edgelist.get(0).intoString());
      this.edgelist.remove(0).write(out);
    }
    this.currentEdgesLength = -1;
    this.edgelist = null;
  }
  
  /**
   * Initialize the edge pool.
   * @param size
   */
  private void initEdgePool(int size) {
    this.edgePoolSize = size;
    this.edgePool = new ArrayList<Edge>(size);
    // Note Edge Class Type Should Be Ok
    for (int i = 0; i < size; i++) {
      this.edgePool.add(getEdgeInstance());
    }
  }
  
  /**
   * Get edge from pool.
   * @return Edge
   */
  private Edge getEdgeFromPool(int index) {
    if (index < this.edgePoolSize) {
      return this.edgePool.get(index);
    }
    return getEdgeInstance();
  }
  
  /**
   * Get instance of edge.
   * @return Edge
   */
  private Edge getEdgeInstance() {
    try {
      return edgeClass.newInstance();
    } catch (InstantiationException e) {
      LOG.error("[Edge Manageer]get edge instance. ", e);
      throw new RuntimeException("[Edge Manageer] get edge instance:", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("[Edge Manageer] get edge instance:", e);
    }
//    return null;
  }

  public BCBSPSpillingInputBuffer getCurrentReader() {
    return currentReader;
  }
}
