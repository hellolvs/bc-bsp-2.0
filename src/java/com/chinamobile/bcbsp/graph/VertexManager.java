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

import com.chinamobile.bcbsp.api.Vertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

/**
 * Vertex manager. Vertices data default is on disk, with this can load vertices
 * data from disk. After compute, vertex data save into disk.
 */
public class VertexManager implements Writable {
  /** class logger. */
  public static final Log LOG = LogFactory.getLog(VertexManager.class);
  /** current vertex. */
  private Vertex currentVertex;
  /** BCBSP output Buffer:IO Handler. */
  private BCBSPSpillingOutputBuffer[] spillingBuffers;
  /** directory of vertices on disk. */
  private String vertexDir;
  /** current BCBSP output Buffer writer. */
  private BCBSPSpillingOutputBuffer currentWriter;
  /** current BCBSP input Buffer reader. */
  private BCBSPSpillingInputBuffer currentReader;
  
  /**
   * Constructor of VertexManager.
   */
  public VertexManager() {
    initialize();
  }
  
  /** Initialize the edge manager. */
  public void initialize() {
    vertexDir = MetaDataOfGraph.BCBSP_DISKGRAPH_WORKDIR + "/" + "Vertices";
    File tmpFile;
    tmpFile = new File(vertexDir, "init");
    if (!tmpFile.exists()) {
      tmpFile.mkdirs();
    }
    spillingBuffers = new BCBSPSpillingOutputBuffer[MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER];
    try {
      for (int i = 0; i < MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER; i++) {
        
        spillingBuffers[i] = new BCBSPSpillingOutputBuffer(tmpFile.toString()
            + "/Bucket-" + i, MetaDataOfGraph.BCBSP_GRAPH_LOAD_INIT);
        
      }
    } catch (IOException e) {
      throw new RuntimeException("[Vertex Manager] initialize", e);
    }
  }
  
  /**
   * Save vertex, used only once for loading data from HDFS and local saving.
   * @param v
   */
  public void processVertexSaveForAll(Vertex v) {
    int hashNumber = MetaDataOfGraph.localPartitionRule(String.valueOf(v
        .getVertexID()));
    MetaDataOfGraph.VERTEX_NUM_PERBUCKET[hashNumber]++;
    this.currentVertex = v;
    try {
      this.write(this.spillingBuffers[hashNumber]);
    } catch (IOException e) {
      throw new RuntimeException("[Vertex Manageer] Save For All Vertex", e);
    }
  }
  
  /**
   * Save vertex, used only once for loading data from HDFS and local saving.
   * @param v
   */
  public void processVertexSave(Vertex v) {
    this.currentVertex = v;
    if (this.currentWriter == null) {
      return;
    }
    try {
      this.write(this.currentWriter);
    } catch (IOException e) {
      throw new RuntimeException("[Vertex Manager] Save Vertex", e);
    }
  }
  
  /**
   * Load vertex from disk.
   * @param v
   */
  public void processVertexLoad(Vertex v) {
    this.currentVertex = v;
    if (this.currentReader == null) {
      // Note Not Null
      LOG.info("I/O Handler Is Null While Loading Vertices");
      return;
    }
    try {
      this.readFields(this.currentReader);
    } catch (IOException e) {
      throw new RuntimeException("[Vertex Manager] Load Vertex", e);
    }
  }
  
  /**
   * Finish loading of vertices.
   */
  public void finishVertexLoad() {
    try {
      for (int i = 0; i < MetaDataOfGraph.BCBSP_DISKGRAPH_HASHNUMBER; i++) {
        spillingBuffers[i].close();
        spillingBuffers[i] = null;
      }
    } catch (IOException e) {
      throw new RuntimeException("[Vertex Manager] Finish Load Vertex", e);
    }
  }
  
  /**
   * Prepare bucket before compute.
   * @param bucket
   * @param superstep
   */
  public void prepareBucket(int bucket, int superstep) {
    File tmpFileOut, tmpFileIn;
    tmpFileOut = new File(vertexDir, "" + superstep);
    if (superstep == 0) {
      tmpFileIn = new File(vertexDir, "init");
    } else {
      int tmp = superstep - 1;
      tmpFileIn = new File(vertexDir, "" + tmp);
    }
    if (!tmpFileIn.exists()) {
      LOG.info("Input File Not Exits" + "");
      return;
    }
    if (!tmpFileOut.exists()) {
      tmpFileOut.mkdirs();
    }
    try {
      this.currentReader = new BCBSPSpillingInputBuffer(tmpFileIn.toString()
          + "/Bucket-" + bucket, MetaDataOfGraph.BCBSP_GRAPH_LOAD_READSIZE);
      this.currentWriter = new BCBSPSpillingOutputBuffer(tmpFileOut.toString()
          + "/Bucket-" + bucket, MetaDataOfGraph.BCBSP_GRAPH_LOAD_WRITESIZE);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("[Vertex Manager] prepareBucket exception", e);
    }
  }
  
  /**
   * Finish preparing of vertices.
   */
  public void finishPreparedBucket() {
    try {
      this.currentReader.close();
      this.currentReader = null;
      this.currentWriter.close();
      this.currentWriter = null;
    } catch (IOException e) {
      throw new RuntimeException("[Vertex Manager] prepareBucket exception", e);
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.currentVertex.readFields(in);
    boolean active = in.readBoolean();
    this.currentVertex.setActive(active);
    this.currentVertex = null;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    this.currentVertex.write(out);
    out.writeBoolean(this.currentVertex.isActive());
    this.currentVertex = null;
  }
  
  /**
   * Prepare bucket before compute.
   * @param bucket
   * @param superstep
   */
  public void prepareBucketForMigrate(int bucket, int superstep,
      boolean migratedFlag) {
    File tmpFileOut, tmpFileIn;
    tmpFileOut = new File(vertexDir, "" + superstep);
    if (superstep == 0) {
      tmpFileIn = new File(vertexDir, "init");
    } else {
      int tmp = superstep - 1;
      tmpFileIn = new File(vertexDir, "" + tmp);
    }
    LOG.info("MigratedFlag! " + migratedFlag);
    if (!tmpFileIn.exists() && migratedFlag == true) {
      tmpFileIn = new File(vertexDir, "init");
      LOG.info("Input File Not Exits, Use Init File for MigratedStaff!"
          + tmpFileIn.toString());
      // return;
    }
    if (!tmpFileIn.exists()) {
      // tmpFileIn = new File(vertexDir,"init");
      LOG.info("Input File Not Exits" + "");
      return;
    }
    if (!tmpFileOut.exists()) {
      tmpFileOut.mkdirs();
    }
    try {
      this.currentReader = new BCBSPSpillingInputBuffer(tmpFileIn.toString()
          + "/Bucket-" + bucket, MetaDataOfGraph.BCBSP_GRAPH_LOAD_READSIZE);
      this.currentWriter = new BCBSPSpillingOutputBuffer(tmpFileOut.toString()
          + "/Bucket-" + bucket, MetaDataOfGraph.BCBSP_GRAPH_LOAD_WRITESIZE);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("[Vertex Manager] prepareBucket exception", e);
    }
  }
  
  public void prepareBucketForRecovery(int bucket, int superstep,
      boolean recoveryStaffFlag) {
    // TODO Auto-generated method stub
    File tmpFileOut, tmpFileIn;
    tmpFileOut = new File(vertexDir, "" + superstep);
    if (superstep == 0) {
      tmpFileIn = new File(vertexDir, "init");
    } else {
      int tmp = superstep - 1;
      tmpFileIn = new File(vertexDir, "" + tmp);
    }
    LOG.info("recoveryStaffFlag ! " + recoveryStaffFlag);
    if (!tmpFileIn.exists() && recoveryStaffFlag == true) {
      tmpFileIn = new File(vertexDir, "init");
      LOG.info("Input File Not Exits, Use Init File for RecoveryStaff!"
          + tmpFileIn.toString());
      // return;
    }
    if (!tmpFileIn.exists()) {
      // tmpFileIn = new File(vertexDir,"init");
      LOG.info("Input File Not Exits" + "");
      return;
    }
    if (!tmpFileOut.exists()) {
      tmpFileOut.mkdirs();
    }
    try {
      this.currentReader = new BCBSPSpillingInputBuffer(tmpFileIn.toString()
          + "/Bucket-" + bucket, MetaDataOfGraph.BCBSP_GRAPH_LOAD_READSIZE);
      this.currentWriter = new BCBSPSpillingOutputBuffer(tmpFileOut.toString()
          + "/Bucket-" + bucket, MetaDataOfGraph.BCBSP_GRAPH_LOAD_WRITESIZE);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("[Vertex Manager] prepareBucket exception", e);
    }
  }
  
  /** For JUnit test. */
  public BCBSPSpillingOutputBuffer[] getSpillingBuffers() {
    return spillingBuffers;
  }
  
  public void setSpillingBuffers(BCBSPSpillingOutputBuffer[] spillingBuffers) {
    this.spillingBuffers = spillingBuffers;
  }
  
  public BCBSPSpillingOutputBuffer getCurrentWriter() {
    return currentWriter;
  }
  
  public BCBSPSpillingInputBuffer getCurrentReader() {
    return currentReader;
  }
}
