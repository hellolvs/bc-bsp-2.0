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

import java.io.IOException;
import java.util.ArrayList;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.io.RecordWriter;

/**
 * The Interface for GraphData, which manages the graph data.
 */
public interface GraphDataInterface extends GraphInterfaceNew {
  
  /**
   * Add new vertex to the graph data. This is defined for the graph data's
   * loading stage.
   * @param vertex Vertex
   */
  @SuppressWarnings("unchecked")
  void addForAll(Vertex vertex);
  
  /**
   * Because every staff owns a graph data, a graph data needs some properties
   * of the staff.
   * @param staff Staff
   */
  void setStaff(Staff staff);
  
  /**
   * Initialize the graph data.
   */
  void initialize();

  /**
   * Get the size of the graph data. This is defined for the traversal of the
   * graph data for the computation during the super steps.
   * @since 2012-3-6 aborted.
   * @return size
   */
  int size();
  
  /**
   * Get the Vertex for a given index. This is defined for the traversal of the
   * graph data for the computation during the super steps.
   * @since 2012-3-6 aborted.
   * @param index int
   * @return headNode
   */
  @SuppressWarnings("unchecked")
  Vertex get(int index);
  
  /**
   * Set the Vertex a for given index. This is defined for the traversal of the
   * graph data for the computation during the super steps.
   * @param index int
   * @param vertex Vertex
   * @param activeFlag boolean
   */
  @SuppressWarnings("unchecked")
  void set(int index, Vertex vertex, boolean activeFlag);
  
  /**
   * Get the size of all of the graph data. This is defined for the traversal of
   * the whole graph data.
   * @return size
   */
  int sizeForAll();
  
  /**
   * Get the Vertex for a given index for all. This is defined for the traversal
   * of the whole graph data.
   * @param index int
   * @return vertex
   */
  @SuppressWarnings("unchecked")
  Vertex getForAll(int index);
  
 
  /**
   * Get the Vertex's active state flag for a given index. This is defined for
   * the traversal of the whole graph data.
   * @param index int
   * @return boolean
   */
  boolean getActiveFlagForAll(int index);
  
  /**
   * Tell the graph data manager that add operation is all over for the loading
   * process. Invoked after data load process.
   */
  void finishAdd();
  
  /**
   * Tell the graph data manager to clean all the resources for the graph data.
   * Invoked after the local computation and before the staff to exit.
   */
  void clean();
  
  /**
   * Get the total number of active vertices.
   * @return long
   */
  long getActiveCounter();
  
  /**
   * Log the current usage information of memory.
   */
  void showMemoryInfo();
  
  /**
   * Get the total count of edges.
   * @return int
   */
  int getEdgeSize();
  /**
   * Get the size of graph vertex
   * @return int
   */
  int getVertexSize();
  /**
   * set the staff migrate flag
   * @param flag
   */
  void setMigratedStaffFlag(boolean flag);
//  void prepareBucketForMigrate(int bucket, int superstep, boolean migratedFlag);
/**
 * set the staff recovery flag to modify vertex load path
 */
void setRecovryFlag(boolean recovery);

/**
 * feng added for write edge info
 * @param output
 * @param writeEdgeFlag
 * @throws IOException
 * @throws InterruptedException
 */
void saveAllVertices(RecordWriter output, boolean writeEdgeFlag)
		throws IOException, InterruptedException;
}
