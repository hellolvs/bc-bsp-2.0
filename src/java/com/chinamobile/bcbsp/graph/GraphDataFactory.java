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
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.ml.BSPPeer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Graph data factory create GraphDataInterface.
 */
public class GraphDataFactory {
  /** class logger. */
  private static final Log LOG = LogFactory.getLog(GraphDataFactory.class);
  /** the graph data class from conf ,dafault is null.*/
  private Class<?>[] currentGraphData = null;
  /** four kinds of graph data class. */
  private static Class<?>[] defaultClasses = {GraphDataForMem.class,
      GraphDataForDisk.class,GraphDataForBDB.class,GraphDataMananger.class,BSPPeer.class};
  
  /**
   * Constructor of GraphDataFactory.
   * @param conf
   */
  public GraphDataFactory(Configuration conf) {
    getGraphDataClasses(conf);
  }
  
  /**
   * @param conf
   */
  private void getGraphDataClasses(Configuration conf) {
    currentGraphData = conf
        .getClasses(Constants.USER_BC_BSP_JOB_GRAPHDATA_CLASS);
    if (currentGraphData == null || currentGraphData.length == 0) {
      currentGraphData = defaultClasses;
    }
  }
  
  /**
   * Create new graphdata class with staff and version.
   * @param version
   * @param staff
   */
  public GraphDataInterface createGraphData(int version, Staff staff) {
    Class graphDataClass = currentGraphData[version];
    GraphDataInterface graphData = null;
    try {
      graphData = (GraphDataInterface) graphDataClass.newInstance();
    } catch (Exception e) {
      LOG.warn("Can not find the class:" + currentGraphData[version]);
      LOG.warn("Use the default GraphDataForMem");
      graphData = new GraphDataForMem();
    }
    graphData.setStaff(staff);
    graphData.initialize();
    return graphData;
  }
}
