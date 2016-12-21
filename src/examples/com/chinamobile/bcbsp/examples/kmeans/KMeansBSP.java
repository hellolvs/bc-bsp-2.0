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

package com.chinamobile.bcbsp.examples.kmeans;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.util.BSPJob;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * KMeansBSP This is the user-defined arithmetic which implements {@link BSP}.
 * Implements the basic k-means algorithm.
 */
public class KMeansBSP extends BSP<BSPMessage> {
  /** Define LOG for outputting log information */
  public static final Log LOG = LogFactory.getLog(KMeansBSP.class);
  /** State KMEANS_K */
  public static final String KMEANS_K = "kmeans.k";
  /** State KMEANS_CENTERS */
  public static final String KMEANS_CENTERS = "kmeans.centers";
  /** State AGGREGATE_KCENTERS */
  public static final String AGGREGATE_KCENTERS = "aggregate.kcenters";
  /** State BSPJob */
  private BSPJob jobconf;
  /** The count of superStep */
  private int superStepCount;
  /** k center */
  private int k;
  /** State dimension */
  private int dimension;
  /** k center */
  private ArrayList<ArrayList<Float>> kCenters = new
      ArrayList<ArrayList<Float>>();
  /**
   * The threshold for average error between the new k centers
   * and the last k centers.
   */
  private final double errorsThreshold = 0.01;
  /**
   * The real average error between the new k centers and
   * the last k centers.
   */
  private double errors = Double.MAX_VALUE;

  @Override
  public void compute(Iterator<BSPMessage> messages,
      BSPStaffContextInterface context) throws Exception {
    jobconf = context.getJobConf();
    superStepCount = context.getCurrentSuperStepCounter();
    ArrayList<Float> thisPoint = new ArrayList<Float>();
    KMVertex thisVertex = (KMVertex) context.getVertex();
    Iterator<Edge> outgoingEdges = context.getOutgoingEdges();
    // Init this point
    while (outgoingEdges.hasNext()) {
      KMEdge edge = (KMEdge) outgoingEdges.next();
      thisPoint.add(Float.valueOf(edge.getEdgeValue()));
    }
    // Calculate the class tag of this vertex.
    byte tag = 0;
    double minDistance = Double.MAX_VALUE;
    // Find the shortest distance of this point with the kCenters.
    for (byte i = 0; i < kCenters.size(); i++) {
      ArrayList<Float> center = kCenters.get(i);
      double dist = distanceOf(thisPoint, center);
      if (dist < minDistance) {
        tag = i;
        minDistance = dist;
      }
    }
    // Write the vertex's class tag into the vertex value.
    thisVertex.setVertexValue(tag);
    context.updateVertex(thisVertex);
    if (this.errors < this.errorsThreshold) {
      context.voltToHalt();
    }
  } // end-compute

  /**
   * Get distance between p1 and p2.
   * @param p1 ArrayList type
   * @param p2 ArrayList type
   * @return distance
   */
  private double distanceOf(ArrayList<Float> p1, ArrayList<Float> p2) {
    double dist = 0.0;
    // dist = (x1-y1)^2 + (x2-y2)^2 + ... + (xn-yn)^2
    for (int i = 0; i < p1.size(); i++) {
      dist = dist + (p1.get(i) - p2.get(i)) * (p1.get(i) - p2.get(i));
    }
    dist = Math.sqrt(dist);
    return dist;
  }

  @Override
  public void initBeforeSuperStep(SuperStepContextInterface context) {
    this.superStepCount = context.getCurrentSuperStepCounter();
    jobconf = context.getJobConf();
    if (superStepCount == 0) {
      this.k = Integer.valueOf(jobconf.get(KMeansBSP.KMEANS_K));
      // Init the k original centers from job conf.
      String originalCenters = jobconf.get(KMeansBSP.KMEANS_CENTERS);
      String[] centers = originalCenters.split("\\|");
      for (int i = 0; i < centers.length; i++) {
        ArrayList<Float> center = new ArrayList<Float>();
        String[] values = centers[i].split("-");
        for (int j = 0; j < values.length; j++) {
          center.add(Float.valueOf(values[j]));
        }
        kCenters.add(center);
      }
      this.dimension = kCenters.get(0).size();
      LOG.info("[KMeansBSP] K = " + k);
      LOG.info("[KMeansBSP] dimension = " + dimension);
      LOG.info("[KMeansBSP] k centers: ");
      for (int i = 0; i < k; i++) {
        String tmpCenter = "";
        for (int j = 0; j < dimension; j++) {
          tmpCenter = tmpCenter + " " + kCenters.get(i).get(j);
        }
        LOG.info("[KMeansBSP] <" + tmpCenter + " >");
      }
    } else {
      KCentersAggregateValue kCentersAgg = (KCentersAggregateValue) context
          .getAggregateValue(KMeansBSP.AGGREGATE_KCENTERS);
      ArrayList<ArrayList<Float>> newKCenters = new
          ArrayList<ArrayList<Float>>();
      // Calculate the new k centers and save them to newKCenters.
      ArrayList<ArrayList<Float>> contents = kCentersAgg.getValue();
      ArrayList<Float> nums = contents.get(k);
      for (int i = 0; i < k; i++) {
        ArrayList<Float> center = new ArrayList<Float>();
        // Get the sum of coordinates of points in class i.
        ArrayList<Float> sum = contents.get(i);
        // Get the number of points in class i.
        float num = nums.get(i);
        for (int j = 0; j < dimension; j++) {
          // the center's coordinate value.
          center.add(sum.get(j) / num);
        }
        // The i center.
        newKCenters.add(center);
      }
      this.errors = 0.0;
      // Calculate the errors sum between the new k centers and the last k
      // centers.
      for (int i = 0; i < k; i++) {
        for (int j = 0; j < dimension; j++) {
          this.errors = this.errors +
              Math.abs(kCenters.get(i).get(j) - newKCenters.get(i).get(j));
        }
      }
      this.errors = this.errors / (k * dimension);
      this.kCenters.clear();
      this.kCenters = newKCenters;
      LOG.info("[KMeansBSP] k centers: ");
      for (int i = 0; i < k; i++) {
        String tmpCenter = "[" + nums.get(i) + "]";
        for (int j = 0; j < dimension; j++) {
          tmpCenter = tmpCenter + " " + kCenters.get(i).get(j);
        }
        LOG.info("[KMeansBSP] <" + tmpCenter + " >");
      }
    }
    LOG.info("[KMeansBSP]******* Error = " + errors + " ********");
  }
}
