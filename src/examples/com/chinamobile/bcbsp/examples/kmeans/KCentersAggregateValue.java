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

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * KCentersAggregateValue Contains the k classes' sum of coordinate values and
 * the number of points in each class.
 */
public class KCentersAggregateValue
    extends
      AggregateValue<ArrayList<ArrayList<Float>>, BSPMessage> {
  /**
   * First k records are coordinates sum of k classes for current super step,
   * last 1 record are numbers of k classes for current super step. As follows:
   * (k+1) rows totally. -------------------- <S11, S12, ..., S1n> <S21, S22,
   * ..., S2n> ... <Sk1, Sk2, ..., Skn> -------------------- <N1, N2, ..., Nk>
   */
  private ArrayList<ArrayList<Float>> contents = new
      ArrayList<ArrayList<Float>>();
  /** k centers */
  private ArrayList<ArrayList<Float>> kCenters =
      new ArrayList<ArrayList<Float>>();
  /** k classes */
  private int k;
  /** State dimension */
  private int dimension;

  @Override
  public ArrayList<ArrayList<Float>> getValue() {
    return contents;
  }

  /**
   * In the form as follows:
   * S11-S12-...-S1n|S21-S22-...-S2n|...|S2k1-S2k2-...S2kn|N1-N2-...-Nk.
   * @param arg0 initValue
   */
  @Override
  public void initValue(String arg0) {
    String[] centers = arg0.split("\\|");
    for (int i = 0; i < centers.length; i++) {
      ArrayList<Float> center = new ArrayList<Float>();
      String[] values = centers[i].split("-");
      for (int j = 0; j < values.length; j++) {
        center.add(Float.valueOf(values[j]));
      }
      contents.add(center);
    }
  }

  @Override
  public String toString() {
    String value = "";
    for (int i = 0; i < contents.size(); i++) {
      ArrayList<Float> center = contents.get(i);
      if (i > 0) {
        value = value + "|";
      }
      for (int j = 0; j < center.size(); j++) {
        if (j > 0) {
          value = value + "-";
        }
        value = value + center.get(j);
      }
    }
    return value;
  }

  @Override
  public void initValue(Iterator<BSPMessage> messages,
      AggregationContextInterface context) {
    ArrayList<Float> thisPoint = new ArrayList<Float>();
    // Init this point
    Iterator<Edge<?, ?>> bordersItr = context.getOutgoingEdges();
    KMEdge edge;
    while (bordersItr.hasNext()) {
      edge = (KMEdge) bordersItr.next();
      thisPoint.add(Float.valueOf(edge.getEdgeValue()));
    }
    // Init the contents with all 0.0
    this.contents.clear();
    ArrayList<Float> nums = new ArrayList<Float>();
    for (int i = 0; i < k; i++) {
      ArrayList<Float> sum = new ArrayList<Float>();
      for (int j = 0; j < dimension; j++) {
        sum.add(0.0f);
      }
      contents.add(sum);
      nums.add(0.0f);
    }
    contents.add(nums);
    // Calculate the class tag of this vertex.
    int tag = 0;
    double minDistance = Float.MAX_VALUE;
    // Find the shortest distance of this point with the kCenters.
    for (int i = 0; i < kCenters.size(); i++) {
      ArrayList<Float> center = kCenters.get(i);
      double dist = distanceOf(thisPoint, center);
      if (dist < minDistance) {
        tag = i;
        minDistance = dist;
      }
    }
    // Add the value of this vertex into the sum of tag class.
    ArrayList<Float> sum = contents.get(tag);
    for (int i = 0; i < dimension; i++) {
      sum.set(i, thisPoint.get(i));
    }
    // Init the refered class's number to 1.
    nums = contents.get(k);
    nums.set(tag, 1.0f);
  }

  @Override
  public void setValue(ArrayList<ArrayList<Float>> arg0) {
    this.contents = arg0;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    contents.clear();
    int centerSize = in.readInt();
    for (int i = 0; i < centerSize; i++) {
      int valueSize = in.readInt();
      ArrayList<Float> center = new ArrayList<Float>();
      for (int j = 0; j < valueSize; j++) {
        center.add(in.readFloat());
      }
      contents.add(center);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int centerSize = contents.size();
    out.writeInt(centerSize);
    for (int i = 0; i < centerSize; i++) {
      int valueSize = contents.get(i).size();
      out.writeInt(valueSize);
      for (int j = 0; j < valueSize; j++) {
        out.writeFloat(contents.get(i).get(j));
      }
    }
  }

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
    int superStepCount = context.getCurrentSuperStepCounter();
    this.kCenters.clear();
    if (superStepCount == 0) {
      BSPJob jobconf = context.getJobConf();
      k = Integer.valueOf(jobconf.get(KMeansBSP.KMEANS_K));
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
      dimension = kCenters.get(0).size();
    } else {
      KCentersAggregateValue kCentersAgg = (KCentersAggregateValue) context
          .getAggregateValue(KMeansBSP.AGGREGATE_KCENTERS);
      // Calculate the new k centers and save them to KCenters.
      ArrayList<ArrayList<Float>> content = kCentersAgg.getValue();
      ArrayList<Float> nums = content.get(k);
      for (int i = 0; i < k; i++) {
        ArrayList<Float> center = new ArrayList<Float>();
        // Get the sum of coordinates of points in class i.
        ArrayList<Float> sum = content.get(i);
        // Get the number of points in class i.
        float num = nums.get(i);
        for (int j = 0; j < dimension; j++) {
          // the center's coordinate value.
          center.add(sum.get(j) / num);
        }
        // The i center.
        this.kCenters.add(center);
      }
    }
  }
}
