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

package com.chinamobile.bcbsp.router;

import java.util.HashMap;

import org.apache.hadoop.io.Text;

/**
 * The RangeRoute implements route to decide a vertex is belong to which
 * partition.
 * @author wyj
 */
public class RangeRoute implements route {
  /**The RangeRouter is a HashMap which the key is the maxcountid of a
   *partition,the value is the partitionid.*/
  private HashMap<Integer, Integer> rangerouter = null;
  /**
   * Constructor of the class.
   * @param aRangeRouter is a HashMap which the key is the maxcountid of a
   * partition,the value is the partitionid.
   */
  RangeRoute(HashMap<Integer, Integer> aRangeRouter) {
    this.rangerouter = aRangeRouter;
  }
  /**The method decide the vertexid is belong to which partition.
   * @param vertexID The id of the vertex.
   * @return partitionid
   */
  public int getpartitionID(Text vertexID) {
    int vertexid = Integer.parseInt(vertexID.toString());
    int tempMaxMin = Integer.MAX_VALUE;
    for (Integer e : rangerouter.keySet()) {
      if (vertexid <= e) {
        if (tempMaxMin > e) {
          tempMaxMin = e;
        }
      }
    }
    return rangerouter.get(tempMaxMin);
  }
}
