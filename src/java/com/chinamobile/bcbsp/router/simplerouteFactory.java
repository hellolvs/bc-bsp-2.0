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

import com.chinamobile.bcbsp.api.Partitioner;

/**
 * The SimpleRouteFactory decide to use which route.
 * @author wyj
 */
public class simplerouteFactory {
  /**
   * generator a route of the system.
   * @param aRouteparameter contains the information of the Route
   * @return Route to decide the partititon of a vertex
   */
  public static route createroute(routeparameter aRouteparameter) {
    Partitioner<Text> partitioner = aRouteparameter.getPartitioner();
    HashMap<Integer, Integer> hashBucketToPartition = aRouteparameter
        .getHashBucketToPartition();
    HashMap<Integer, Integer> rangeRouter = aRouteparameter.getRangeRouter();
    if (hashBucketToPartition == null) {
      if (rangeRouter == null) {
        return new HashRoute(partitioner);
      } else {
        return new RangeRoute(rangeRouter);
      }
    } else {
      return new BalancerHashRoute(partitioner, hashBucketToPartition);
    }
  }
}
