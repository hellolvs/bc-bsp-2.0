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
 * The BalancerHashRoute implements route to decide a vertex is belong to which
 * partition.
 * @author wyj
 */
public class BalancerHashRoute implements route {
  /**The partitioner to hash a vertex to a bucket.*/
  private Partitioner<Text> partitioner;
  /**The hashBucketToPartition decide a bucket to which partition.*/
  private HashMap<Integer, Integer> hashBucketToPartition;
  /**
   * Constructor of the class.
   * @param aPartitioner partition a vertex to a bucket
   * @param aHashBucketToPartition decide a bucket to which partition
   */
  BalancerHashRoute(Partitioner<Text> aPartitioner,
      HashMap<Integer, Integer> aHashBucketToPartition) {
    this.partitioner = aPartitioner;
    this.hashBucketToPartition = aHashBucketToPartition;
  }
  /**The method decide the vertexid is belong to which partition.
   * @param vertexID vertexid.
   * @return partitionid
   */
  public int getpartitionID(Text vertexID) {
    int halfdestination = this.partitioner.getPartitionID(vertexID);
    return this.hashBucketToPartition.get(halfdestination);
  }
}
