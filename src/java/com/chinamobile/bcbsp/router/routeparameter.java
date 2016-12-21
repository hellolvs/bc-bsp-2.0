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
 * The RouteParameter contains the information of the Route.
 * @author wyj
 */
public class routeparameter {
  /**The partitioner to hash a vertex to a bucket.*/
  private Partitioner<Text> partitioner = null;
  /**The hashBucketToPartition decide a bucket to which partition.*/
  private HashMap<Integer, Integer> hashBucketToPartition = null;
  /**The RangeRouter is a HashMap which the key is the maxcountid of a
   *partition,the value is the partitionid.*/
  private HashMap<Integer, Integer> rangerouter = null;
  /**The get method of getting the RangeRouter.
   *@return rangerouter
   */
  public HashMap<Integer, Integer> getRangeRouter() {
    return rangerouter;
  }
  /**The set method of setting the RangeRouter.
   *@param rangeRouter is a HashMap which the key is the maxcountid of a
   *partition,the value is the partitionid.
   */
  public void setRangeRouter(HashMap<Integer, Integer> rangeRouter) {
    rangerouter = rangeRouter;
  }
  /**The get method of getting the hashBucketToPartition.
   *@return hashBucketToPartition
   */
  public HashMap<Integer, Integer> getHashBucketToPartition() {
    return hashBucketToPartition;
  }
  /**The set method of setting the hashBucketToPartition.
   * @param aHashBucketToPartition decide a bucket to which partition
   */
  public void setHashBucketToPartition(
      HashMap<Integer, Integer> aHashBucketToPartition) {
    this.hashBucketToPartition = aHashBucketToPartition;
  }
  /**The get method of getting the partitioner.
   *@return partitioner
   */
  public Partitioner<Text> getPartitioner() {
    return partitioner;
  }
  /**The set method of setting the partitioner.
   *@param aPartitioner to hash a vertex to a bucket
   */
  public void setPartitioner(Partitioner<Text> aPartitioner) {
    this.partitioner = aPartitioner;
  }
}
