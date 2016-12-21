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

package com.chinamobile.bcbsp.comm;

/** Access the partition and bucket index. */
public class PartitionRule {
  
  /** Constructor of PartitionRule. */
  private PartitionRule() {
  }
  
  /**
   * Get the local partition rule, and return the bucket ID.
   * @param dstVertexID
   * @return bucket ID
   */
  public static int localPartitionRule(String dstVertexID) {
    int hashBucketNumber = MetaDataOfMessage.HASH_NUMBER;
    int hashCode = dstVertexID.hashCode();
    int hashIndex = hashCode % hashBucketNumber; // bucket index
    hashIndex = (hashIndex < 0 ? hashIndex + hashBucketNumber : hashIndex);
    return hashIndex;
  }
  
  /**
   * Get destination partition bucket ID.
   * @param destPartition
   *        int
   * @param bucket
   *        int
   * @return int
   */
  public static int getDestPartitionBucketId(int destPartition, int bucket) {
    return destPartition * MetaDataOfMessage.HASH_NUMBER + bucket;
  }
  
  /**
   * Get source partition bucket ID.
   * @param destPartition
   *        int
   * @param bucket
   *        int
   * @return int
   */
  public static int getSrcPartitionBucketId(int destPartition, int bucket) {
    return destPartition * MetaDataOfMessage.HASH_NUMBER + bucket;
  }
  
  /**
   * Get partition ID with hash rule.
   * @param partitionBucketId
   * @return int
   */
  public static int getPartition(int partitionBucketId) {
    return partitionBucketId / MetaDataOfMessage.HASH_NUMBER;
  }
  
  /**
   * Get bucket ID with hash rule.
   * @param partitionBucketId int
   * @return int
   */
  public static int getBucket(int partitionBucketId) {
    return partitionBucketId % MetaDataOfMessage.HASH_NUMBER;
  }
}
