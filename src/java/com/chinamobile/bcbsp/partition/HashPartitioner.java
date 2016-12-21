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

package com.chinamobile.bcbsp.partition;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.chinamobile.bcbsp.api.Partitioner;

/**
 * HashPartitioner is partitioning by the MD5-based hash code of the vertex id.
 * @param <IDT> the type of the vertex
 */
public class HashPartitioner<IDT> extends Partitioner<IDT> {
  /**the index of the array.*/
  private static final int INDEX_0 = 0;
  /**the index of the array.*/
  private static final int INDEX_1 = 1;
  /**the index of the array.*/
  private static final int INDEX_2 = 2;
  /**the index of the array.*/
  private static final int INDEX_3 = 3;
  /**the offset of the number.*/
  private static final int OFFSET_1 = 8;
  /**the offset of the number.*/
  private static final int OFFSET_2 = 16;
  /**the offset of the number.*/
  private static final int OFFSET_3 = 24;
  /**the compared number.*/
  private static final int NUMBER = 0xFF;
  /**
   * Constructor method.
   */
  public HashPartitioner() {
  }
  /**
   * Constructor method.
   * @param numpartition the number of the partition.
   */
  public HashPartitioner(int numpartition) {
    this.numPartition = numpartition;
  }
  /**
   * Partitions a vertex according the id mapping to a partition.
   * @return a number between 0 and numPartition that tells which partition it
   *         belongs to.
   * @param id the vertex id
   */
  public int getPartitionID(IDT id) {
    String url = id.toString();
    MessageDigest md5 = null;
    if (md5 == null) {
      try {
        md5 = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("++++ no md5 algorythm found");
      }
    }
    md5.reset();
    md5.update(url.getBytes());
    byte[] bKey = md5.digest();
    long hashcode = ((long) (bKey[INDEX_3] & NUMBER) << OFFSET_3)
         | ((long) (bKey[INDEX_2] & NUMBER) << OFFSET_2)
         | ((long) (bKey[INDEX_1] & NUMBER) << OFFSET_1)
         | (long) (bKey[INDEX_0] & NUMBER);
    int result = (int) (hashcode % this.numPartition);
    if (result < 0) {
      result = result + this.numPartition;
    }
    return result;
  }
}
