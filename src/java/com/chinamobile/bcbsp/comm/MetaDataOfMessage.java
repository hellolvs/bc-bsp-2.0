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

/**
 * Meta Data Used In Communication Of Local Computing And Some Parameters In
 * Message Manager.
 * @author Liu Jinpeng
 */
public class MetaDataOfMessage {
  /** the threshold message send buffer. */
  public static int MESSAGE_SEND_BUFFER_THRESHOLD;
  /** the threshold message send spilt. */
  public static int MESSAGE_SEND_SPILL_THRESHOLD;
  // Note ALL Messages Is On Disk By Default.
  /** the threshold message receiver buffer. */
  public static int MESSAGE_RECEIVED_BUFFER_THRESHOLD = 0;
  /** the partition number. */
  public static int PARTITION_NUMBER;
  /** default hash value. */
  public static int HASH_NUMBER;
  /** the number of buckets in partition. */
  public static int PARTITIONBUCKET_NUMBER;
  /** partition ID. */
  public static int PARTITION_ID;
  // Note Statistics :Recording Buffer Used For Messages.
  /** the array of receive message buffer length. */
  public static int[] RMBLength;
  /** the array of send message buffer length. */
  public static int[] SMBLength;
  /** Set The Size Of Thread Pool. */
  public static int DEFAULT_MESSAGE_THREAD_BASE = 10;
  /** message Data For Byte Compute */
  public static byte BYTESIZE_PER_MESSAGE = 0;
  /** message IO size. */
  public static int MESSAGE_IO_BYYES = -1;
  /** statistics for send message counting. */
  public static long SENDMSGCOUNTER;
  /** counter of network messages. */
  public static long NETWORKEDMSGCOUNTER;
  // Note Add As Above 20140315
  /** counter of network messages byte. */
  public static long NETWORKEDMSGBYTECOUNTER;
  /** counter of send messages byte. */
  public static long SENDMSGBYTESCOUNTER;
  /** statistics for receive message counting. */
  public static long RECEIVEMSGCOUNTER;
  /** statistics for receive message byte counting. */
  public static long RECEIVEMSGBYTESCOUNTER;
  /** Flag Associated With Send Buffer Directing Message Sending. */
  public static class BufferStatus {
    public static final int NORMAL = 1;
    public static final int SPILL = 2;
    public static final int OVERFLOW = 3;
  }
  
  /** PARTITIONBUCKET_NUMBER Should Be Set Before This Method. */
  public static void initializeMBLength() {
    RMBLength = new int[PARTITIONBUCKET_NUMBER];
    SMBLength = new int[PARTITIONBUCKET_NUMBER];
    for (int i = 0; i < PARTITIONBUCKET_NUMBER; i++) {
      RMBLength[i] = 0;
      SMBLength[i] = 0;
    }
  }
  
  /** Clear the array of send message buffer length. */
  public static void clearSMBLength() {
    for (int i = 0; i < PARTITIONBUCKET_NUMBER; i++) {
      SMBLength[i] = 0;
    }
  }
  
  /** reset the array of receive message buffer length as 0. */
  public static void recreateRMBLength() {
    RMBLength = new int[PARTITIONBUCKET_NUMBER];
    for (int i = 0; i < PARTITIONBUCKET_NUMBER; i++) {
      RMBLength[i] = 0;
    }
  }
  
  /**
   * Compute the array of receive message buffer length.
   * @return int
   */
  public static int computeRMBLength() {
    int count = 0;
    for (int i = 0; i < MetaDataOfMessage.PARTITIONBUCKET_NUMBER; i++) {
      count += MetaDataOfMessage.RMBLength[i];
    }
    return count;
  }
  
  /**
   * Compute the array of send message buffer length.
   * @return int
   */
  public static int computeSMBLength() {
    int count = 0;
    for (int i = 0; i < MetaDataOfMessage.PARTITIONBUCKET_NUMBER; i++) {
      count += MetaDataOfMessage.SMBLength[i];
    }
    return count;
  }
}
