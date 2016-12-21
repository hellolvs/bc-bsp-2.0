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

import java.util.Map;

import org.apache.hadoop.io.Writable;

/**
 * BSPMessage consists of the tag and the arbitrary amount of data to be
 * communicated.
 * @param <I>
 *        Vertex id
 * @param <V>
 *        Vertex data
 * @param <T>
 *        tag
 */
public interface IMessage<I, V, T> extends Writable {
  
  /**
   * Get the destination partition ID.
   * @return int
   */
  int getDstPartition();
  
  /**
   * Set the destination partition ID.
   * @param partitionID
   *        int
   */
  void setDstPartition(int partitionID);
  
  /**
   * Set the destination partition ID.
   * @return int
   */
  String getDstVertexID();
  
  /**
   * Get the message ID.
   * @return I
   */
  I getMessageId();
  
  /**
   * Set the message ID.
   * @param id
   *        I
   */
  void setMessageId(I id);
  
  /**
   * BSP messages are typically identified with tags. This allows to get the tag
   * of data.
   * @return tag of data of BSP message
   */
  T getTag();
  
  /**
   * Set tag.
   * @param tag
   *        T
   */
  void setTag(T tag);
  
  /**
   * Get length of tag.
   * @return int
   */
  int getTagLen();
  
  /**
   * Get data of message.
   * @return data of BSP message
   */
  T getData();
  
  /**
   * Get data of message.
   * @return data of BSP message
   */
  V getContent();
  
  /**
   * Get content(data) of message.
   * @return data of BSP message
   */
  int getContentLen();
  /**
   * Set content.
   * @param content
   *        V
   */
  void setContent(V content);
  
  /**
   * Into String of Message.
   * @return String
   */
  String intoString();
  
  /**
   * From String of Message.
   * @param msgData
   *        String
   */
  void fromString(String msgData);
  
  /**
   * Compute the size of message.
   * @return long
   */
  long size();
  
  /**
   * Combine messages loading from disk.
   * If true, combine successfully, the message object can't be reused.
   * @param container Map<I, IMessage<I, V, T>>
   * @return boolean
   */
  boolean combineIntoContainer(Map<String, IMessage<I, V, T>> container);
}
