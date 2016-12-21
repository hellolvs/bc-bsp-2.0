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

package com.chinamobile.bcbsp.examples.simrank;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.comm.IMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * PageRank message.
 */
public class SRmessage implements IMessage<String, Double, Integer> {
  /** Id of message */
  private String messageId;
  /** value of message */
  private double value;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(messageId);
    out.writeDouble(value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.messageId = in.readUTF();
    this.value = in.readDouble();
  }

  @Override
  public int getDstPartition() {
    return -1;
  }

  @Override
  public void setDstPartition(int partitionID) {

  }

  @Override
  public String getDstVertexID() {
    return String.valueOf(messageId);
  }

  @Override
  public String getMessageId() {
    return messageId;
  }

  @Override
  public void setMessageId(String id) {
    messageId = id;
  }

  @Override
  public Integer getTag() {
    return null;
  }

  @Override
  public void setTag(Integer tag) {
  }

  @Override
  public int getTagLen() {
    return 0;
  }

  @Override
  public Integer getData() {
    return null;
  }

  @Override
  public Double getContent() {
    return value;
  }

  @Override
  public int getContentLen() {
    return 8;
  }

  @Override
  public void setContent(Double content) {
    value = content;
  }

  @Override
  public String intoString() {
	  String message = this.getDstVertexID() + Constants.SPLIT_FLAG + String.valueOf(this.value);
    return message;
  }

  @Override
  public void fromString(String msgData) {
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public boolean combineIntoContainer(
      Map<String, IMessage<String, Double, Integer>> container) {

      return false;
    
  }
}