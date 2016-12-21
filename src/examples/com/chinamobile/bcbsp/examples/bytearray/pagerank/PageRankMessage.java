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

package com.chinamobile.bcbsp.examples.bytearray.pagerank;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.comm.IMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.mortbay.log.Log;

/**
 * PageRank message.
 */
public class PageRankMessage implements IMessage<Integer, Float, Integer> {
  /** Id of message */
  private int messageId;
  /** value of message */
  private float value;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(messageId);
    out.writeFloat(value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.messageId = in.readInt();
    this.value = in.readFloat();
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
  public Integer getMessageId() {
    return messageId;
  }

  @Override
  public void setMessageId(Integer id) {
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
  public Float getContent() {
    return value;
  }

  @Override
  public int getContentLen() {
    return 4;
  }

  @Override
  public void setContent(Float content) {
    value = content;
  }

  @Override
  public String intoString() {
	  String message = this.getDstVertexID() + Constants.SPLIT_FLAG + String.valueOf(this.value);
    return message;
  }

  @Override
  public void fromString(String msgData) {
	  String[] entry=msgData.split(Constants.SPLIT_FLAG);
	  this.setMessageId(Integer.parseInt(entry[0]));
	  this.setContent(Float.parseFloat(entry[1]));
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public boolean combineIntoContainer(
      Map<String, IMessage<Integer, Float, Integer>> container) {
    IMessage tmp = container.get(messageId);
    if (tmp != null) {
      float val = (Float) tmp.getContent() + value;
      tmp.setContent(val);
      return true;
    } else {
      container.put(messageId+"", this);
      return false;
    }
  }
}