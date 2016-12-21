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

package com.chinamobile.bcbsp.examples.semicluster;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.examples.semicluster.SemiClusterBSP.SemiClusterTreeSetWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * SemiCluster message.
 */
public class SemiClusterMessage implements
    IMessage<Integer, SemiClusterTreeSetWritable, Integer> {
  /** Id of message */
  private int messageId;
  /** value of message */
  private SemiClusterTreeSetWritable value = new SemiClusterTreeSetWritable();
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(messageId);
    value.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.messageId = in.readInt();
    value.readFields(in);
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
  public SemiClusterTreeSetWritable getContent() {
    return value;
  }
  
  @Override
  public int getContentLen() {
    return 4;
  }
  
  @Override
  public void setContent(SemiClusterTreeSetWritable content) {
    value = content;
  }
  
  @Override
  public String intoString() {
    String message = this.getDstVertexID() + Constants.SPLIT_FLAG
        + value.intoString();
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
      Map<String, IMessage<Integer, SemiClusterTreeSetWritable, Integer>> container) {
    IMessage tmp = container.get(messageId);
    container.put(String.valueOf(messageId), this);
    return false;
  }
  
}
