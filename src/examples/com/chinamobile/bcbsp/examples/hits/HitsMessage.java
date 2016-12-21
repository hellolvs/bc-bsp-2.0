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

package com.chinamobile.bcbsp.examples.hits;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.comm.IMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Hits message.
 */
public class HitsMessage implements IMessage<Integer, Double, Integer> {
  /** Id of message */
  private Integer messageId;
  /** value of message */
  private String value;
  
  private int srcVertexID;
  

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(messageId);
    out.writeUTF(value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.messageId = in.readInt();
    this.value = in.readUTF();
  }

  @Override
  public int getDstPartition() {
    return -1;
  }

  @Override
  public void setDstPartition(int partitionID) {

  }

  
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
  public Double getContent() {
    return null;
  }

  @Override
  public int getContentLen() {
    return this.value.length();
  }

  @Override
  public void setContent(Double content) {
    //value = content;
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
      Map<String, IMessage<Integer, Double, Integer>> container) {


	    IMessage tmp = container.get(messageId);
	    if (tmp != null) {
	      String val = (String) tmp.getContent() + value;
	      tmp.setContent(val);
	      return true;
	    } else {
	      container.put(String.valueOf(messageId), this);
	      return false;
	    }
	  
  
  }
  public int getSrcVertexID(){
	  return this.srcVertexID;
  }
  public void setSrcVertexID(int srcID){
	  this.srcVertexID=srcID;
  }
  public String getNewContent(){
	  return this.value;
  }
  public void setNewContent(String content,Integer srcID){
	  value = content+" "+srcID.toString();
  }
}