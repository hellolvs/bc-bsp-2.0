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

import com.chinamobile.bcbsp.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.UTF8;

/**
 * BSPMessage consists of the tag and the arbitrary amount of data to be
 * communicated. Note Writable Is A Interface For Disk And Network IO
 * Operations.
 */
public class BSPMessage implements IMessage<String, byte[], byte[]> {
  /** the Id of serial version. */
  private static final long serialVersionUID = 1L;
  /** the Id of serial version. */
  private static final int MSGSIZE = 4;
  /** the destination partition-ID. */
  protected int dstPartition;
  /** the destination vertex-ID. */
  protected String dstVertex;
  /** the tag of message. */
  protected byte[] tag;
  /** the data of message. */
  protected byte[] data;
  
  /** default Constructor of BSPMessage. */
  public BSPMessage() {
  }
  
  /**
   * Constructor of BSPMessage.
   * @param tag
   *        of data
   * @param data
   *        of message
   */
  public BSPMessage(int dstPart, String dstVertex, byte[] tag, byte[] data) {
    this.dstPartition = dstPart;
    this.dstVertex = dstVertex;
    this.tag = new byte[tag.length];
    this.data = new byte[data.length];
    System.arraycopy(tag, 0, this.tag, 0, tag.length);
    System.arraycopy(data, 0, this.data, 0, data.length);
  }
  
  /**
   * Constructor of BSPMessage.
   * @param dstVertex
   * @param tag
   * @param data
   */
  public BSPMessage(String dstVertex, byte[] tag, byte[] data) {
    this.dstVertex = dstVertex;
    this.tag = new byte[tag.length];
    this.data = new byte[data.length];
    System.arraycopy(tag, 0, this.tag, 0, tag.length);
    System.arraycopy(data, 0, this.data, 0, data.length);
  }
  
  /**
   * Constructor of BSPMessage.
   * @param dstVertex
   * @param data
   */
  public BSPMessage(String dstVertex, byte[] data) {
    this.dstVertex = dstVertex;
    this.data = new byte[data.length];
    System.arraycopy(data, 0, this.data, 0, data.length);
    this.tag = new byte[0];
  }
  
  /**
   * Constructor of BSPMessage.
   * @param dstVertex
   * @param data
   * @param dstPartition
   */
  public BSPMessage(int dstPartition, String dstVertex, byte[] data) {
    this.dstPartition = dstPartition;
    this.dstVertex = dstVertex;
    this.data = new byte[data.length];
    System.arraycopy(data, 0, this.data, 0, data.length);
    this.tag = new byte[0];
  }
  
  /**
   * Constructor of BSPMessage.
   * @param s
   *        (string)
   */
  // Zhicheng add
  public BSPMessage(String s) {
    String[] infos = s.split(Constants.SPLIT_FLAG);
    if (infos.length == MSGSIZE) {
      this.dstPartition = Integer.parseInt(infos[0]);
      this.dstVertex = infos[1];
      this.tag = infos[2].getBytes();
      this.data = infos[3].getBytes();
    } else {
      this.dstPartition = Integer.parseInt(infos[0]);
      this.dstVertex = infos[1];
      this.data = infos[2].getBytes();
    }
  }
  
  /** get method of dstPartition. */
  public int getDstPartition() {
    return this.dstPartition;
  }
  
  /** set method of dstPartition. */
  public void setDstPartition(int partitionID) {
    this.dstPartition = partitionID;
  }
  
  /** get method of dstVertex. */
  public String getDstVertexID() {
    return this.dstVertex;
  }
  
  /**
   * BSP messages are typically identified with tags. This allows to get the tag
   * of data.
   * @return tag of data of BSP message
   */
  public byte[] getTag() {
    byte[] result = this.tag;
    return result;
  }
  
  /**
   * @return data of BSP message
   */
  public byte[] getData() {
    byte[] result = this.data;
    return result;
  }
  
  public String intoString() {
    String buffer = this.dstPartition + Constants.SPLIT_FLAG + this.dstVertex +
        Constants.SPLIT_FLAG +
        // new String(this.tag) + Constants.SPLIT_FLAG +
        new String(this.data);
    return buffer;
  }
  
  /**the rewrite method of formString*/
  public void fromString(String msgData) {
    String[] buffer = msgData.split(Constants.SPLIT_FLAG);
    this.dstPartition = Integer.parseInt(buffer[0]);
    this.dstVertex = buffer[1];
    // this.tag = buffer[2].getBytes();
    this.data = buffer[2].getBytes();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.dstPartition = in.readInt();
    int a = in.readInt();
    this.data = new byte[a];
    in.readFully(data, 0, this.data.length);
    this.dstVertex = UTF8.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(dstPartition);
    out.writeInt(data.length);
    out.write(data);
    UTF8.writeString(out, dstVertex);
  }

  // Note Add Method By liu Jinpeng 20140122 For Generic Type Usage
  @Override
  public String getMessageId() {
    return this.dstVertex;
  }

  @Override
  public void setMessageId(String id) {
    this.dstVertex = id;
  }
  
  @Override
  public byte[] getContent() {
    return this.data;
  }
  
  @Override
  public void setContent(byte[] content) {
    this.data = content;
  }
  
  @Override
  public void setTag(byte[] tag) {
    // TODO Auto-generated method stub
  }
  
  @Override
  public int getTagLen() {
    if (this.tag == null) {
      return 0;
    }
    return this.tag.length;
  }
  
  @Override
  public int getContentLen() {
    if (this.data == null) {
      return 0;
    }
    return this.data.length;
  }
  
  /** return the Message's bytes in memory. */
  public long size() {
    if (tag == null) {
      return MSGSIZE + dstVertex.length() * 2 + data.length;
    }
    return  MSGSIZE + dstVertex.length() * 2 + tag.length + data.length;
  }
  
  @Override
  public boolean combineIntoContainer(
      Map<String, IMessage<String, byte[], byte[]>> container) {
    // TODO Auto-generated method stub
    return false;
  }
  
  /** get method of serialVersionUID. */
  public static long getSerialversionuid() {
    return serialVersionUID;
  }
  
  /** get method of dstVertex. */
  public String getDstVertex() {
    return dstVertex;
  }
  
  /** set method of dstVertex. */
  public void setDstVertex(String dstVertex) {
    this.dstVertex = dstVertex;
  }
  
  /** set method of data. */
  public void setData(byte[] data) {
    this.data = data;
  }
}
