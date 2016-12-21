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

import com.chinamobile.bcbsp.comm.io.util.WritableBSPMessages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * BSPMessagesPack. To pack the messages into array list for sending and
 * receiving all together.
 */
public class BSPMessagesPack implements Serializable, WritableBSPMessages {
  /** Class logger. */
  private static final Log LOG = LogFactory.getLog(BSPMessagesPack.class);
  /** the ID of serial version. */
  private static final long serialVersionUID = 1L;
  /** list of message to pack. */
  private ArrayList<IMessage> pack;
  
  /**
   * Constructor of BSPMessage.
   */
  public BSPMessagesPack() {
  }
  
  /** set method of pack. */
  public void setPack(ArrayList<IMessage> pack) {
    this.pack = pack;
  }
  
  /** get method of pack. */
  public ArrayList<IMessage> getPack() {
    return this.pack;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int i = 0;
    IMessage bspMsg = null;
    int length = in.readInt();
    pack = new ArrayList<IMessage>();
    try {
      for (int j = 0; j < length; j++) {
        bspMsg = CommunicationFactory.createBspMessage();
        bspMsg.readFields(in);
        pack.add(bspMsg);
      }
    } catch (Exception e) {
      throw new RuntimeException("[RPC BSPMsgPack  write Exception]", e);
    }
    return;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    int i = 0;
    Iterator<IMessage> bspMsgs = pack.iterator();
    out.writeInt(pack.size());
    try {
      while (bspMsgs.hasNext()) {
        IMessage bspMsg = (IMessage) bspMsgs.next();
        bspMsg.write(out);
      }
    } catch (Exception e) {
      throw new RuntimeException("[RPC BSPMsgPack  write Exception]", e);
    }
    return;
  }
  
  @Override
  public int getMsgSize() {
    return 0;
  }
  
  @Override
  public int getMsgCount() {
    return this.pack.size();
  }
  
  @Override
  public void clearBSPMsgs() {
    // TODO Auto-generated method stub
  }
}
