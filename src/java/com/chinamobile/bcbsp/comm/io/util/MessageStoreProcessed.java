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

package com.chinamobile.bcbsp.comm.io.util;

import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.comm.MetaDataOfMessage;
import com.chinamobile.bcbsp.comm.PartitionRule;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//Note One Message One Vertex.

public class MessageStoreProcessed extends MessageStore {
  /** current Message Objects. */
  private HashMap<String, IMessage> currentMessageObjects;
  
 // private static final Log LOG = LogFactory.getLog(BSPStaff.class);
  
  /**
   * Constructor of MessageStoreListProcessed.
   * @param cachePartitionNum
   */
  public MessageStoreProcessed(int cachePartitionNum) {
    super(cachePartitionNum);
    initializeMessageObjects();
  }
  
  /** Initialize current Message Objects. */
  private void initializeMessageObjects() {
    this.currentMessageObjects = new HashMap<String, IMessage>();
  }
  
  /**
   * Message From BytesPool To Object Organization.BytePool Is Ready.
   * @param partitionId
   * @param superstep
   */
  public void preLoadMessages(int partitiId, int superstep) {
    this.currentMessageObjects.clear();
    MessageIterator iter = this.getmCache().removeMsgBytePool(partitiId)
        .getIterator();
    //int i = 0;
    while (iter.hasNext()) {
      iter.next();
      // Note Force To Change .Error Ever.
      //LOG.info("Feng test preLoadMessages! "+i);
      IMessage o = iter.getCurrentMessage();
      boolean flag = o.combineIntoContainer(this.currentMessageObjects);
      //i++;
      if (!flag) {
        iter.releaseCurrentMessage();
      }
    }
  }
  
  /**
   * Add the byte array message poll into message objects.
   * @param messages
   */
  public void addMsgBytePoolToMsgObjects(MessageBytePoolPerPartition messages) {
    MessageIterator iter = messages.getIterator();
    
    while (iter.hasNext()) {
      iter.next();
      // Note Force To Change .Error Ever.
      IMessage o = iter.getCurrentMessage();
      boolean flag = o.combineIntoContainer(this.currentMessageObjects);
      if (!flag) {
        iter.releaseCurrentMessage();
      }
    }
  }
  
  /**
   * Remove message from one vertex.
   * @param vertex
   * @param partitionId
   * @return
   */
  public ArrayList<IMessage> removeMessagesForOneVertex(String vertex,
      int partitionId) {
    IMessage msg = this.currentMessageObjects.get(vertex);
    if (msg == null) {
      return null;
    }
    ArrayList<IMessage> msgs = new ArrayList<IMessage>();
    msgs.add(msg);
    return msgs;
  }
  
  /** Clear current message object for reused. */
  public void clean() {
    this.currentMessageObjects.clear();
  }
  
//  /**
//   * For Update The ByteArray Message For Next Superstep In Exchage Procedure.
//   * @param msgStore
//   */
//  public void refreshMessageBytePools(MessageStore msgStore) {
//    for (int i = 0; i < MetaDataOfMessage.PARTITIONBUCKET_NUMBER; i++) {
//      int bucketId = PartitionRule.getBucket(i);
//      this.getmCache().addMessageBytePool(msgStore.removeMessageBytesPool(i),
//          bucketId);
//    }
//  }

  /** For JUnit test. */
  public HashMap<String, IMessage> getCurrentMessageObjects() {
    return currentMessageObjects;
  }
  //Biyahui
  public void preLoadMessagesNew(int partitiId, int superstep) {
	    this.currentMessageObjects.clear();
	    MessageIterator iter = this.getmCache().removeMsgBytePool(partitiId)
	        .getIterator();
	    //int i = 0;
	    while (iter.hasNext()) {
	      iter.next();
	      // Note Force To Change .Error Ever.
	      //LOG.info("Feng test preLoadMessages! "+i);
	      IMessage o = iter.getCurrentMessage();
	      boolean flag = o.combineIntoContainer(this.currentMessageObjects);
	      //i++;
	      if (!flag) {
	        iter.releaseCurrentMessage();
	      }
	    }
	  }
}
