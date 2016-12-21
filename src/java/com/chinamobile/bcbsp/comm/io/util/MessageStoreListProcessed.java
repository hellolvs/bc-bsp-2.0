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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.comm.MetaDataOfMessage;
import com.chinamobile.bcbsp.comm.PartitionRule;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * Message of list.
 */
public class MessageStoreListProcessed extends MessageStore implements
    IMessageStoreProcessed {
//	private static final Log LOG = LogFactory.getLog(MessageStoreProcessed.class);
	
	/** flag of combiner. */
	  private boolean combinerFlag = false;
	  /** combiner of receiver. */
	  private Combiner combiner = null;
	  
	  BSPJob job = null;
	  
  /** current Message Objects. */
  private HashMap<String, List<IMessage>> currentMessageObjects;
  
  /**
   * Constructor of MessageStoreListProcessed.
   * @param cachePartitionNum
   */
  public MessageStoreListProcessed(int cachePartitionNum,BSPJob job) {
    super(cachePartitionNum);
    initializeMessageObjects();
    this.job = job;
  }
  
  /** Initialize current Message Objects. */
  private void initializeMessageObjects() {
    this.currentMessageObjects = new HashMap<String, List<IMessage>>();
  }
  
  // Message From BytesPool To Object Organization.BytePool Is Ready.
  @Override
  public void preLoadMessages(int partitionId, int superstep) {
    this.currentMessageObjects.clear();
    MessageIterator iter = this.getmCache().removeMsgBytePool(partitionId)
        .getIterator();
    ArrayList<String> idArrayList = new ArrayList();
    while (iter.hasNext()) {
      iter.next();
      // Note Force To Change .Error Ever.
      IMessage o = iter.getCurrentMessage();
      String id = o.getMessageId().toString();
      List mList = this.currentMessageObjects.get(id);
      if (mList == null) {
        mList = new ArrayList<IMessage>();
        this.currentMessageObjects.put(id, mList);
        idArrayList.add(id);
      }
      mList.add(o);
      iter.releaseCurrentMessage();
    }
    this.combinerFlag = job.isReceiveCombinerSetFlag();
    if (combinerFlag) {
    Iterator idArrayListIte = idArrayList.iterator();
    while(idArrayListIte.hasNext()){
    	String mId = idArrayListIte.next().toString();
        List tmp = this.currentMessageObjects.get(mId);
        Iterator it = tmp.iterator();
       
        	this.combiner = this.getCombiner();
        	IMessage message = (IMessage)combiner.combine(it);
        	List mList1 = new ArrayList<IMessage>();
        	mList1.add(message);
        	this.currentMessageObjects.put(mId, mList1);
        }
    }
  }
  /**
   * Biyahui added
   * @param bucket
   * @param superStep
   */
  @Override
  public void preLoadMessagesNew(int partitionId, int superstep) {
    this.currentMessageObjects.clear();
    MessageIterator iter = this.getmCache().getBytePoolPerPartition(partitionId)
        .getIterator();
    ArrayList<String> idArrayList = new ArrayList();
    while (iter.hasNext()) {
      iter.next();
      // Note Force To Change .Error Ever.
      IMessage o = iter.getCurrentMessage();
      String id = o.getMessageId().toString();
      List mList = this.currentMessageObjects.get(id);
      if (mList == null) {
        mList = new ArrayList<IMessage>();
        this.currentMessageObjects.put(id, mList);
        idArrayList.add(id);
      }
      mList.add(o);
      iter.releaseCurrentMessage();
    }
    this.combinerFlag = job.isReceiveCombinerSetFlag();
    if (combinerFlag) {
    Iterator idArrayListIte = idArrayList.iterator();
    while(idArrayListIte.hasNext()){
    	String mId = idArrayListIte.next().toString();
        List tmp = this.currentMessageObjects.get(mId);
        Iterator it = tmp.iterator();
       
        	this.combiner = this.getCombiner();
        	IMessage message = (IMessage)combiner.combine(it);
        	List mList1 = new ArrayList<IMessage>();
        	mList1.add(message);
        	this.currentMessageObjects.put(mId, mList1);
        }
    }
  }
  
  public void preLoadPeerMessages(int bucket, int superStep){
    this.currentMessageObjects.clear();
    MessageIterator iter = this.getmCache().removeMsgBytePool(bucket)
        .getIterator();
    int i=0;
    while (iter.hasNext()) {
      iter.next();
      // Note Force To Change .Error Ever.
      try {
        IMessage o = iter.getCurrentMessage();
        String id =String.valueOf((Constants.DEFAULT_PEER_DST_MESSSAGE_ID));
      List mList = this.currentMessageObjects.get(id);
      if (mList == null) {
        mList = new ArrayList<IMessage>();
        this.currentMessageObjects.put(id, mList);
      }
      mList.add(o);
      }catch(Exception e){
        throw new RuntimeException(e);
      }
      iter.releaseCurrentMessage();
      i++;
    }
  }
  
public Combiner getCombiner() {
	  
	    if (this.job != null) {
	      return (Combiner) ReflectionUtils.newInstance(
	          job.getConf().getClass(Constants.USER_BC_BSP_JOB_COMBINER_CLASS,
	              Combiner.class), job.getConf());
	    } else {
	      return null;
	    }
	  }
  
  @Override
  public void addMsgBytePoolToMsgObjects(MessageBytePoolPerPartition messages) {
    MessageIterator iter = messages.getIterator();
    ArrayList<String> idArrayList = new ArrayList();
    while (iter.hasNext()) {
      iter.next();
      // Note Force To Change .Error Ever.
      IMessage o = iter.getCurrentMessage();
      String id = o.getMessageId().toString();
      List mList = this.currentMessageObjects.get(id);
      if (mList == null) {
        mList = new ArrayList<IMessage>();
        this.currentMessageObjects.put(id, mList);
        idArrayList.add(id);
      }
      mList.add(o);
      iter.releaseCurrentMessage();
    }
    
    this.combinerFlag = job.isReceiveCombinerSetFlag();
    if (combinerFlag) {
    Iterator idArrayListIte = idArrayList.iterator();
    while(idArrayListIte.hasNext()){
    	String mId = idArrayListIte.next().toString();
        List tmp = this.currentMessageObjects.get(mId);
        Iterator it = tmp.iterator();
       
        	this.combiner = this.getCombiner();
        	IMessage message = (IMessage)combiner.combine(it);
        	List mList1 = new ArrayList<IMessage>();
        	mList1.add(message);
        	this.currentMessageObjects.put(mId, mList1);
        }
    }
  }
  
  @Override
  public ArrayList<IMessage> removeMessagesForOneVertex(String vertex,
      int partitionId) {
    return (ArrayList<IMessage>) this.currentMessageObjects.remove(vertex);
  }
  
  @Override
  public void clean() {
    this.currentMessageObjects.clear();
  }
  
  // Note For Update The ByteArray Message For Next Superstep In Exchage
  // Procedure.
  @Override
  public void refreshMessageBytePools(MessageStore msgStore) {
    for (int i = 0; i < MetaDataOfMessage.PARTITIONBUCKET_NUMBER; i++) {
      int bucketId = PartitionRule.getBucket(i);
      this.getmCache().addMessageBytePool(msgStore.removeMessageBytesPool(i),
          bucketId);
    }
  }
}
