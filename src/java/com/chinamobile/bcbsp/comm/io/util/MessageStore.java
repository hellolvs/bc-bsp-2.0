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

import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.comm.MetaDataOfMessage;
import com.chinamobile.bcbsp.comm.PartitionRule;

/**
 * Message store.
 * 
 * @author Liu Jinpeng.
 */
public abstract class MessageStore {
	/** message cache of receiver. */
	private MessageReceiveCache mCache;

	/**
	 * Constructor of MessageReceiveCache.
	 * 
	 * @param cachePartitionNum
	 *            int
	 */
	public MessageStore(int cachePartitionNum) {
		this.mCache = new MessageReceiveCache(cachePartitionNum);
	}

	/** Initialize MessageStore. */
	public void initialize() {
	}

	/**
	 * Add byte array message partition into pool and process for disk flush.
	 * 
	 * @param msgBytes
	 *            MessageBytePoolPerPartition
	 * @param partitionId
	 *            int
	 * @return int
	 */
	public int add(MessageBytePoolPerPartition msgBytes, int partitionId) {
		return this.mCache.addMessageBytePool(msgBytes, partitionId);
	}

	public void clear() {
	}

	/**
	 * Remove byte array message partition from message pool.
	 * 
	 * @param partitionId
	 * @return MessageBytePoolPerPartition
	 */
	public MessageBytePoolPerPartition removeMessageBytesPool(int partitionId) {
		return this.mCache.removeMsgBytePool(partitionId);
	}

	/**
	 * Get the total counter for numbers of all MessageBytePoolPerPartitions.
	 * 
	 * @return int
	 */
	public int getTotalMessageCount() {
		return this.mCache.getTotalCount();
	}

	/**
	 * Get the total counter for sizes of all MessageBytePoolPerPartitions.
	 * 
	 * @return int
	 */
	public int getTotalMessageSize() {
		return this.mCache.getTotalSize();
	}

	/**
	 * Get method of MessageReceiveCache.
	 * 
	 * @return MessageReceiveCache
	 */
	public MessageReceiveCache getmCache() {
		return mCache;
	}

	/**
	 * For Update The ByteArray Message For Next Superstep In Exchage Procedure.
	 * 
	 * @param msgStore
	 */
	public void refreshMessageBytePools(MessageStore msgStore) {
		for (int i = 0; i < MetaDataOfMessage.PARTITIONBUCKET_NUMBER; i++) {
			int bucketId = PartitionRule.getBucket(i);
			this.getmCache().addMessageBytePool(
					msgStore.removeMessageBytesPool(i), bucketId);
		}
	}

	public abstract void clean();

	public abstract ArrayList<IMessage> removeMessagesForOneVertex(
			String dstVertexID, int hashIndex);

	public abstract void addMsgBytePoolToMsgObjects(
			MessageBytePoolPerPartition singletonBytePartition);

	public abstract void preLoadMessages(int bucket, int superStep);
	/*Biyahui added*/
	public abstract void preLoadMessagesNew(int bucket, int superStep);
}
