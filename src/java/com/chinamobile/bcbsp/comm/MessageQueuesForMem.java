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


import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * MessageQueuesForMem implements MessageQueuesInterface for only memory storage
 * of the message queues.
 */
public class MessageQueuesForMem implements MessageQueuesInterface {
  /** Outgoing Message Queues hash indexed by <WorkerManagerName:Port>. */
  @SuppressWarnings("unchecked")
  private final ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues =
      new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
  /** Incoming Message Queues hash indexed by vertexID(int). */
  private ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> incomingQueues =
      new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
  /** Incomed Message Queues hash indexed by vertexID(int). */
  private ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> incomedQueues =
      new ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>>();
  /** Counter of next Outgoing queue. */
  private int nextOutgoingQueueCount = 1;
  
  @Override
  public void clearAllQueues() {
    clearIncomedQueues();
    clearIncomingQueues();
    clearOutgoingQueues();
  }
  
  @Override
  public void clearIncomedQueues() {
    this.incomedQueues.clear();
  }
  
  @Override
  public void clearIncomingQueues() {
    this.incomingQueues.clear();
  }
  
  @Override
  public void clearOutgoingQueues() {
    this.outgoingQueues.clear();
  }
  
  @Override
  public void exchangeIncomeQueues() {
    ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> tempQueues =
        this.incomedQueues;
    this.incomedQueues = this.incomingQueues;
    // Note: 20140412 Error Ever for shortest path.....Incomed queque not
    // empty.....
    tempQueues.clear();
    this.incomingQueues = tempQueues;
  }
  
  @Override
  public int getIncomedQueuesSize() {
    int size = 0;
    Entry<String, ConcurrentLinkedQueue<IMessage>> entry;
    Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it =
        this.incomedQueues.entrySet().iterator();
    ConcurrentLinkedQueue<IMessage> queue;
    while (it.hasNext()) {
      entry = it.next();
      queue = entry.getValue();
      size += queue.size();
    }
    return size;
  }
  
  @Override
  public int getIncomingQueuesSize() {
    int size = 0;
    Entry<String, ConcurrentLinkedQueue<IMessage>> entry;
    Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it =
        this.incomingQueues.entrySet().iterator();
    ConcurrentLinkedQueue<IMessage> queue;
    while (it.hasNext()) {
      entry = it.next();
      queue = entry.getValue();
      size += queue.size();
    }
    return size;
  }
  
  @Override
  public String getMaxIncomingQueueIndex() {
    int maxSize = 0;
    String maxIndex = null;
    ConcurrentLinkedQueue<IMessage> tempQueue = null;
    Entry<String, ConcurrentLinkedQueue<IMessage>> entry = null;
    Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it =
        this.incomingQueues.entrySet().iterator();
    while (it.hasNext()) { // Get the queue with the max size
      entry = it.next();
      tempQueue = entry.getValue();
      if (tempQueue.size() > maxSize) {
        maxSize = tempQueue.size();
        maxIndex = entry.getKey();
      } // if
    } // while
    
    return maxIndex;
  }
  
  @Override
  public String getMaxOutgoingQueueIndex() {
    
    int maxSize = 0;
    String maxIndex = null;
    ConcurrentLinkedQueue<IMessage> tempQueue = null;
    Entry<String, ConcurrentLinkedQueue<IMessage>> entry = null;
    Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it =
        this.outgoingQueues.entrySet().iterator();
    while (it.hasNext()) { // Get the queue with the max size
      entry = it.next();
      tempQueue = entry.getValue();
      if (tempQueue.size() > maxSize) {
        maxSize = tempQueue.size();
        maxIndex = entry.getKey();
      }
    }
    return maxIndex;
  }
  
  @Override
  public int getOutgoingQueuesSize() {
    int size = 0;
    Entry<String, ConcurrentLinkedQueue<IMessage>> entry;
    Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it =
        this.outgoingQueues.entrySet().iterator();
    ConcurrentLinkedQueue<IMessage> queue;
    while (it.hasNext()) {
      entry = it.next();
      queue = entry.getValue();
      size += queue.size();
    }
    return size;
  }
  
  @Override
  public void incomeAMessage(String dstVertexID, IMessage msg) {
    ConcurrentLinkedQueue<IMessage> incomingQueue =
        incomingQueues.get(dstVertexID);
    if (incomingQueue == null) {
      incomingQueue = new ConcurrentLinkedQueue<IMessage>();
    }
    incomingQueue.add(msg);
    incomingQueues.put(dstVertexID, incomingQueue);
  }
  
  @Override
  public void outgoAMessage(String outgoingIndex, IMessage msg) {
    ConcurrentLinkedQueue<IMessage> outgoingQueue =
        outgoingQueues.get(outgoingIndex);
    if (outgoingQueue == null) {
      outgoingQueue = new ConcurrentLinkedQueue<IMessage>();
    }
    outgoingQueue.add(msg);
    outgoingQueues.put(outgoingIndex, outgoingQueue);
  }
  
  @Override
  public ConcurrentLinkedQueue<IMessage> removeIncomedQueue(String dstVertID) {
    ConcurrentLinkedQueue<IMessage> incomedQueue = incomedQueues
        .remove(dstVertID);
    if (incomedQueue == null) {
      incomedQueue = new ConcurrentLinkedQueue<IMessage>();
    }
    return incomedQueue;
  }
  
  @Override
  public int getIncomingQueueSize(String dstVertexID) {
    if (incomingQueues.containsKey(dstVertexID)) {
      return incomingQueues.get(dstVertexID).size();
    } else {
      return 0;
    }
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public ConcurrentLinkedQueue<IMessage> removeIncomingQueue(String dstVertID) {
    ConcurrentLinkedQueue<IMessage> incomingQueue = incomingQueues
        .remove(dstVertID);
    if (incomingQueue == null) {
      incomingQueue = new ConcurrentLinkedQueue<IMessage>();
    }
    return incomingQueue;
  }
  
  @Override
  public ConcurrentLinkedQueue<IMessage> removeOutgoingQueue(String index) {
    ConcurrentLinkedQueue<IMessage> outgoingQueue = outgoingQueues
        .remove(index);
    if (outgoingQueue == null) {
      outgoingQueue = new ConcurrentLinkedQueue<IMessage>();
    }
    return outgoingQueue;
  }
  
  @Override
  public int getOutgoingQueueSize(String index) {
    if (outgoingQueues.containsKey(index)) {
      return outgoingQueues.get(index).size();
    } else {
      return 0;
    }
  }
  
  @Override
  public void showMemoryInfo() {
  }
  
  @Override
  public String getNextOutgoingQueueIndex() {
    String nextIndex = null;
    synchronized (this.outgoingQueues) {
      int size = this.outgoingQueues.size();
      if (size == 0) {
        return null;
      }
      if (this.nextOutgoingQueueCount > size) {
        this.nextOutgoingQueueCount = 1;
      }
      Entry<String, ConcurrentLinkedQueue<IMessage>> entry = null;
      Iterator<Entry<String, ConcurrentLinkedQueue<IMessage>>> it =
          this.outgoingQueues.entrySet().iterator();
      for (int i = 0; i < this.nextOutgoingQueueCount; i++) {
        entry = it.next();
        nextIndex = entry.getKey();
      }
      this.nextOutgoingQueueCount++;
    }
    return nextIndex;
  }
  
  /* Zhicheng Liu added */
  @Override
  public IMessage getAMessage() {
    Iterator<String> it = this.incomingQueues.keySet().iterator();
    IMessage mes = null;
    while (it.hasNext()) {
      String vertexID = it.next();
      if (vertexID != null) {
        ConcurrentLinkedQueue<IMessage> messages = this.incomingQueues
            .get(vertexID);
        if (messages == null) {
          continue;
        }
        mes = messages.peek();
      }
      break;
    }
    return mes;
  }

  /** For JUnit test. */
  public ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> getIncomedQueues() {
    return incomedQueues;
  }

  public void setIncomedQueues(
      ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> incomedQueues) {
    this.incomedQueues = incomedQueues;
  }

  public ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> getIncomingQueues() {
    return incomingQueues;
  }

  public void setIncomingQueues(
      ConcurrentHashMap<String, ConcurrentLinkedQueue<IMessage>> incomingQueues) {
    this.incomingQueues = incomingQueues;
  }
}
