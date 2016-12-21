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

import com.chinamobile.bcbsp.api.Combiner;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * CombinerTool is a single thread belongs to a Sender, to do combination
 * operation for outgoing queues that is over the sendingCombineThreshold.
 */
public class CombinerTool extends Thread {
  
  /** Class logger. */
  private static final Log LOG = LogFactory.getLog(CombinerTool.class);
  /** time change. */
  private static float TIME_CHANGE = 1000f;
  /** manages message queues. */
  private MessageQueuesInterface messageQueues = null;
  /** combiner. */
  private Combiner combiner = null;
  /** default number of combiner threshold. */
  private int combineThreshold = 1000;
  /** sender of combine. */
  private CombineSenderInterface sender = null;
  /**
   * Notes the count since last combination of the queue indexed by String. So
   * that if the new increasing count overs the threshold, we will combine them,
   * otherwise, do not combine them.
   */
  private HashMap<String, Integer> combinedCountsMap =
      new HashMap<String, Integer>();
  
  /**
   * Constructor of CombinerTool.
   * @param aSender
   * @param msgQueues
   * @param combiner
   * @param threshold
   */
  public CombinerTool(CombineSenderInterface aSender,
      MessageQueuesInterface msgQueues, Combiner combiner, int threshold) {
    this.sender = aSender;
    this.messageQueues = msgQueues;
    this.combiner = combiner;
    this.combineThreshold = threshold;
  }
  
  /** run method. */
  public void run() {
    LOG.info("[CombinerTool] Start!");
    long time = 0;
    long totalBeforeSize = 0;
    long totalAfterSize = 0;
    String outgoIndex = null;
    ConcurrentLinkedQueue<IMessage> maxQueue = null;
    int maxSize = 0;
    int lastCount = 0;
    while (!this.sender.getNoMoreMessagesFlag()) {
      outgoIndex = this.messageQueues.getMaxOutgoingQueueIndex();
      if (outgoIndex != null) {
        maxSize = this.messageQueues.getOutgoingQueueSize(outgoIndex);
      }
      lastCount = (this.combinedCountsMap.get(outgoIndex) == null ? 0
          : this.combinedCountsMap.get(outgoIndex));
      // If new updated size is over the threshold
      if ((maxSize - lastCount) > this.combineThreshold) {
        // Get the queue out of the map.
        maxQueue = this.messageQueues.removeOutgoingQueue(outgoIndex);
        if (maxQueue == null) {
          continue;
        }
        long start = System.currentTimeMillis();
        /* Clock */
        // Combine the messages.
        maxQueue = combine(maxQueue);
        long end = System.currentTimeMillis();
        /* Clock */
        time = time + end - start;
        int maxSizeBefore = maxSize;
        totalBeforeSize += maxSizeBefore;
        maxSize = maxQueue.size();
        totalAfterSize += maxSize;
        // Note the count after combination.
        this.combinedCountsMap.put(outgoIndex, maxQueue.size());
        // Put the combined messages back to the outgoing queue.
        Iterator<IMessage> iter = maxQueue.iterator();
        while (iter.hasNext()) {
          this.messageQueues.outgoAMessage(outgoIndex, iter.next());
        }
        maxQueue.clear();
      } else {
        try {
          // Wait for 500ms until next check.
          Thread.sleep(500);
        } catch (Exception e) {
          throw new RuntimeException("[[CombinerTool] run exception", e);
        }
      }
    }
    LOG.info("[CombinerTool] has combined totally (" + totalBeforeSize
        + ") messages into (" + totalAfterSize + "). Compression rate = "
        + (float) totalAfterSize * 100 / totalBeforeSize + "%.");
    LOG.info("[CombinerTool] has used time: " + time / TIME_CHANGE
        + " seconds totally!");
    LOG.info("[CombinerTool] Die!");
  }
  
  /** combine the message queues.
   * @param outgoingQueue
   */
  private ConcurrentLinkedQueue<IMessage> combine(
      ConcurrentLinkedQueue<IMessage> outgoingQueue) {
    // Map of outgoing queues indexed by destination vertex ID.
    TreeMap<String, ConcurrentLinkedQueue<IMessage>> outgoingQueues =
        new TreeMap<String, ConcurrentLinkedQueue<IMessage>>();
    ConcurrentLinkedQueue<IMessage> tempQueue = null;
    IMessage tempMessage = null;
    // Traverse the outgoing queue and put the messages with the same
    // dstVertexID into the same queue in the tree map.
    Iterator<IMessage> iter = outgoingQueue.iterator();
    String dstVertexID = null;
    /**The result queue for return.*/
    ConcurrentLinkedQueue<IMessage> resultQueue =
        new ConcurrentLinkedQueue<IMessage>();
    while (iter.hasNext()) {
      tempMessage = iter.next();
      dstVertexID = tempMessage.getDstVertexID();
      tempQueue = outgoingQueues.get(dstVertexID);
      if (tempQueue == null) {
        tempQueue = new ConcurrentLinkedQueue<IMessage>();
      }
      tempQueue.add(tempMessage);
      outgoingQueues.put(dstVertexID, tempQueue);
    }
    // Do combine operation for each of the outgoing queues.
    for (Entry<String, ConcurrentLinkedQueue<IMessage>> entry : outgoingQueues
        .entrySet()) {
      tempQueue = entry.getValue();
      tempMessage = (IMessage) this.combiner.combine(tempQueue.iterator());
      resultQueue.add(tempMessage);
    }
    outgoingQueue.clear();
    outgoingQueues.clear();
    return resultQueue;
  }
}
