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

import com.chinamobile.bcbsp.comm.BSPMessagesPack;
import com.chinamobile.bcbsp.comm.CommunicationFactory;
import com.chinamobile.bcbsp.comm.IMessage;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Common implementation for VertexIdEdgeIterator, VertexIdMessageIterator and
 * VertexIdMessageBytesIterator.
 */
public class MessageIterator {
  /** Class logger. */
  private static final Log LOG = LogFactory.getLog(MessageIterator.class);
  /** reader of the serialized edges. */
  private final ExtendedDataInput extendedDataInput;
  /** current vertex id. */
  private IMessage msg;
  
  /**
   * Constructor.
   * @param extendedDataOutput
   *        Extended data output
   */
  public MessageIterator(ExtendedDataOutput extendedDataOutput) {
    extendedDataInput = CommunicationFactory.createExtendedDataInput(
        extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());
  }
  
  /**
   * Returns true if the iteration has more elements.
   * @return True if the iteration has more elements.
   */
  public boolean hasNext() {
    return extendedDataInput.available() > 0;
  }
  
  /**
   * Moves to the next element in the iteration.
   */
  public void next() {
    if (this.msg == null) {
      this.msg = CommunicationFactory.createPagerankMessage();
    }
    try {
      this.msg.readFields(extendedDataInput);
      // Log.info("<####Debug>"+msg.getDstVertexID()+ "  " +msg.getContent());
    } catch (IOException e) {
      throw new RuntimeException(
          "[MessageBytePoolPerPartition] next exception: ", e);
    }
  }
  
  /**
   * Get the current vertex id. Ihis object's contents are only guaranteed until
   * next() is called. To take ownership of this object call
   * releaseCurrentVertexId() after getting a reference to this object.
   * @return Current vertex id
   */
  public IMessage getCurrentMessage() {
    return msg;
  }
  
  /**
   * The backing store of the current vertex id is now released. Further calls
   * to getCurrentVertexId () without calling next() will return null.
   * @return Current vertex id that was released
   */
  public IMessage releaseCurrentMessage() {
    IMessage releasedMsgId = msg;
    msg = null;
    return releasedMsgId;
  }
}
