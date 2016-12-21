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

package com.chinamobile.bcbsp.examples;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * AggregateValueMsgCount An example implementation of AggregateValue.
 */
public class AggregateValueMsgCount extends AggregateValue<Long, BSPMessage> {
  /** State the count of message */
  private Long msgCount;

  public Long getValue() {
    return this.msgCount;
  }

  public void setValue(Long value) {
    this.msgCount = value;
  }

  /**
   * Transform messageCount to string.
   * @return msgCount
   */
  public String toString() {
    return this.msgCount.toString();
  }

  /**
   * Transform messageCount to long.
   * @param msgNum messageCount
   */
  public void initValue(String msgNum) {
    this.msgCount = Long.valueOf(msgNum);
  }

  /**
   * Get the count of message.
   * @param messages BSPMessage
   * @param context AggregationContextInterface
   */
  public void initValue(Iterator<BSPMessage> messages,
      AggregationContextInterface context) {
    long count = 0;
    while (messages.hasNext()) {
      count++;
      messages.next();
    }
    this.msgCount = count;
  }

  /**
   * Deserialize the messageCount.
   * @param in messageCount
   */
  public void readFields(DataInput in) throws IOException {
    this.msgCount = in.readLong();
  }

  /**
   * Serialize the messageCount.
   * @param out messageCount
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.msgCount);
  }
}
