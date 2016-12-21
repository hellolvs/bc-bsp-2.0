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

import com.chinamobile.bcbsp.comm.io.util.ExtendedByteArrayDataInput;
import com.chinamobile.bcbsp.comm.io.util.ExtendedByteArrayDataOutput;
import com.chinamobile.bcbsp.comm.io.util.ExtendedDataInput;
import com.chinamobile.bcbsp.comm.io.util.ExtendedDataOutput;
import com.chinamobile.bcbsp.comm.io.util.MessageBytePoolPerPartition;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
/**
 * Constructor of GraphDataFactory. Providing The Unified Interface.
 * @author Liu Jinpeng
 */
public class CommunicationFactory {
  /** message class. */
  private static Class messageClass;
  /** Class logger. */
  private static final Log LOG = LogFactory.getLog(CommunicationFactory.class);
  /**
   * Constructor of CommunicationFactory.
   */
  private CommunicationFactory() {
  }
  
  /**
   * Set method of messageClass.
   * @param cls
   */
  public static void setMessageClass(Class cls) {
    messageClass = cls;
    LOG.info("[Message Class Name Is] " + messageClass.getName());
  }
  
  /** Create the message byte thread pool of per partition. */
  public static MessageBytePoolPerPartition createMsgBytesPoolPerPartition() {
    return new MessageBytePoolPerPartition(10000);
  }
  
  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in bytes.
   * @param size
   * @return ExtendedDataOutput
   */
  public static ExtendedDataOutput createBytesPool(int size) {
    return new ExtendedByteArrayDataOutput(size);
  }
  
  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in bytes.
   * @param size
   * @param buf
   * @return ExtendedDataOutput
   */
  public static ExtendedDataOutput createBytesPool(byte[] buf, int size) {
    return new ExtendedByteArrayDataOutput(buf, size);
  }
  
  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in bytes.
   * @param buf
   *        byte[]
   * @param length
   *        int
   * @return ExtendedDataOutput
   */
  public static ExtendedDataInput createExtendedDataInput(byte[] buf, int off,
      int length) {
    return new ExtendedByteArrayDataInput(buf, off, length);
  }
  
  /**
   * Create BSP message.
   * @return IMessage
   */
  public static IMessage createBspMessage() {
    return new BSPMessage();
  }
  
  /**
   * Create Pagerank message.
   * @return IMessage
   */
  public static IMessage createPagerankMessage() {
    try {
      return (IMessage) messageClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("[CombinerTool] caught", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("[CombinerTool] caught", e);
    }
   // return new BSPMessage();
  }
}
