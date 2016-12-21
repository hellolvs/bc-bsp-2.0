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

package com.chinamobile.bcbsp.pipes;

import java.io.IOException;

/**
 * The abstract description of the downward (from Java to C++) Pipes protocol.
 * All of these calls are asynchronous and return before the message has been
 * processed.�?
 */
public interface DownwardProtocol {
  /**
   * Start communication.
   * @throws IOException e
   */
  void start() throws IOException;

  /**
   * Run a superStep in C++ client.
   * @throws IOException e
   */
  void runASuperStep() throws IOException;

  /**
   * Send Message to C++ client.
   * @param message send the message  to c++ client
   * @throws IOException e
   */
  void sendMessage(String message) throws IOException;

  /**
   * Send Key/Value to C++ client.
   * @param key the key
   * @param value the value
   * @throws IOException e
   */
  void sendKeyValue(String key, String value) throws IOException;

  /**
   * Send vertex key , staff num and get partition Id.
   * @param key the vertex key
   * @param num the staff num
   * @throws IOException e
   * @throws InterruptedException e
   */
  void sendKey(String key, int num) throws IOException, InterruptedException;

  /**
   * send staffId to C++ client.
   * @param staffId the staff id.
   * @throws IOException e
   */
  void sendStaffId(String staffId) throws IOException;

  /**
   * Get partition id.
   * @throws IOException e
   * @return the partition id
   */
  int getPartionId() throws IOException;

  /**
   * Send new Aggregate Value.
   * @param aggregateValue send new aggregate value to c++　client
   * @throws IOException e
   */
  void sendNewAggregateValue(String[] aggregateValue) throws IOException;

  /**
   * Save result.
   * @throws IOException e
   */

  void saveResult() throws IOException;

  /**
   * The task has no more input coming, but it should finish processing it's
   * input.
   * @throws IOException e
   */
  void endOfInput() throws IOException;

  /**
   * The task should stop as soon as possible, because something has gone wrong.
   * @throws IOException e
   */
  void abort() throws IOException;

  /**
   * Flush the data through any buffers.
   * @throws IOException e
   */
  void flush() throws IOException;

  /**
   * Close the connection.
   * @throws IOException e
   * @throws InterruptedException e
   */
  void close() throws IOException, InterruptedException;

}
