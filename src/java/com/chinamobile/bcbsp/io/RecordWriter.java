/**
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

package com.chinamobile.bcbsp.io;

import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * RecordWriter This class can write a record in the format of Key-Value.
 */
public abstract class RecordWriter<K, V> {

  /**
   * Writes a key/value pair. <code>RecordWriter</code> writes the output
   * &lt;key, value&gt; pairs to an output file.
   *
   * @param key
   *        the key to write.
   * @param value
   *        the value to write.
   * @throws IOException
   */
  public abstract void write(K key, V value) throws IOException,
      InterruptedException;

  /**
   * Writes a key/value pair. <code>RecordWriter</code> writes the output
   * &lt;key, value&gt; pairs to an output file.
   *
   * @param key
   *        the key to write.
   * @param value
   *        the value to write.
   * @throws IOException
   */
  public abstract void write(K keyValue) throws IOException,
      InterruptedException;
  
  /**
   * Close this <code>RecordWriter</code> to future operations.
   *
   * @param job
   *        The current BSPJob job
   * @throws IOException
   */
  public abstract void close(BSPJob job) throws IOException,
      InterruptedException;
}
