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

package com.chinamobile.bcbsp.io.mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * A MySQL split corresponds to a key range (low, high). All references to row
 * below refer to the key of the row.
 */
public class DBInputSplit extends InputSplit implements Writable,
    Comparable<DBInputSplit> {
  /** The split end */
  private long end = 0;
  /** The split start */
  private long start = 0;
  /** the region location */
  private String regionLocation = null;

  /**
   * Default Constructor
   */
  public DBInputSplit() {
  }

  /**
   * Convenience Constructor
   *
   * @param start
   *        the index of the first row to select
   * @param end
   *        the index of the last row to select
   */
  public DBInputSplit(long start, long end) {
    this.start = start;
    this.end = end;
  }

  public String getRegionLocation() {
    return regionLocation;
  }

  /**
   * Returns the region's location as an array.
   *
   * @return The array containing the region location.
   */

  @Override
  public String[] getLocations() throws IOException {
    regionLocation = "";
    return new String[] {regionLocation};
  }

  /**
   * @return The index of the first row to select
   */
  public long getStart() {
    return start;
  }

  /**
   * @return The index of the last row to select
   */
  public long getEnd() {
    return end;
  }

  /**
   * @return The total row count in this split
   */
  @Override
  public long getLength() throws IOException {
    return end - start;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    start = input.readLong();
    end = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(start);
    output.writeLong(end);
  }

  @Override
  public int compareTo(DBInputSplit split) {
    // return long.compareTo(getStart(), split.getStart());
    return 0;
  }
}
