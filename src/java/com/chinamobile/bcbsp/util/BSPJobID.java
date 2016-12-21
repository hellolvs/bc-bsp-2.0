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

package com.chinamobile.bcbsp.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * BSPJobID represents the immutable and unique identifier for the job.
 */
public class BSPJobID extends ID implements Comparable<ID> {
  /** The format of the id. */
  protected static final NumberFormat IDFORMAT = NumberFormat.getInstance();
  /** Define a job. */
  protected static final String JOB = "job";
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(BSPJobID.class);
  /** Define a text identifier */
  private final Text jtIdentifier;

  static {
    IDFORMAT.setGroupingUsed(false);
    IDFORMAT.setMinimumIntegerDigits(4);
  }

  /**
   * Constructor.
   */
  public BSPJobID() {
    jtIdentifier = new Text();
  }

  /**
   * Constructor.
   *
   * @param jtIdentifier The text identifier.
   * @param id The id.
   */
  public BSPJobID(String jtIdentifier, int id) {
    super(id);
    this.jtIdentifier = new Text(jtIdentifier);
  }

  public String getJtIdentifier() {
    return jtIdentifier.toString();
  }

  /**
   * Judge whether two objects are equal.
   *
   * @param o The incoming object.
   * @return Whether two objects are equal.
   */
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    BSPJobID that = (BSPJobID) o;
    return this.jtIdentifier.equals(that.jtIdentifier);
  }

  /**
   * Compare bytes from {#getBytes()}.
   * @see org.apache.hadoop.io.WritableComparator
   * #compareBytes(byte[],int,int,byte[],int,int).
   *
   * @param o The incoming object.s
   * @return Compare the results.
   */
  @Override
  public int compareTo(ID o) {
    BSPJobID that = (BSPJobID) o;
    int jtComp = this.jtIdentifier.compareTo(that.jtIdentifier);
    if (jtComp == 0) {
      return this.id - that.id;
    } else {
      return jtComp;
    }
  }

  /**
   * String concatenation.
   * @param builder A StringBuilder variable.
   * @return builder.
   */
  public StringBuilder appendTo(StringBuilder builder) {
    builder.append(SEPARATOR);
    builder.append(jtIdentifier);
    builder.append(SEPARATOR);
    builder.append(IDFORMAT.format(id));
    return builder;
  }

  /**
   * @return a hash of the bytes returned from {#getBytes()}.
   * @see org.apache.hadoop.io.WritableComparator#hashBytes(byte[],int)
   */
  @Override
  public int hashCode() {
    return jtIdentifier.hashCode() + id;
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(JOB)).toString();
  }

  /** deserialize
   *
   * @param in Reads some bytes from an input.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.jtIdentifier.readFields(in);
  }

  /** serialize
   * write this object to out.
   *
   * @param out Writes to the output stream.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jtIdentifier.write(out);
  }

  public BSPJobID forName(String str) throws IllegalArgumentException {
    if (str == null) {
      return null;
    }
    try {
      String[] parts = str.split("_");
      if (parts.length == 3) {
        if (parts[0].equals(JOB)) {
          return new BSPJobID(parts[1], Integer.parseInt(parts[2]));
        }
      }
    } catch (Exception ex) {
      LOG.error("[forName]", ex);
    }
    throw new IllegalArgumentException("JobId string : " + str +
        " is not properly formed");
  }
}
