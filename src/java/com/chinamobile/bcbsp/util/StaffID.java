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

/**
 * StaffID StaffID represents the immutable and unique identifier for a BSP
 * Staff.
 */
public class StaffID extends ID {
  /** A sign */
  protected static final String STAFF = "staff";
  /** The format of the id. */
  protected static final NumberFormat IDFORMAT = NumberFormat.getInstance();
  static {
    IDFORMAT.setGroupingUsed(false);
    IDFORMAT.setMinimumIntegerDigits(6);
  }
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(StaffID.class);
  /** id of the job. */
  private BSPJobID jobId;

  /**
   * Constructor.
   */
  public StaffID() {
    jobId = new BSPJobID();
  }

  /**
   * Constructor.
   *
   * @param jobId id of the job.
   * @param id  integer.
   */
  public StaffID(BSPJobID jobId, int id) {
    super(id);
    if (jobId == null) {
      throw new IllegalArgumentException("jobId cannot be null");
    }
    this.jobId = jobId;
  }

  /**
   * Constructor.
   *
   * @param jtIdentifier the unique name for this staff attempt.
   * @param jobId id of the job.
   * @param id the unique integer.
   */
  public StaffID(String jtIdentifier, int jobId, int id) {
    this(new BSPJobID(jtIdentifier, jobId), id);
  }

  /** @return the {@link BSPJobID} object that this tip belongs to */
  public BSPJobID getJobID() {
    return jobId;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    StaffID that = (StaffID) o;
    return this.jobId.equals(that.jobId);
  }

  @Override
  public int compareTo(ID o) {
    StaffID that = (StaffID) o;
    int jobComp = this.jobId.compareTo(that.jobId);
    if (jobComp == 0) {
      return this.id - that.id;
    } else {
      return jobComp;
    }
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(STAFF)).toString();
  }

  /**
   * @param builder A StringBuilder variable.
   * @return a string concatenation.
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return jobId.appendTo(builder).append(SEPARATOR)
        .append(IDFORMAT.format(id));
  }

  @Override
  public int hashCode() {
    return jobId.hashCode() * 524287 + id;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    jobId.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jobId.write(out);
  }

  /**
   * Split string concatenation
   *
   * @param str That will be split string.
   * @return StaffID a unique identifier for a staff.
   */
  public static StaffID forName(String str) throws IllegalArgumentException {
    if (str == null) {
      return null;
    }
    try {
      String[] parts = str.split("_");
      if (parts.length == 5) {
        if (parts[0].equals(STAFF)) {
          return new StaffID(parts[1], Integer.parseInt(parts[2]),
              Integer.parseInt(parts[4]));
        }
      }
    } catch (Exception ex) {
      LOG.error("[forName]", ex);
    }
    throw new IllegalArgumentException("StaffId string : " + str +
        " is not properly formed");
  }
}
