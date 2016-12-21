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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * StaffAttemptID StaffAttemptID is a unique identifier for a staff attempt.
 */
public class StaffAttemptID extends ID {
  /** A sign */
  protected static final String ATTEMPT = "attempt";
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(StaffAttemptID.class);
  /** the unique name for this staff*/
  private StaffID staffId;

  /**
   * Constructor.
   */
  public StaffAttemptID() {
    staffId = new StaffID();
  }

  /**
   * Constructor.
   *
   * @param staffId the unique name for this staff attempt.
   * @param id the unique.
   */
  public StaffAttemptID(StaffID staffId, int id) {
    super(id);
    if (staffId == null) {
      throw new IllegalArgumentException("staffId cannot be null");
    }
    this.staffId = staffId;
  }

  /**
   * Constructor.
   *
   * @param jtIdentifier the unique name for this staff attempt.
   * @param jobId the unique name for this job.
   * @param staffId the unique name for this staff attempt.
   * @param id the unique integer.
   */
  public StaffAttemptID(String jtIdentifier, int jobId, int staffId, int id) {
    this(new StaffID(jtIdentifier, jobId, staffId), id);
  }

  public BSPJobID getJobID() {
    return staffId.getJobID();
  }

  public StaffID getStaffID() {
    return staffId;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    StaffAttemptID that = (StaffAttemptID) o;
    return this.staffId.equals(that.staffId);
  }

  /**
   * @param builder A StringBuilder variable.
   * @return a string concatenation.
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return staffId.appendTo(builder).append(SEPARATOR).append(id);
  }

  /**
   * deserialize
   *
   * @param in Reads some bytes from an input.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    staffId.readFields(in);
  }

  /** serialize
   * write this object to out.
   *
   * @param out Writes to the output stream.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    staffId.write(out);
  }

  @Override
  public int hashCode() {
    return staffId.hashCode() * 5 + id;
  }

//  @Override
//  public int compareTo(ID o) {
//    StaffAttemptID that = (StaffAttemptID) o;
//    int tipComp = this.staffId.compareTo(that.staffId);
//    if (tipComp == 0) {
//      return this.id - that.id;
//    } else {
//      return tipComp;
//    }
//  }

  /**
   * @return a string concatenation.
   */
  @Override
  public String toString() {
    return appendTo(new StringBuilder(ATTEMPT)).toString();
  }

  /**
   * Split string concatenation
   *
   * @param str That will be split string.
   * @return StaffAttemptID a unique identifier for a staff attempt.
   */
  public static StaffAttemptID forName(String str)
      throws IllegalArgumentException {
    if (str == null) {
      return null;
    }
    try {
      String[] parts = str.split(Character.toString(SEPARATOR));
      if (parts.length == 5) {
        if (parts[0].equals(ATTEMPT)) {
          return new StaffAttemptID(parts[1], Integer.parseInt(parts[2]),
              Integer.parseInt(parts[3]), Integer.parseInt(parts[4]));
        }
      }
    } catch (Exception ex) {
      LOG.error("[forName]", ex);
    }
    throw new IllegalArgumentException("StaffAttemptId string : " + str +
        " is not properly formed");
  }
}
