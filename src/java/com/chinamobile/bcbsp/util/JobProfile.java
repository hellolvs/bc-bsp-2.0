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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * JobProfile A JobProfile tracks job's status.
 */
public class JobProfile implements Writable {
  /** userid of the person who submitted the job. */
  private String user;
  /** id of the job. */
  private final BSPJobID jobid;
  /** job configuration file.. */
  private String jobFile;
  /** user-specified job name. */
  private String name;
  static { // register a ctor
    WritableFactories.setFactory(JobProfile.class, new WritableFactory() {
      public Writable newInstance() {
        return new JobProfile();
      }
    });
  }

  /**
   * Construct an empty {@link JobProfile}.
   */
  public JobProfile() {
    jobid = new BSPJobID();
  }

  /**
   * Construct a {@link JobProfile} the userid, jobid, job config-file,
   * job-details url and job name.
   *
   * @param user
   *        userid of the person who submitted the job.
   * @param jobid
   *        id of the job.
   * @param jobFile
   *        job configuration file.
   * @param name
   *        user-specified job name.
   */
  public JobProfile(String user, BSPJobID jobid, String jobFile, String name) {
    this.user = user;
    this.jobid = jobid;
    this.jobFile = jobFile;
    this.name = name;
  }

  public String getUser() {
    return user;
  }

  public BSPJobID getJobID() {
    return jobid;
  }

  public String getJobFile() {
    return jobFile;
  }

  public String getJobName() {
    return name;
  }

  /** serialize
   * write this object to out.
   *
   * @param out Writes to the output stream.
   */
  public void write(DataOutput out) throws IOException {
    jobid.write(out);
    Text.writeString(out, jobFile);
    Text.writeString(out, user);
    Text.writeString(out, name);
  }

  /**
   * deserialize
   *
   * @param in Reads some bytes from an input.
   */
  public void readFields(DataInput in) throws IOException {
    jobid.readFields(in);
    this.jobFile = Text.readString(in);
    this.user = Text.readString(in);
    this.name = Text.readString(in);
  }
}
