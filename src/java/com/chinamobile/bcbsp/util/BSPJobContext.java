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

package com.chinamobile.bcbsp.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;

import java.io.IOException;
import java.io.OutputStream;

/**
 * BSPJobContext This class is a base job configuration information container.
 */
public class BSPJobContext {
  /** The job configuration file */
  protected final Configuration conf;
  /** Job id */
  private final BSPJobID jobId;

  /**
   * Constructor.
   *
   * @param conf Job configuration.
   * @param jobId Job id.
   */
  public BSPJobContext(Configuration conf, BSPJobID jobId) {
    this.conf = conf;
    this.jobId = jobId;
  }

  /**
   * Constructor.
   *
   * @param config LocalJobFile xml path.
   * @param jobId Job id.
   */
  public BSPJobContext(Path config, BSPJobID jobId) throws IOException {
    this.conf = new BSPConfiguration();
    this.jobId = jobId;
    this.conf.addResource(config);
  }

  public BSPJobID getJobID() {
    return jobId;
  }

  /**
   * Constructs a local file name. Files are distributed among configured local
   * directories.
   *
   * @param pathString Local file directories.
   * @return Configured local directories.
   * @throws IOException
   */
  public Path getLocalPath(String pathString) throws IOException {
    return conf.getLocalPath(Constants.BC_BSP_LOCAL_DIRECTORY, pathString);
  }

  /**
   * Write out the non-default properties in this configuration to the give
   * {@link OutputStream}.
   *
   * @param out the output stream to write to.
   */
  public void writeXml(OutputStream out) throws IOException {
    conf.writeXml(out);
  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Set the <code>value</code> of the <code>name</code> property.
   *
   * @param name property name.
   * @param value property value.
   */
  public void set(String name, String value) {
    conf.set(name, value);
  }

  /**
   * Get the value of the <code>name</code> property, <code>null</code> if
   * no such property exists.
   *
   * Values are processed for <a href="#VariableExpansion">variable expansion
   * </a> before being returned.
   *
   * @param name the property name.
   * @return the value of the <code>name</code> property,
   *         or null if no such property exists.
   */
  public String get(String name) {
    return conf.get(name);
  }

  /**
   * Get the value of the <code>name</code> property. If no such property
   * exists, then <code>defaultValue</code> is returned.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @return property value, or <code>defaultValue</code> if the property
   *         doesn't exist.
   */
  public String get(String name, String defaultValue) {
    return conf.get(name, defaultValue);
  }

  /**
   * Set the value of the <code>name</code> property to an <code>int</code>.
   *
   * @param name property name.
   * @param value <code>int</code> value of the property.
   */
  public void setInt(String name, int value) {
    conf.setInt(name, value);
  }

  /**
   * Get the value of the <code>name</code> property as an <code>int</code>.
   *
   * If no such property exists, or if the specified value is not a valid
   * <code>int</code>, then <code>defaultValue</code> is returned.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as an <code>int</code>,
   *         or <code>defaultValue</code>.
   */
  public int getInt(String name, int defaultValue) {
    return conf.getInt(name, defaultValue);
  }
}
