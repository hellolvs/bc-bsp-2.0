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

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;

/**
 * BDB database environments, can cache {@link StoredClassCatalog} and share it.
 */
public class BdbEnvironment extends Environment {
  /** My process health znode. */
  private StoredClassCatalog classCatalog;
  /** My process health znode. */
  private Database classCatalogDB;

  /**
   * Constructor method.
   * @param envHome
   *        home directory
   * @param envConfig
   *        config options configurations
   * @throws DatabaseException
   */
  public BdbEnvironment(File envHome, EnvironmentConfig envConfig) {
    super(envHome, envConfig);
    }

  /**
   * return StoredClassCatalog.
   * @return the cached class catalog
   */
  public StoredClassCatalog getClassCatalog() {
    if (classCatalog == null) {
      DatabaseConfig dbConfig = new DatabaseConfig();
      dbConfig.setAllowCreate(true);
      try {
        classCatalogDB = openDatabase(null, "classCatalog", dbConfig);
        classCatalog = new StoredClassCatalog(classCatalogDB);
      } catch (DatabaseException e) {
        throw new RuntimeException(e);
      }
    }
    return classCatalog;
  }

  @Override
  public synchronized void close() {
    if (classCatalogDB != null) {
      classCatalogDB.close();
    }
    super.close();
  }

  /** access method of classCatalog. */
  public void setClassCatalog(StoredClassCatalog classCatalog) {
    this.classCatalog = classCatalog;
  }
}
