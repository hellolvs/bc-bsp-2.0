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
package com.chinamobile.bcbsp.io.titan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.Writable;

/**
 * TitanTableSplit describe the table split in Titan database.
 * @author Zhicheng Liu 2013/4/25
 */
public class TitanTableSplit extends InputSplit implements Writable,
    Comparable<TitanTableSplit> {
  /**input table name*/
  private String tableName;
  /**first vertexID in the table*/
  private long firstVertexID;
  /**last vertexID in the table*/
  private long lastVertexID;

  /**
   * TitanTableSplit construct method
   * @param tableName
   *        titan table name
   * @param firstVertexID
   *        first VertexID to read from.
   * @param lastVertexID
   *        vertexID where to end.
   */
  public TitanTableSplit(String tableName, long firstVertexID,
      long lastVertexID) {
    this.tableName = tableName;
    this.firstVertexID = firstVertexID;
    this.lastVertexID = lastVertexID;
  }

  /**
   * TitanTableSplit construct method.
   */
  public TitanTableSplit() {
  }

  /**
   * set titan table name.
   * @param name
   *        table name to set.
   */
  public void setTableName(String name) {
    this.tableName = name;
  }

  /**
   * set the first VertexID
   * @param id
   *        first id num to set.
   */
  public void setFirstVertexID(long id) {
    this.firstVertexID = id;
  }

  /**
   * set last vertexID num
   * @param id
   *        last id num to set.
   */
  public void setLastVertexID(long id) {
    this.lastVertexID = id;
  }

  /**
   * get titan table name.
   * @return titan table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * get the first vertexID
   * @return first vertex id.
   */
  public long getFirstVertexID() {
    return firstVertexID;
  }

  /**
   * get the last vertex ID.
   * @return last vertex ID.
   */
  public long getLastVertexID() {
    return lastVertexID;
  }

  @Override
  public String toString() {
    return tableName + ":" + firstVertexID + "," + lastVertexID;
  }

  @Override
  public int compareTo(TitanTableSplit o) {
    return firstVertexID == o.getFirstVertexID() ? 0 : firstVertexID < o
        .getFirstVertexID() ? -1 : 1;
  }

  /**
   * rewrite equals for TitanTableSplit
   * @param obj
   *        obj to compare
   * @return judge result.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof TitanTableSplit)) {
      return false;
    }
    TitanTableSplit split = (TitanTableSplit) obj;
    return tableName.compareTo(split.getTableName()) != 0 ? false :
        firstVertexID != split.getFirstVertexID() ? false :
            lastVertexID != split.getLastVertexID() ? false : true;
  }

  /**
   * corresponding definition of
   *  'hashCode()' for equal()
   * @return sum of tablename hashcode and firstVertexID and lastvertexID
   */
  @Override
  public int hashCode() {
    return this.tableName.hashCode() + (int) this.firstVertexID +
        (int) this.lastVertexID;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    tableName = in.readUTF();
    firstVertexID = in.readLong();
    lastVertexID = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(tableName);
    out.writeLong(firstVertexID);
    out.writeLong(lastVertexID);
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    String[] locations = new String[1];
    locations[0] = "";
    return locations;
  }
}
