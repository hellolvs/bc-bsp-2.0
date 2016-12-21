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

package com.chinamobile.bcbsp.ml;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.NamedDoubleVector;

import com.chinamobile.bcbsp.Constants;

/**
 * Writable for dense vectors.
 */
public class VectorWritable extends Key implements
    WritableComparable<VectorWritable> {
  
  public static final Log LOG = LogFactory.getLog(VectorWritable.class);
  private DoubleVector vector;
  
  public VectorWritable() {
    super();
  }
  
  public VectorWritable(VectorWritable v) {
    this.vector = v.getVector();
  }
  
  public VectorWritable(DoubleVector v) {
    this.vector = v;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    writeVector(this.vector, out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.vector = readVector(in);
  }
  
  @Override
  public final int compareTo(VectorWritable o) {
    return compareVector(this, o);
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((vector == null) ? 0 : vector.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    VectorWritable other = (VectorWritable) obj;
    if (vector == null) {
      if (other.vector != null)
        return false;
    } else if (!vector.equals(other.vector))
      return false;
    return true;
  }
  
  /**
   * @return the embedded vector
   */
  public DoubleVector getVector() {
    return vector;
  }
  
  @Override
  public String toString() {
    return vector.toString();
  }
  
  public static void writeVector(DoubleVector vector, DataOutput out)
      throws IOException {
    if(vector == null){
      LOG.info("lin test : VectorWritable write Vector is null");
    }else{
      LOG.info("lin test : VectorWritable write Vector is not null");
    }
    out.writeInt(vector.getLength());
    for (int i = 0; i < vector.getDimension(); i++) {
      out.writeDouble(vector.get(i));
    }
    
    if (vector.isNamed() && vector.getName() != null) {
      out.writeBoolean(true);
      out.writeUTF(vector.getName());
    } else {
      out.writeBoolean(false);
    }
  }
  
  public static DoubleVector readVector(DataInput in) throws IOException {
    int length = in.readInt();
    DoubleVector vector;
    vector = new DenseDoubleVector(length);
    for (int i = 0; i < length; i++) {
      vector.set(i, in.readDouble());
    }
    
    if (in.readBoolean()) {
      vector = new NamedDoubleVector(in.readUTF(), vector);
    }
    return vector;
  }
  
  public static int compareVector(VectorWritable a, VectorWritable o) {
    return compareVector(a.getVector(), o.getVector());
  }
  
  public static int compareVector(DoubleVector a, DoubleVector o) {
    DoubleVector subtract = a.subtractUnsafe(o);
    return (int) subtract.sum();
  }
  
  public static VectorWritable wrap(DoubleVector a) {
    return new VectorWritable(a);
  }
  
  public void set(DoubleVector vector) {
    this.vector = vector;
  }
  
  // ljn add
  public void fromString(String vectorData) {
    StringTokenizer str = new StringTokenizer(vectorData,
        Constants.SPACE_SPLIT_FLAG);
    ArrayList<Double> elementArray = new ArrayList<Double>();
    while (str.hasMoreTokens()) {
      String test = str.nextToken();
      // elementArray.add(Double.parseDouble(str.nextToken()));
      elementArray.add(Double.parseDouble(test));
    }
    DoubleVector vector;
    vector = new DenseDoubleVector(elementArray.size());
    for (int i = 0; i < elementArray.size(); i++) {
      vector.set(i, elementArray.get(i));
    }
    this.vector = vector;
  }
}
