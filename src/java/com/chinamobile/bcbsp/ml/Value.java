package com.chinamobile.bcbsp.ml;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;

public abstract class Value<V> extends Edge<V, Object>{

  private V value;
    
  @Override
  final public void setVertexID(V vertexID) {
    this.value = vertexID;
  }

  @Override
  final public V getVertexID() {
    // TODO Auto-generated method stub
    return this.value;
  }

  @Override
  public void setEdgeValue(Object edgeValue) {
    
  }

  @Override
  public Object getEdgeValue() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public abstract String intoString();

  @Override
  public abstract void fromString(String valueData) throws Exception;

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    // TODO Auto-generated method stub   
  }
  
  final public void setValue(V value){
    setVertexID(value);
  }
  
  
  final public V getValue() {
    // TODO Auto-generated method stub
    return getVertexID();
  }
}
