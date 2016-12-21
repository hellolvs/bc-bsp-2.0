
package com.chinamobile.bcbsp.ml;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.chinamobile.bcbsp.api.Vertex;

public abstract class KeyValuePair<K , V> extends Vertex<K, Object, V> {
  
  public KeyValuePair() {
    
  }
  
  public KeyValuePair(K key, V value) {
    super();
    setKey(key);
    setValue(value);
  }
  
  public abstract K getKey();
  
  public abstract V getValue();
  
  public abstract void setKey(K key);
  
  public abstract void setValue(V value);
  
  @Override
  public abstract String intoString();
  
  @Override
  public abstract void fromString(String vertexData) throws Exception;
  
  @Override
  public abstract void readFields(DataInput arg0) throws IOException;
  
  @Override
  public abstract void write(DataOutput arg0) throws IOException;
  
  public void clear() {
    setKey(null);
    setValue(null);
  }
  
  @Override
  public void setVertexID(K vertexID) {
    setKey(vertexID);
  }
  
  @Override
  public K getVertexID() {
    return getKey();
  }
  
  @Override
  public void setVertexValue(Object vertexValue) {
    
  }
  
  @Override
  public Object getVertexValue() {
    return null;
  }
  
  @Override
  public void addEdge(Object edge) {
    
  }
  
  @Override
  public List getAllEdges() {
    List<V> edges = new ArrayList<V>();
    edges.add(getValue());
    return edges;
  }
  
  @Override
  public void removeEdge(Object edge) {
  }
  
  @Override
  public void updateEdge(Object edge) {
    
  }
  
  @Override
  public int getEdgesNum() {
    return 0;
  }
  
  public void setEdgeToValue(List<V> allEdges) {
    this.setValue(allEdges.get(0));
  }
  
}
