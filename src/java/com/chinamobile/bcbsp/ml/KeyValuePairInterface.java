
package com.chinamobile.bcbsp.ml;

public interface KeyValuePairInterface<K , V> {
  
  K getKey();
  
  V getValue();
  
  void setKey(K key);
  
  void setValue(V value);
  
}
