/**
 * CopyRight by Chinamobile
 *
 * EdgeInterface.java
 */

package com.chinamobile.bcbsp.api;

/**
 * Edge interface.
 * @param <V2>
 * @param <K>
 */
public interface EdgeInterface<K, V2> {

  /**
   * Set the destination vertex ID.
   *
   * @param vertexID
   *        vertex ID
   */
  void setVertexID(K vertexID);

  /**
   * Get the destination vertex ID.
   *
   * @return vertexID
   */
  K getVertexID();

  /**
   * Set the edge value.
   *
   * @param edgeValue
   *        edge value
   */
  void setEdgeValue(V2 edgeValue);

  /**
   * Get the edge value.
   *
   * @return edgeValue
   *
   */
  V2 getEdgeValue();

  /**
   * Transform into a String.
   *
   * @return
   *       a string
   */
  String intoString();

  /**
   * Transform from a String.
   *
   * @param edgeData
   *        edge data
   * @throws Exception
   */
  void fromString(String edgeData) throws Exception;

  /**
   * equals
   *
   * @param object
   *        object
   * @return boolean
   */
  boolean equals(Object object);

}
