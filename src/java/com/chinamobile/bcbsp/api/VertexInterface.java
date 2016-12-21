/**
 * CopyRight by Chinamobile
 *
 * VertexInterface.java
 */

package com.chinamobile.bcbsp.api;

import java.util.List;

/**
 * Vertex interface.
 *
 * @param <K>
 * @param <V1>
 * @param <E>
 */
public interface VertexInterface<K, V1, E> {

  /**
   * Set the vertex ID.
   *
   * @param vertexID
   *        vertex ID
   */
  void setVertexID(K vertexID);

  /**
   * Get the vertex ID.
   *
   * @return vertexID
   */
  K getVertexID();

  /**
   * Set the vertex value.
   *
   * @param vertexValue
   *        vertex value
   */
  void setVertexValue(V1 vertexValue);

  /**
   * Get the vertex value.
   *
   * @return vertexValue
   */
  V1 getVertexValue();

  /**
   * Add an edge to the edge list.
   *
   * @param edge
   *        edge
   */
  void addEdge(E edge);

  /**
   * Get the whole list of edges of the vertex.
   *
   * @return List of edges
   */
  List<E> getAllEdges();

  /**
   * Remove the edge from the edge list.
   *
   * @param edge
   *        edge
   */
  void removeEdge(E edge);

  /**
   * Update the edge.
   *
   * @param edge
   *        edge
   */
  void updateEdge(E edge);

  /**
   * Get the number of edges.
   *
   * @return
   *       the number of edges
   *
   */
  int getEdgesNum();

  /**
   * Hash code.
   *
   * @return int
   */
  int hashCode();

  /**
   * Transform into a String.
   *
   * @return
   *      a String
   */
  String intoString();

  /**
   * Transform from a String.
   *
   * @param vertexData
   *        vertex data
   * @throws Exception
   */
  void fromString(String vertexData) throws Exception;
}
