/**
 * CopyRight by Chinamobile
 *
 * AggregationContextInterface.java
 */

package com.chinamobile.bcbsp.api;

import com.chinamobile.bcbsp.util.BSPJob;

import java.util.Iterator;

/**
 *
 *AggregationContextInterface A interface that aggregates context.
 */
public interface AggregationContextInterface {

  /**
   * Get the VertexID.
   *
   * @return
   *       VertexID
   */
  String getVertexID();

  /**
   * Get the value of one vertex.
   *
   * @return
   *        the value of one vertex
   */
  String getVertexValue();

  /**
   * Get the number of outgoing edges.
   *
   * @return
   *       the number of outgoing edges
   */
  int getOutgoingEdgesNum();

  /**
   * Get outgoing edges.
   *
   * @return
   *        outgoing edges
   */
  Iterator<Edge<?, ?>> getOutgoingEdges();

  /**
   * Get the current superstep counter.
   *
   * @return
   *       the current superstep counter
   */
  int getCurrentSuperStepCounter();

  /**
   * Get the BSP Job Configuration.
   *
   * @return
   *        the BSP Job Configuration
   */
  BSPJob getJobConf();

  /**
   * User interface to get an aggregate value aggregated from the previous super
   * step.
   *
   * @param name
   *        String
   * @return
   *        an aggregate value aggregated
   */
  @SuppressWarnings("unchecked")
  AggregateValue getAggregateValue(String name);
}
