/**
 * CopyRight by Chinamobile
 *
 * AggregatorInterface.java
 */

package com.chinamobile.bcbsp.api;

/**
 * Interface Aggregator for user to define.
 *
 * @param <T>
 *
 */
public interface AggregatorInterface<T> {
  /**
   * The method for aggregate algorithm that should be implements by the user.
   *
   * @param aggValues
   *        Iterable<T>
   * @return the aggregate result
   */
  T aggregate(Iterable<T> aggValues);
}
