/**
 * CopyRight by Chinamobile
 *
 * AggregateValueInterface.java
 */

package com.chinamobile.bcbsp.api;

import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;

import java.util.Iterator;

/**
 * AggregateValue Interface
 *
 * @param <T>
 * @param <M>
 *
 */
public interface AggregateValueInterface<T, M> {

  /**
   * getter method
   * @return
   *        value
   */
  T getValue();

  /**
   * setter method
   * @param value
   *        value
   */
  void setValue(T value);

  /**
   * For the value to transferred through the synchronization process.
   *
   * @return String
   */
  String toString();

  /**
   * For the value to transferred through the synchronization process.
   *
   * @param s
   *        String
   */
  void initValue(String s);

  /**
   * A user defined method that will be called for each vertex of the graph to
   * init the aggregate value from the information of the graph.
   *
   * @param messages
   *        Iterator
   * @param context
   *        AggregationContextInterface
   */
  void initValue(Iterator<M> messages,
      AggregationContextInterface context);

  /**
   * Initialize before each super step. User can init some global variables for
   * each super step.
   *
   * @param context
   *        SuperStepContextInterface
   */
  void initBeforeSuperStep(SuperStepContextInterface context);
}
