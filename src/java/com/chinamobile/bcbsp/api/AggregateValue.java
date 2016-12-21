/**
 * CopyRight by Chinamobile
 *
 * AggregateValue.java
 */

package com.chinamobile.bcbsp.api;

import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;

import org.apache.hadoop.io.Writable;

/**
 * AggregateValue Abstract class that implements AggregateValueInterface, it
 * should be extended by the user to define own aggregate value data structure.
 *
 * @param <T>
 * @param <M>
 *
 */
public abstract class AggregateValue<T, M> implements
    AggregateValueInterface<T, M>, Writable, Cloneable {

  /**
   * The default implementation of initBeforeSuperStep does nothing.
   *
   * @param context
   *        SuperStepContextInterface
   */
  @Override
  public void initBeforeSuperStep(SuperStepContextInterface context) {

  }

  /**
   * Creates and returns a copy of this object.
   * @return
   *        a clone of this instance
   */
  @SuppressWarnings("unchecked")
  public Object clone() {
    AggregateValue<T, M> aggValue = null;
    try {
      aggValue = (AggregateValue<T, M>) super.clone();
    } catch (CloneNotSupportedException e) {
      //e.printStackTrace();
    	throw new RuntimeException("clone failed",e);
    }
    return aggValue;
  }
}
