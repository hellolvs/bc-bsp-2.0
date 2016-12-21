
package com.chinamobile.bcbsp.examples.pagerank;

/**
 * ErrorSumAggregator.java
 */
import com.chinamobile.bcbsp.api.Aggregator;

import java.util.Iterator;

/**
 * ErrorSumAggregator.java This is used to aggregate the global Error.
 *
 *
 *
 */

public class ErrorSumAggregator extends Aggregator<ErrorAggregateValue> {

  @Override
  public ErrorAggregateValue
  aggregate(Iterable<ErrorAggregateValue> aggValues) {
    ErrorAggregateValue errorSum = new ErrorAggregateValue();
    Iterator<ErrorAggregateValue> it = aggValues.iterator();
    double errorValue = 0.0;

    while (it.hasNext()) {
      errorValue += Double.parseDouble(it.next().getValue());
    }
    errorSum.setValue(Double.toString(errorValue));

    return errorSum;
  }
}
