/**
 * SumCombiner.java
 */

package com.chinamobile.bcbsp.examples.pagerank;

import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.comm.BSPMessage;

import java.util.Iterator;

/**
 * SumCombiner An example implementation of combiner, to sum the data of all
 * messages sent to the same destination vertex, and return only one message.
 *
 *
 *
 */
public class SumCombiner extends Combiner<BSPMessage> {

  /**
   * (non-Javadoc)
   * @see com.chinamobile.bcbsp.api.Combiner#combine(java.util.Iterator)
   * @param messages
   *        Iterator<BSPMessage>
   * @return
   *        msg
   */
  @Override
  public BSPMessage combine(Iterator<BSPMessage> messages) {

    BSPMessage msg;
    double sum = 0.0;
    do {
      msg = messages.next();
      String tmpValue = new String(msg.getData());
      sum = sum + Double.parseDouble(tmpValue);
    } while (messages.hasNext());

    String newData = Double.toString(sum);

    msg = new BSPMessage(msg.getDstPartition(), msg.getDstVertexID(),
        newData.getBytes());

    return msg;
  }

}
