/**
 * ShortestPathCombiner.java
 */

package com.chinamobile.bcbsp.examples.sssp;

import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.comm.BSPMessage;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ShortestPathCombiner An example implementation of combiner, to get the
 * shortest distance from the data of all messages sent to the same destination
 * vertex, and return only one message.
 *
 *
 *
 */
public class ShortestPathCombiner extends Combiner<BSPMessage> {

  /** Define Log variable output messages */
  public static final Log LOG = LogFactory.getLog(ShortestPathCombiner.class);
  /** State shortestDis */
  private int shortestDis = 0;

  /*
   * (non-Javadoc)
   * @see com.chinamobile.bcbsp.api.Combiner#combine(java.util.Iterator)
   */
  @Override
  public BSPMessage combine(Iterator<BSPMessage> messages) {

    BSPMessage msg = new BSPMessage();
    shortestDis = SSPBSP.MAXIMUM;
    while (messages.hasNext()) {

      msg = messages.next();
      int tmpValue = Integer.parseInt(new String(msg.getData()));
      if (shortestDis > tmpValue) {
        shortestDis = tmpValue;
      }
    }

    String newData = Integer.toString(shortestDis);
    msg = new BSPMessage(msg.getDstPartition(), msg.getDstVertexID(),
        newData.getBytes());
    return msg;
  }

}
