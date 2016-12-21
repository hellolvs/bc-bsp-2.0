
package com.chinamobile.bcbsp.examples.pagerank;

/**
 * PageRankBSP.java
 */
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.IMessage;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * PageRankBSP.java This is the user-defined arithmetic which implements
 * {@link BSP}.
 *
 *
 *
 */

public class PageRankBSP extends BSP<BSPMessage> {

  /** Define Log variable output messages */
  public static final Log LOG = LogFactory.getLog(PageRankBSP.class);
  /** State error sum */
  public static final String ERROR_SUM = "aggregator.error.sum";
  /** State error threshold */
  public static final double ERROR_THRESHOLD = 0.1;
  /** State click rp value */
  public static final double CLICK_RP = 0.0001;
  /** State factor value */
  public static final double FACTOR = 0.15;

  /** Variables of the Graph. */
  private float newVertexValue = 0.0f;
  /** Variables of the Graph. */
  private float receivedMsgValue = 0.0f;
  /** Variables of the Graph. */
  private float receivedMsgSum = 0.0f;
  /** Variables of the Graph. */
  private float sendMsgValue = 0.0f;
  /** warning */
  @SuppressWarnings("unchecked")
  private Iterator<Edge> outgoingEdges;
  /** Variables of the Graph. */
  private PREdgeLite edge;
  /** Variables of the Graph. */
  private IMessage msg;
  /** Variables of the Graph. */
  private ErrorAggregateValue errorValue;
  /** Variables of the Graph. */
  private int failCounter = 0;

  @Override
  public void setup(Staff staff) {
    this.failCounter = staff.getFailCounter();
    LOG.info("Test FailCounter: " + this.failCounter);
  }

  @Override
  public void compute(Iterator<BSPMessage> messages,
      BSPStaffContextInterface context) throws Exception {
    /** Receive messages sent to this Vertex. */
    receivedMsgValue = 0.0f;
    receivedMsgSum = 0.0f;
    LOG.info("Vertex is processed! PageRankBSP");
    while (messages.hasNext()) {
      receivedMsgValue = Float
          .parseFloat(new String(messages.next().getData()));
      receivedMsgSum += receivedMsgValue;
    }

    PRVertexLite vertex = (PRVertexLite) context.getVertex();



    /** Handle received messages and Update vertex value.*/
    if (context.getCurrentSuperStepCounter() == 0) {
    /** old vertex value */
      sendMsgValue = Float.valueOf(vertex.getVertexValue()) /
          context.getOutgoingEdgesNum();
    } else {
      /**According to the sum of error to judge the convergence.*/
      errorValue = (ErrorAggregateValue) context.getAggregateValue(ERROR_SUM);
      if (Double.parseDouble(errorValue.getValue()) < ERROR_THRESHOLD) {
        context.voltToHalt();
        return;
      }

      newVertexValue = (float) (CLICK_RP * FACTOR + receivedMsgSum *
           (1 - FACTOR));
      sendMsgValue = newVertexValue / context.getOutgoingEdgesNum();
      vertex.setVertexValue(newVertexValue);
      context.updateVertex(vertex);
    }

    /** Send new messages.*/
    outgoingEdges = context.getOutgoingEdges();
    while (outgoingEdges.hasNext()) {
      edge = (PREdgeLite) outgoingEdges.next();

      msg = new BSPMessage(String.valueOf(edge.getVertexID()), Float.toString(
          sendMsgValue).getBytes());
      context.send(msg);
    }

    return;
  }

  @Override
  public void initBeforeSuperStep(SuperStepContextInterface arg0) {
  }
}
