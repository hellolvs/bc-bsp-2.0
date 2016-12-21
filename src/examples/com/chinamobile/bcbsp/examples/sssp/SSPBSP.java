
package com.chinamobile.bcbsp.examples.sssp;

/**
 * ShortestPath.java
 */
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.util.BSPJob;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ShortestPath.java This is the user-defined arithmetic which implements
 * {@link BSP}.
 */

public class SSPBSP extends BSP<BSPMessage> {

  /** Define Log variable output messages */
  public static final Log LOG = LogFactory.getLog(SSPBSP.class);
  /** Variables of the Graph.*/
  public static final int MAXIMUM = Integer.MAX_VALUE / 2;
  /** Variables of the Graph.*/
  public static final  String SOURCEID = "bcbsp.sssp.source.id";
  /** Variables of the Graph.*/
  private int sourceID = 1;
  /** Variables of the Graph.*/
  private int sendMsgValue = 0;
  /** warning */
  @SuppressWarnings("unchecked")
  /** Variables of the Graph.*/
  private Iterator<Edge> adjacentNodes;
  /** Variables of the Graph.*/
  private SSPEdge edge = null;
  /** Variables of the Graph.*/
  private IMessage msg;

  /** Variables of the Graph.*/
  private int newVertexValue;
  /** Variables of the Graph.*/
  private int newDistance = MAXIMUM;
  /** Variables of the Graph.*/
  private int eachNodeDistance = 0;

  @Override
  public void initBeforeSuperStep(SuperStepContextInterface context) {
    BSPJob job = context.getJobConf();
    if (context.getCurrentSuperStepCounter() == 0) {
      this.sourceID = job.getInt(SOURCEID, 1);
    }
  }

  @Override
  public void compute(Iterator<BSPMessage> messages,
      BSPStaffContextInterface context) throws Exception {

    SSPVertex vertex = (SSPVertex) context.getVertex();
    if (context.getCurrentSuperStepCounter() == 0) {

      int vertexID = vertex.getVertexID();
      int dis;

      if (vertexID == sourceID) {
        LOG.info("Source:" + vertexID);
        dis = 0;
        notityAdjacentNode(0, context);
      } else {
        dis = MAXIMUM;
      }
      newVertexValue = dis;
      vertex.setVertexValue(newVertexValue);
      context.updateVertex(vertex);
    } else {
      newDistance = getNewDistance(messages);
      int previousDis = vertex.getVertexValue();
      if (previousDis > newDistance) {
        notityAdjacentNode(newDistance, context);
        vertex.setVertexValue(newDistance);
        context.updateVertex(vertex);
      }

    }
    context.voltToHalt();
  }

  /**
   * notify adjacent node
   * @param nodedis
   *        node distance
   * @param context
   *        BSPStaffContextInterface
   */
  private void notityAdjacentNode(int nodedis,
      BSPStaffContextInterface context) {

    adjacentNodes = context.getOutgoingEdges();
    while (adjacentNodes.hasNext()) {
      edge = (SSPEdge) adjacentNodes.next();
      sendMsgValue = edge.getEdgeValue() + nodedis;
      msg = new BSPMessage(String.valueOf(edge.getVertexID()), Integer
          .toString(sendMsgValue).getBytes());
      context.send(msg);

    }
  }

  /**
   * get new distance
   * @param messages
   *        Iterator<BSPMessage>
   * @return
   *        new distance
   */
  private int getNewDistance(Iterator<BSPMessage> messages) {
    int shortestDis = MAXIMUM;
    while (messages.hasNext()) {
      eachNodeDistance = new Integer(new String(messages.next().getData()));
      if (eachNodeDistance < shortestDis) {
        shortestDis = eachNodeDistance;
      }
    }
    return shortestDis;
  }

}
