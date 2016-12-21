/**
 * PREdge.java
 */

package com.chinamobile.bcbsp.examples.sssp;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Edge implementation for PageRank.
 *
 *
 *
 */
public class SSPEdge extends Edge<Integer, Integer> {

  /** State vertex ID */
  private int vertexID = 0;
  /** State edge value */
  private int edgeValue = 0;

  @Override
  public void fromString(String edgeData) throws Exception {
    StringTokenizer str = new StringTokenizer(edgeData, Constants.SPLIT_FLAG);
    if (str.countTokens() != 2) {
      throw new Exception();
    }
    this.vertexID = Integer.valueOf(str.nextToken());
    this.edgeValue = Integer.valueOf(str.nextToken());
  }

  @Override
  public Integer getEdgeValue() {
    return this.edgeValue;
  }

  @Override
  public Integer getVertexID() {
    return this.vertexID;
  }

  @Override
  public String intoString() {
    return this.vertexID + Constants.SPLIT_FLAG + this.edgeValue;
  }

  @Override
  public void setEdgeValue(Integer edgeValue) {
    this.edgeValue = edgeValue;
  }

  @Override
  public void setVertexID(Integer vertexID) {
    this.vertexID = vertexID;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readInt();
    this.edgeValue = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.vertexID);
    out.writeInt(edgeValue);
  }

  @Override
  public boolean equals(Object object) {
    SSPEdge edge = (SSPEdge) object;

    return this.vertexID == edge.getVertexID();
  }

}
