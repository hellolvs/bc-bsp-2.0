/**
 * PREdgeLite.java
 */

package com.chinamobile.bcbsp.examples.pagerank;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Edge Lite implementation for PageRank.
 *
 *
 *
 */
public class PREdgeLite extends Edge<Integer, Byte> {

  /** State vertex ID */
  private int vertexID = 0;

  @Override
  public void fromString(String edgeData) throws Exception {
    StringTokenizer str = new StringTokenizer(edgeData, Constants.SPLIT_FLAG);
    if (str.countTokens() != 2) {
      throw new Exception();
    }
    this.vertexID = Integer.valueOf(str.nextToken());
  }

  @Override
  public Byte getEdgeValue() {
    return 0;
  }

  @Override
  public Integer getVertexID() {
    return this.vertexID;
  }

  @Override
  public String intoString() {
    return this.vertexID + Constants.SPLIT_FLAG + "0";
  }

  @Override
  public void setEdgeValue(Byte edgeValue) {
  }

  @Override
  public void setVertexID(Integer vertexID) {
    this.vertexID = vertexID;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.vertexID);
  }

  /**
   * equals Compare the vertex ID
   * @param object
   *        the reference object with which to compare.
   * @return
   *        {@code true} if this object is the same as the obj
   *          argument; {@code false} otherwise.
   */
  @Override
  public boolean equals(Object object) {
    PREdgeLite edge = (PREdgeLite) object;

    return this.vertexID == edge.getVertexID();
  }

}
