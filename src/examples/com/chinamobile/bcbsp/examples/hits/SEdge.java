/**
 * PREdgeLite.java
 */

package com.chinamobile.bcbsp.examples.hits;

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
public class SEdge extends Edge<String, String> {

  /** State vertex ID */
  private String vertexID = "";
  private String value ="";

  @Override
  public void fromString(String edgeData) throws Exception {
   this.vertexID = edgeData;
  }

  @Override
  public String getEdgeValue() {
    return this.value;
  }

  @Override
  public String getVertexID() {
    return this.vertexID;
  }

  @Override
  public String intoString() {
    return this.vertexID ;
  }

  @Override
  public void setEdgeValue(String edgeValue) {
	  
  }

  @Override
  public void setVertexID(String vertexID) {
    this.vertexID = vertexID;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.vertexID);
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
    SEdge edge = (SEdge) object;

    return this.vertexID.equals(edge.getVertexID());
  }

}
