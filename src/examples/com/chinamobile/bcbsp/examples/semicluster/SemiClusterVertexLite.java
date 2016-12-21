/**
 * SemiClusterVertexLite.java
 */

package com.chinamobile.bcbsp.examples.semicluster;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.examples.semicluster.SemiClusterBSP.SemiClusterTreeSetWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Vertex implementation for SemiCluster.
 *
 * @version 1.0
 */
public class SemiClusterVertexLite extends Vertex<Integer, SemiClusterTreeSetWritable, SemiClusterEdgeLite> {

  /** State vertex ID */
  private int vertexID = 0;
  /** State vertex value */
  private SemiClusterTreeSetWritable vertexValue = new SemiClusterTreeSetWritable();
  /** create a List<SemiClusterEdgeLite> object */
  private List<SemiClusterEdgeLite> edgesList = new ArrayList<SemiClusterEdgeLite>();

  @Override
  public void addEdge(SemiClusterEdgeLite edge) {
    this.edgesList.add(edge);
  }

  @Override
  public void fromString(String vertexData) throws Exception {
    String[] buffer = new String[2];
    StringTokenizer str = new StringTokenizer(vertexData,
        Constants.KV_SPLIT_FLAG);
    if (str.hasMoreElements()) {
      buffer[0] = str.nextToken();
    } else {
      throw new Exception();
    }
    if (str.hasMoreElements()) {
      buffer[1] = str.nextToken();
    }
    str = new StringTokenizer(buffer[0], Constants.SPLIT_FLAG);
    if (str.countTokens() != 2) {
      throw new Exception();
    }

    this.vertexID = Integer.valueOf(str.nextToken());
    this.vertexValue.fromString(str.nextToken());

    if (buffer[1].length() > 1) { // There has edges.

      str = new StringTokenizer(buffer[1], Constants.SPACE_SPLIT_FLAG);
      while (str.hasMoreTokens()) {
        SemiClusterEdgeLite edge = new SemiClusterEdgeLite();
        edge.fromString(str.nextToken());
        this.edgesList.add(edge);
      }

    }
  }

  @Override
  public List<SemiClusterEdgeLite> getAllEdges() {
    return this.edgesList;
  }

  @Override
  public int getEdgesNum() {
    return this.edgesList.size();
  }

  @Override
  public Integer getVertexID() {
    return this.vertexID;
  }

  @Override
  public SemiClusterTreeSetWritable getVertexValue() {
    return this.vertexValue;
  }

  @Override
  public String intoString() {
    String buffer = vertexID + Constants.SPLIT_FLAG + vertexValue.toString();
    buffer = buffer + Constants.KV_SPLIT_FLAG;
    int numEdges = edgesList.size();
    if (numEdges != 0) {
      buffer = buffer + edgesList.get(0).intoString();
    }
    for (int i = 1; i < numEdges; i++) {
      buffer = buffer + Constants.SPACE_SPLIT_FLAG +
          edgesList.get(i).intoString();
    }

    return buffer;
  }

  @Override
  public void removeEdge(SemiClusterEdgeLite edge) {
    this.edgesList.remove(edge);
  }

  @Override
  public void setVertexID(Integer vertexID) {
    this.vertexID = vertexID;
  }

  @Override
  public void setVertexValue(SemiClusterTreeSetWritable vertexValue) {
    this.vertexValue = vertexValue;
  }

  @Override
  public void updateEdge(SemiClusterEdgeLite edge) {
    removeEdge(edge);
    this.edgesList.add(edge);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readInt();
    this.vertexValue.readFields(in);
    this.edgesList.clear();
    int numEdges = in.readInt();
    SemiClusterEdgeLite edge;
    for (int i = 0; i < numEdges; i++) {
      edge = new SemiClusterEdgeLite();
      edge.readFields(in);
      this.edgesList.add(edge);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.vertexID);
    this.vertexValue.write(out);;
    out.writeInt(this.edgesList.size());
    for (SemiClusterEdgeLite edge : edgesList) {
      edge.write(out);
    }
  }


}
