/**
 * PRVertexLite.java
 */

package com.chinamobile.bcbsp.examples.simrank;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Vertex implementation for PageRank.
 *
 *
 * @version 1.0
 */
public class SRVertex extends Vertex<String, String, SREdge> {

  /** State vertex ID */
  private String vertexID = "";
  /** State vertex value */
  private String vertexValue = "";
  /** create a List<PREdgeLite> object */
  private List<SREdge> edgesList = new ArrayList<SREdge>();

  @Override
  public void addEdge(SREdge edge) {
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
    str = new StringTokenizer(buffer[0], ":");
    if (str.countTokens() != 3) {
      throw new Exception();
    }

    this.vertexID = str.nextToken();
    double value = Double.valueOf(str.nextToken());
    int count = Integer.parseInt(str.nextToken());
    this.vertexValue = value+":"+count;


      str = new StringTokenizer(buffer[1], ":");
      while (str.hasMoreTokens()) {
        SREdge edge = new SREdge();
        edge.fromString(str.nextToken());
        this.edgesList.add(edge);
      }

    
  }

  @Override
  public List<SREdge> getAllEdges() {
    return this.edgesList;
  }

  @Override
  public int getEdgesNum() {
    return this.edgesList.size();
  }

  @Override
  public String getVertexID() {
    return this.vertexID;
  }

  @Override
  public String getVertexValue() {
    return this.vertexValue;
  }

  @Override
  public String intoString() {
    String buffer = vertexID + ":" + vertexValue;
    buffer = buffer + Constants.KV_SPLIT_FLAG;
    int numEdges = edgesList.size();
    if (numEdges != 0) {
      buffer = buffer + edgesList.get(0).intoString();
    }
    for (int i = 1; i < numEdges; i++) {
      buffer = buffer + ":" +
          edgesList.get(i).intoString();
    }

    return buffer;
  }

  @Override
  public void removeEdge(SREdge edge) {
    this.edgesList.remove(edge);
  }

  @Override
  public void setVertexID(String vertexID) {
    this.vertexID = vertexID;
  }

  @Override
  public void setVertexValue(String vertexValue) {
    this.vertexValue = vertexValue;
  }

  @Override
  public void updateEdge(SREdge edge) {
    removeEdge(edge);
    this.edgesList.add(edge);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readUTF();
    this.vertexValue = in.readUTF();
    this.edgesList.clear();
    int numEdges = in.readInt();
    SREdge edge;
    for (int i = 0; i < numEdges; i++) {
      edge = new SREdge();
      edge.readFields(in);
      this.edgesList.add(edge);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.vertexID);
    out.writeUTF(this.vertexValue);
    out.writeInt(this.edgesList.size());
    for (SREdge edge : edgesList) {
      edge.write(out);
    }
  }




}
