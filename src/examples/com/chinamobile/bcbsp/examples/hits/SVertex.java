/**
 * PRVertexLite.java
 */

package com.chinamobile.bcbsp.examples.hits;

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
public class SVertex extends Vertex<Integer, String, SEdge> {

  /** State vertex ID */
  private Integer vertexID = 0;
  /** State vertex value */
  private String vertexValue = "";
  /** create a List<PREdgeLite> object */
  private List<SEdge> edgesList = new ArrayList<SEdge>();

  @Override
  public void addEdge(SEdge edge) {
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

    this.vertexID = Integer.parseInt(str.nextToken());
    double vertexAuthorityValue = Double.valueOf(str.nextToken());
    double vertexHubValue = Double.valueOf(str.nextToken());
    this.vertexValue = vertexAuthorityValue+":"+vertexHubValue;


      str = new StringTokenizer(buffer[1], " ");
      while (str.hasMoreTokens()) {
        SEdge edge = new SEdge();
        edge.fromString(str.nextToken());
        this.edgesList.add(edge);
      }

    
  }

  @Override
  public List<SEdge> getAllEdges() {
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
  public void removeEdge(SEdge edge) {
    this.edgesList.remove(edge);
  }

  @Override
  public void setVertexID(Integer vertexID) {
    this.vertexID = vertexID;
  }

  @Override
  public void setVertexValue(String vertexValue) {
    this.vertexValue = vertexValue;
  }

  @Override
  public void updateEdge(SEdge edge) {
    removeEdge(edge);
    this.edgesList.add(edge);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexID = in.readInt();
    this.vertexValue = in.readUTF();
    this.edgesList.clear();
    int numEdges = in.readInt();
    SEdge edge;
    for (int i = 0; i < numEdges; i++) {
      edge = new SEdge();
      edge.readFields(in);
      this.edgesList.add(edge);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.vertexID);
    out.writeUTF(this.vertexValue);
    out.writeInt(this.edgesList.size());
    for (SEdge edge : edgesList) {
      edge.write(out);
    }
  }




}
