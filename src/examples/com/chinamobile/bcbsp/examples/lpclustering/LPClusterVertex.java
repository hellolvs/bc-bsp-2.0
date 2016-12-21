package com.chinamobile.bcbsp.examples.lpclustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;

public class LPClusterVertex extends Vertex<Integer, Float, LPClusterEdge>{

	  /** State vertex ID */
	  private int vertexID = 0;
	  /** State vertex value */
	  private Float vertexValue = 0f;
	  /** State vertex value */
	  private int vertexLabel = 0;
	  
	  public int getVertexLabel() {
		return vertexLabel;
	}

	public void setVertexLabel(int vertexLabel) {
		this.vertexLabel = vertexLabel;
	}

	/** Create a List<SSPEdge> object */
	  private List<LPClusterEdge> edgesList = new ArrayList<LPClusterEdge>();
	  
	@Override
	public void addEdge(LPClusterEdge edge) {
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
	    if (str.countTokens() == 2) {
	    this.vertexID = Integer.valueOf(str.nextToken());
	    this.vertexValue = Float.valueOf(str.nextToken());
	    this.vertexLabel=this.vertexID;
	    }
	    if (str.countTokens() == 3){
	        this.vertexID = Integer.valueOf(str.nextToken());
		    this.vertexValue = Float.valueOf(str.nextToken());
	    	this.vertexLabel=Integer.valueOf(str.nextToken());
	    }
	    if (buffer[1].length() > 0) { // There has edges.
	      str = new StringTokenizer(buffer[1], Constants.SPACE_SPLIT_FLAG);
	      while (str.hasMoreTokens()) {
	    	  LPClusterEdge edge = new LPClusterEdge();
	        edge.fromString(str.nextToken());
	        this.edgesList.add(edge);
	      }
	    }
		
	}

	@Override
	public List<LPClusterEdge> getAllEdges() {
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
	public Float getVertexValue() {
		return this.vertexValue;
	}

	@Override
	public String intoString() {
	    String buffer = vertexID + Constants.SPLIT_FLAG + vertexValue+ Constants.SPLIT_FLAG + vertexLabel;
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
	public void removeEdge(LPClusterEdge edge) {
		this.edgesList.remove(edge);
		
	}

	@Override
	public void setVertexID(Integer vertexID) {
		this.vertexID = vertexID;
		
	}

	@Override
	public void updateEdge(LPClusterEdge edge) {
	    removeEdge(edge);
	    this.edgesList.add(edge);
		
	}
	  @Override
	  public void readFields(DataInput in) throws IOException {
	    this.vertexID = in.readInt();
	    this.vertexValue = in.readFloat();
	    this.vertexLabel=in.readInt();
	    this.edgesList.clear();
	    int numEdges = in.readInt();
	    LPClusterEdge edge;
	    for (int i = 0; i < numEdges; i++) {
	      edge = new  LPClusterEdge();
	      edge.readFields(in);
	      this.edgesList.add(edge);
	    }
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeInt(this.vertexID);
	    out.writeFloat(this.vertexValue);
	    out.writeInt(this.vertexLabel);
	    out.writeInt(this.edgesList.size());
	    for ( LPClusterEdge edge : edgesList) {
	      edge.write(out);
	    }
	  }

	  @Override
	  public int hashCode() {
	    return Integer.valueOf(this.vertexID).hashCode();
	  }

	@Override
	public void setVertexValue(Float arg0) {
		// TODO Auto-generated method stub
		
	}

}
