package com.chinamobile.bcbsp.examples.lpclustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;

public class LPClusterEdge extends Edge<Integer, Byte> {

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
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Integer getVertexID() {
		// TODO Auto-generated method stub
		return this.vertexID;
	}

	@Override
	public String intoString() {
		// TODO Auto-generated method stub
	    return this.vertexID + Constants.SPLIT_FLAG + "0";
	}

	@Override
	public void setEdgeValue(Byte edgeValue) {

		
	}

	@Override
	public void setVertexID(Integer vertexID) {
		// TODO Auto-generated method stub
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

	  @Override
	  public boolean equals(Object object) {
	    LPClusterEdge edge = (LPClusterEdge) object;
	    return this.vertexID == edge.getVertexID();
	  }

}
