package com.chinamobile.bcbsp.examples.copra;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;

public class C_Edge extends Edge<String, String> {

	private String vertexID;
	private String label;            // not used

	@Override
	public boolean equals(Object obj) {
		C_Edge edge = (C_Edge)obj;
		return vertexID.equals(edge.getVertexID());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		vertexID = in.readUTF();
		label = in.readUTF();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(vertexID);
		out.writeUTF(label);
	}
	
	@Override
	public void fromString(String edge) throws Exception {
		String[] edgeInfos = edge.split(Constants.SPLIT_FLAG);
		vertexID = edgeInfos[0];
		label = edgeInfos[1];
	}
	
	@Override
	public String getEdgeValue() {
		return label;
	}
	@Override
	public String getVertexID() {
		return vertexID;
	}
	
	@Override
	public String intoString() {
		return vertexID + Constants.SPLIT_FLAG + label;
	}
	
	@Override
	public void setEdgeValue(String lab) {
		label = lab;
	}
	
	@Override
	public void setVertexID(String id) {
		vertexID = id;
	}	
}