package com.chinamobile.bcbsp.examples.copra;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;

public class C_Vertex extends Vertex<String, String, C_Edge> {
	
	private String vertexID;
	private String labels;
	private List<C_Edge> edges = new ArrayList<C_Edge>();
	
	@Override
	public void addEdge(C_Edge edge) {
		edges.add(edge);
	}
	
	@Override
	public void fromString(String vertex) throws Exception {
		String[] vertexInfos = vertex.split(Constants.KV_SPLIT_FLAG); 
		String[] vInfos = vertexInfos[0].split(Constants.SPLIT_FLAG);
		vertexID = vInfos[0];
		labels = vInfos[1];
		
		//has edges
		if(vertexInfos[1].length() != 0) {
			String[] edgeInfos = vertexInfos[1].split(Constants.SPACE_SPLIT_FLAG);
			for(int i = 0; i != edgeInfos.length; ++ i) {
				C_Edge edge = new C_Edge();
				edge.fromString(edgeInfos[i]);
				edges.add(edge);
			}
		}
	}
	
	@Override
	public List<C_Edge> getAllEdges() {
		return edges;
	}

	@Override
	public int getEdgesNum() {
		return edges.size();
	}
	
	@Override
	public String getVertexID() {
		return vertexID;
	}

	@Override
	public String getVertexValue() {
		return labels;
	}
	
	@Override
	public String intoString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append(vertexID + Constants.SPLIT_FLAG + labels);
		sb.append(Constants.KV_SPLIT_FLAG);
		
		Iterator<C_Edge> it_E = edges.iterator();
		while(it_E.hasNext()) {
			sb.append(it_E.next().intoString() + Constants.SPACE_SPLIT_FLAG);
		}
		
		return sb.toString();
	}
	
	@Override
	public void removeEdge(C_Edge edge) {
		edges.remove(edge);
	}

	@Override
	public void setVertexID(String id) {
		vertexID = id;
	}

	@Override
	public void setVertexValue(String vertex) {
		labels = vertex;
	}

	@Override
	public void updateEdge(C_Edge edge) {
		removeEdge(edge);
		edges.add(edge);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		vertexID = in.readUTF();
		labels = in.readUTF();
		
		int edgeNum = in.readInt();
		C_Edge edge;
		for(int i = 0; i != edgeNum; ++ i) {
			edge = new C_Edge();
			edge.readFields(in);
			edges.add(edge);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(vertexID);
		out.writeUTF(labels);
		
		out.writeInt(edges.size());
		for(C_Edge edge : edges) {
			edge.write(out);
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		C_Vertex vertex = (C_Vertex)obj;
		return vertexID.equals(vertex.getVertexID());
	}
	
	@Override
	public int hashCode() {
		return vertexID.hashCode();
	}
}