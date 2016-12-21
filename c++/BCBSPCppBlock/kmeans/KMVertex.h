/*
 * KMVertex.h
 *
 *  Created on: Jun 5, 2014
 *      Author: root
 */

#ifndef KMVERTEX_H_
#define KMVERTEX_H_

#pragma once
#include <string>
#include <list>
#include <iterator>
#include "KMEdge.h"
#include "../src/Constants.h"

#include <sstream>
#include <stdlib.h>
#include <string>
#include <list>
#include <fstream>
#include <stdio.h>

using std::string;
using std::list;
using std::iterator;

/**
 * KMVertex
 * Implementation of vertex of k-means
 *
 * @author Song Jianze
 * @version 2.0
 *
 */




namespace BSPPipes {

class KMVertex: public BSPPipes::Vertex {

private:
	string vertexID;
	string vertexValue;
	list<KMEdge> edgeslist;

public:
	KMVertex() {
		vertexID = "";
		vertexValue = "";
	}

	KMVertex(const string& _vertexID, const string& _vertexValue, KMEdge edge) {
		vertexID = _vertexID;
		vertexValue = _vertexValue;
		edgeslist.push_back(edge);
	}
	KMVertex(Vertex* _vertex) {
		vertexID = _vertex->getVertexID();
		vertexValue = _vertex->getVertexValue();

		list<KMEdge> prEdges;
		list<Edge*> edges = _vertex->getAllEdges();
		list<Edge*>::iterator ite = edges.begin();
		while (ite != edges.end()) {
			prEdges.push_back(
					KMEdge((*ite)->getVertexID(), (*ite)->getEdgeValue()));
			++ite;
		}
		edgeslist = prEdges;
	}
	KMVertex(const KMVertex& _vertex) {
		vertexID = _vertex.getVertexID();
		vertexValue = _vertex.getVertexValue();

		list<KMEdge> prEdges;
		list<Edge*> edges = _vertex.getAllEdges();
		list<Edge*>::iterator ite = edges.begin();
		while (ite != edges.end()) {
			prEdges.push_back(
					KMEdge((*ite)->getVertexID(), (*ite)->getEdgeValue()));
			++ite;
		}
		edgeslist = prEdges;
	}

	void setVertexID(const string& _vertexID) {
		vertexID = _vertexID;
	}

	string getVertexID() const {
		return vertexID;
	}

	void setVertexValue(const string& _vertexValue) {
		vertexValue = _vertexValue;
	}

	string getVertexValue() const {
		return vertexValue;
	}

	void addEdge(const string& outgoingID, const string& edgeValue) {
		edgeslist.push_back(KMEdge(outgoingID, edgeValue));
	}

	list<Edge*> getAllEdges() const {

		list<Edge*> edges;
		Edge* p = NULL;
		list<KMEdge>::const_iterator ite = edgeslist.begin();
		while (ite != edgeslist.end()) {
			p = &(const_cast<KMEdge&>(*ite));
			edges.push_back(p);
			++ite;
		}
		return edges;

	}

	void removeEdge(const string& outgoingID, const string& edgeValue) {
		edgeslist.remove(KMEdge(outgoingID, edgeValue));
	}

	void updateEdge(const string& outgoingID, const string& edgeValue) {
		removeEdge(outgoingID, edgeValue);
		edgeslist.push_back(KMEdge(outgoingID, edgeValue));
	}

	int getEdgesNum() const {
		return edgeslist.size();
	}

	string intoString() {
		string ver = vertexID + SPLIT_FLAG + vertexValue;
		string edgs;
		if (edgeslist.size() != 0) {
			list<KMEdge>::iterator ite = edgeslist.begin();
			edgs = ite->intoString();
			++ite;
			while (ite != edgeslist.end()) {
				edgs = edgs + SPACE_SPLIT_FLAG + ite->intoString();
				++ite;
			}
		}
		string result = ver + KV_SPLIT_FLAG + edgs;
		return result;
	}

	void fromString(string& vertexData) {
		ofstream sinfamily;
		sinfamily.open("/home/bcbsp/PRVertex.txt", ios::app);
		string pattern = KV_SPLIT_FLAG;
		vector<string> vertexEdges = StringUtil::stringSplit(vertexData,
				pattern);
		string vertex;
		string edges;
		vertex = vertexEdges[0];
		if (vertexEdges.size() == 2) {
			edges = vertexEdges[1];
		}

		string pattern1 = SPLIT_FLAG;
		vector<string> ver = StringUtil::stringSplit(vertex, pattern1);
		vertexID = ver[0];
		vertexValue = ver[1];

		string pattern2 = SPACE_SPLIT_FLAG;
		vector<string> edge = StringUtil::stringSplit(edges, pattern2);
		sinfamily<<"edgeNum:"<<edge.size()<<endl;
		vector<string>::iterator ite_edge = edge.begin();
		while (ite_edge != edge.end()) {
			KMEdge edge(*ite_edge);
			addEdge(edge.getVertexID(), edge.getEdgeValue());
			//edgeslist.push_back(KMEdge(*ite_edge));
			++ite_edge;
		}
		list<KMEdge>::iterator ite=edgeslist.begin();
		sinfamily<<vertexID<<endl;
		while(ite!=edgeslist.end()){
			sinfamily<<ite->getVertexID()<<":"<<ite->getEdgeValue()<<endl;
			ite++;
		}
		sinfamily.flush();
		sinfamily.close();
	}

	bool operator ==(const KMVertex& _vertex) const {
		return vertexID == _vertex.getVertexID()
				&& vertexValue == _vertex.getVertexValue() ? true : false;
	}

	bool operator <(const KMVertex& _vertex) const {
		return vertexID < _vertex.getVertexID() ? true : false;
	}

	KMVertex& operator =(KMVertex& _vertex) {
		vertexID = _vertex.getVertexID();
		vertexValue = _vertex.getVertexValue();

		list<KMEdge> prEdges;
		list<Edge*> edges = _vertex.getAllEdges();
		list<Edge*>::iterator ite = edges.begin();
		while (ite != edges.end()) {
			prEdges.push_back(
					KMEdge((*ite)->getVertexID(), (*ite)->getEdgeValue()));
			++ite;
		}

		edgeslist = prEdges;
		return *this;
	}

	~KMVertex() {
	}
	;

};

}


#endif /* KMVERTEX_H_ */
