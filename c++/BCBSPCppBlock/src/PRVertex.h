/*
 * PRVertex.h
 *
 *  Created on: Dec 23, 2013
 *      Author: root
 */

#ifndef PRVERTEX_H_
#define PRVERTEX_H_

#pragma once
#include <string>
#include <list>
#include <iterator>
#include "PREdge.h"
#include "Constants.h"

#include <sstream>
#include <stdlib.h>
#include <string>
#include <list>
#include <fstream>
#include <stdio.h>

using std::string;
using std::list;
using std::iterator;

namespace BSPPipes {

class PRVertex: public BSPPipes::Vertex {

private:
	string vertexID;
	string vertexValue;
	list<PREdge> edgeslist;

public:
	PRVertex() {
		vertexID = "";
		vertexValue = "";
	}

	PRVertex(const string& _vertexID, const string& _vertexValue, PREdge edge) {
		vertexID = _vertexID;
		vertexValue = _vertexValue;
		edgeslist.push_back(edge);
	}
	PRVertex(Vertex* _vertex) {
		vertexID = _vertex->getVertexID();
		vertexValue = _vertex->getVertexValue();

		list<PREdge> prEdges;
		list<Edge*> edges = _vertex->getAllEdges();
		list<Edge*>::iterator ite = edges.begin();
		while (ite != edges.end()) {
			prEdges.push_back(
					PREdge((*ite)->getVertexID(), (*ite)->getEdgeValue()));
			++ite;
		}
		edgeslist = prEdges;
	}
	PRVertex(const PRVertex& _vertex) {
		vertexID = _vertex.getVertexID();
		vertexValue = _vertex.getVertexValue();

		list<PREdge> prEdges;
		list<Edge*> edges = _vertex.getAllEdges();
		list<Edge*>::iterator ite = edges.begin();
		while (ite != edges.end()) {
			prEdges.push_back(
					PREdge((*ite)->getVertexID(), (*ite)->getEdgeValue()));
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
		edgeslist.push_back(PREdge(outgoingID, edgeValue));
	}

	list<Edge*> getAllEdges() const {

		list<Edge*> edges;
		Edge* p = NULL;
		list<PREdge>::const_iterator ite = edgeslist.begin();
		while (ite != edgeslist.end()) {
			p = &(const_cast<PREdge&>(*ite));
			edges.push_back(p);
			++ite;
		}
		return edges;

	}

	void removeEdge(const string& outgoingID, const string& edgeValue) {
		edgeslist.remove(PREdge(outgoingID, edgeValue));
	}

	void updateEdge(const string& outgoingID, const string& edgeValue) {
		removeEdge(outgoingID, edgeValue);
		edgeslist.push_back(PREdge(outgoingID, edgeValue));
	}

	int getEdgesNum() const {
		return edgeslist.size();
	}

	string intoString() {
		string ver = vertexID + SPLIT_FLAG + vertexValue;
		string edgs;
		if (edgeslist.size() != 0) {
			list<PREdge>::iterator ite = edgeslist.begin();
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
		//ofstream sinfamily;
		//sinfamily.open("/home/bcbsp/PRVertex.txt", ios::app);
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
		//sinfamily<<"edgeNum:"<<edge.size()<<endl;
		vector<string>::iterator ite_edge = edge.begin();
		while (ite_edge != edge.end()) {
			PREdge edge(*ite_edge);
			addEdge(edge.getVertexID(), edge.getEdgeValue());
			//edgeslist.push_back(PREdge(*ite_edge));
			++ite_edge;
		}
		list<PREdge>::iterator ite=edgeslist.begin();
		//sinfamily<<vertexID<<endl;
		while(ite!=edgeslist.end()){
			//sinfamily<<ite->getVertexID()<<":"<<ite->getEdgeValue()<<endl;
			ite++;
		}
		//sinfamily.flush();
		//sinfamily.close();
	}

	bool operator ==(const PRVertex& _vertex) const {
		return vertexID == _vertex.getVertexID()
				&& vertexValue == _vertex.getVertexValue() ? true : false;
	}

	bool operator <(const PRVertex& _vertex) const {
		return vertexID < _vertex.getVertexID() ? true : false;
	}

	PRVertex& operator =(PRVertex& _vertex) {
		vertexID = _vertex.getVertexID();
		vertexValue = _vertex.getVertexValue();

		list<PREdge> prEdges;
		list<Edge*> edges = _vertex.getAllEdges();
		list<Edge*>::iterator ite = edges.begin();
		while (ite != edges.end()) {
			prEdges.push_back(
					PREdge((*ite)->getVertexID(), (*ite)->getEdgeValue()));
			++ite;
		}

		edgeslist = prEdges;
		return *this;
	}

	~PRVertex() {
	}
	;

};

}

#endif /* PRVERTEX_H_ */
