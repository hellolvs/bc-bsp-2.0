/*
 * KMEdge.h
 *
 *  Created on: Jun 5, 2014
 *      Author: root
 */

#ifndef KMEDGE_H_
#define KMEDGE_H_

#pragma once
#include <string>
#include <vector>
#include <iostream>
#include <stdlib.h>
//#include "Edge.h"
#include "../src/StringUtil.h"
#include "../src/Constants.h"
#include "../src/Pipes.h"

using std::string;
using std::vector;
using std::iostream;
using BSPPipes::StringUtil;


namespace BSPPipes {

class KMEdge : public BSPPipes::Edge {

private:
	string vertexID;
	string edgeValue;

public:
	KMEdge(string& edgeData) {
		string split = ":";
		vector<string> vec = StringUtil::stringSplit(edgeData,split);
		vertexID = vec[0];
		edgeValue = vec[1];
	}

	KMEdge(const string& _vertexID, const string& _edgeValue) {
		vertexID = _vertexID;
		edgeValue = _edgeValue;
	}

	KMEdge(const KMEdge& edge) {
		vertexID = edge.getVertexID();
		edgeValue = edge.getEdgeValue();
	}

	void setVertexID(const string& _vertexID) {
		vertexID = _vertexID;
	}

	string getVertexID() const {
		return vertexID;
	}

	void setEdgeValue(const string& _edgeValue) {
		edgeValue = _edgeValue;
	}

	string getEdgeValue() const {
		return edgeValue;
	}

	string intoString() {
		string s = vertexID + SPLIT_FLAG + edgeValue;
		return s ;
	}

	void fromString(string& edgeData) {
		string split = ":";
		vector<string> vec = StringUtil::stringSplit(edgeData,split);
		vertexID = vec[0];
		edgeValue = vec[1];
	}

	bool operator ==(const KMEdge& edgestring) const {
		return vertexID == edgestring.getVertexID() ? true : false;
	}

	bool operator <(const KMEdge& edgestring) const {
		return vertexID < edgestring.getVertexID() ? true : false;
	}

	KMEdge& operator =(const KMEdge& edgestring) {
		vertexID = edgestring.getVertexID();
		edgeValue = edgestring.getEdgeValue();
		return *this;
	}

	~KMEdge() {};

};

}



#endif /* KMEDGE_H_ */
