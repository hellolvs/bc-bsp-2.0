/*
 * PREdge.h
 *
 *  Created on: Dec 23, 2013
 *      Author: root
 */

#ifndef PREDGE_H_
#define PREDGE_H_

#pragma once
#include <string>
#include <vector>
#include <iostream>
#include <stdlib.h>
//#include "Edge.h"
#include "StringUtil.h"
#include "Constants.h"
#include "Pipes.h"

using std::string;
using std::vector;
using std::iostream;
using BSPPipes::StringUtil;

namespace BSPPipes {

class PREdge : public BSPPipes::Edge {

private:
	string vertexID;
	string edgeValue;

public:
	PREdge(string& edgeData) {
		string split = ":";
		vector<string> vec = StringUtil::stringSplit(edgeData,split);
		vertexID = vec[0];
		edgeValue = vec[1];
	}

	PREdge(const string& _vertexID, const string& _edgeValue) {
		vertexID = _vertexID;
		edgeValue = _edgeValue;
	}

	PREdge(const PREdge& edge) {
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

	bool operator ==(const PREdge& edgestring) const {
		return vertexID == edgestring.getVertexID() ? true : false;
	}

	bool operator <(const PREdge& edgestring) const {
		return vertexID < edgestring.getVertexID() ? true : false;
	}

	PREdge& operator =(const PREdge& edgestring) {
		vertexID = edgestring.getVertexID();
		edgeValue = edgestring.getEdgeValue();
		return *this;
	}

	~PREdge() {};

};

}



#endif /* PREDGE_H_ */
