/*
 * RecordParseKmeans.h
 *
 *  Created on: Jul 30, 2014
 *      Author: root
 */

#ifndef RECORDPARSEKMEANS_H_
#define RECORDPARSEKMEANS_H_


#pragma once
#include <string>
#include <vector>
#include "../src/Pipes.h"
#include "../src/Constants.h"
#include "../src/StringUtil.h"

#include <sstream>
#include <stdlib.h>
#include <string>
#include <list>
#include <fstream>
#include <stdio.h>

using std::string;
using std::vector;

namespace BSPPipes {

class RecordParseKmeans : public BSPPipes::RecordParse {

public:

	Vertex* recordParse(string& key, string& value) const {
		//ofstream sinfamily;
		Vertex* Vertex = new KMVertex();
		string kv=key + KV_SPLIT_FLAG + value;
		//sinfamily.open("/home/bcbsp/RecordParseDefault.txt", ios::app);
		//sinfamily<<"KV:"<<kv<<endl;
		Vertex->fromString(kv);
		/*sinfamily<<"Vertex"<<prVertex->getVertexID()<<"EdgesNum:"<<prVertex->getEdgesNum()<<endl;
		sinfamily.flush();
		sinfamily.close();*/
		return Vertex;
	}

	string getVertexID(string& key) const {
		string pattern = SPLIT_FLAG;
		vector<string> vec = StringUtil::stringSplit(key, pattern);
		string vertexID = vec[0];
		return vertexID;
	}

};

}


#endif /* RECORDPARSEKMEANS_H_ */
