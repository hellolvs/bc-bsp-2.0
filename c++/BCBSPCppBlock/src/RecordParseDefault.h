#pragma once
#include <string>
#include <vector>
#include "Pipes.h"
#include "PREdge.h"
#include "PRVertex.h"
#include "Constants.h"
#include "StringUtil.h"

#include <sstream>
#include <stdlib.h>
#include <string>
#include <list>
#include <fstream>
#include <stdio.h>

using std::string;
using std::vector;

namespace BSPPipes {

class RecordParseDefault : public BSPPipes::RecordParse {

public:
	
	Vertex* recordParse(string& key, string& value) const {
		Vertex* prVertex = new PRVertex();
		string kv=key + KV_SPLIT_FLAG + value;
		prVertex->fromString(kv);
		return prVertex;
	}
	
	string getVertexID(string& key) const {
		string pattern = SPLIT_FLAG;
		vector<string> vec = StringUtil::stringSplit(key, pattern);
		string vertexID = vec[0];
		return vertexID;
	}
	
};

}
