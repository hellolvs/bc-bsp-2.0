/*
 * Pagerank.cpp
 *
 *  Created on: Jan 6, 2014
 *      Author: root
 */
#include "Pipes.h"
#include "PRVertex.h"
#include <vector>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <list>
#include <fstream>
#include <stdio.h>

using namespace std;
using std::vector;
using namespace BSPPipes;
using BSPPipes::BSPMessage;

class Pagerank: public BSPPipes::BSP {
private:
	//PREdge edge;
	list<Edge*> outgoingEdges;
	float receivedMsgvalue;
	float receivedSum;
	float sendMsgvalue;
	float newVertexValue;
	static const double CLICK_RP = 0.0001;
	static const double FACTOR = 0.15;
public:
	Pagerank() {
		receivedSum = 0.0f;
		sendMsgvalue = 0.0f;
	}
	;
	void compute() {

	}
	void compute(vector<BSPMessage> BspMessage, BSPStaffContext* context) {

		vector<BSPMessage>::iterator ite = BspMessage.begin();
		receivedMsgvalue = 0.0f;
		receivedSum = 0.0f;

		while (ite != BspMessage.end()) {
			receivedMsgvalue = atof(ite->getdata().c_str());
			receivedSum = receivedSum+receivedMsgvalue;
			ite++;
		}
		Vertex* vertex = context->getVertex();
		if (context->getCurrentSuperStepCounter() == 0) {
			float x = atof(context->getVertex()->getVertexValue().c_str());
			float outnum = context->getOutgoingEdgesNum();
			sendMsgvalue = x / outnum;
			sendMsgvalue = (atof(context->getVertex()->getVertexValue().c_str()))
					/ (context->getOutgoingEdgesNum());
		} else {
			newVertexValue = CLICK_RP * FACTOR + receivedSum * (1 - FACTOR);
			sendMsgvalue = newVertexValue / context->getOutgoingEdgesNum();
			ostringstream oss;
			oss << newVertexValue;
			string str(oss.str());
			context->getVertex()->setVertexValue(str);
			//context->updateVertex(vertex);
		}
		ostringstream os;
		os << sendMsgvalue;
		string st(os.str());
		outgoingEdges = context->getOutgoingEdges();
		list<Edge*>::iterator it = outgoingEdges.begin();
		while (it != outgoingEdges.end()) {
			string vertexID = (*it)->getVertexID();
			BSPMessage msg(vertexID, st);
			context->sendMessage(msg);
			it++;
		}
		return;
	}

	void compute(vector<BSPMessage*> BspMessage, BSPStaffContext* context) {
		receivedMsgvalue = 0.0f;
		receivedSum = 0.0f;
		vector<BSPMessage*>::iterator ite = BspMessage.begin();
		while (ite != BspMessage.end()) {
			receivedMsgvalue = atof((*ite)->getdata().c_str());
			receivedSum = +receivedMsgvalue;
			ite++;
		}
		Vertex* vertex = context->getVertex();
		PRVertex prVertex(vertex);
		if (context->getCurrentSuperStepCounter() == 0) {
			sendMsgvalue = atof(prVertex.getVertexValue().c_str())
					/ context->getOutgoingEdgesNum();
		} else {
			newVertexValue = CLICK_RP * FACTOR + receivedSum * (1 - FACTOR);
			sendMsgvalue = newVertexValue / context->getOutgoingEdgesNum();
			ostringstream oss;
			oss << newVertexValue;
			string str(oss.str());
			prVertex.setVertexValue(str);
			vertex = &prVertex;
			context->updateVertex(vertex);
		}
		ostringstream os;
		os << sendMsgvalue;
		string st(os.str());
		outgoingEdges = context->getOutgoingEdges();
		list<Edge*>::iterator it = outgoingEdges.begin();
		while (it != outgoingEdges.end()) {
			string vertexID = (*it)->getVertexID();
			BSPMessage msg(vertexID, st);
			context->sendMessage(msg);
			it++;
		}
		return;
	}
	string floatIntoString(float a){
		ostringstream os;
				os << a;
				string st(os.str());
				return st;
	}
	void initBeforeSuperStep(SuperStepContext context){
		//just for test
//		ofstream sinfamily;
//		sinfamily.open("/home/bcbsp/userDefine.txt", ios::trunc);
//		char* test=getenv("userDefine");
//		sinfamily<<"userDefine :"<<test<<endl;
//		sinfamily.flush();
//		sinfamily.close();
	}
	~Pagerank() {
	};
};

