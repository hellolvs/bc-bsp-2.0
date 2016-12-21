/*
 * PRAggregateValueTop10Node.h
 *
 *  Created on: Dec 27, 2013
 *      Author: root
 */

#include <string>
#include "Pipes.h"
#include "PREdge.h"
#include "PRVertex.h"

using std::string;

namespace BSPPipes {

class PRAggregateValueTop10Node: public BSPPipes::AggregateValue {
	/**
	 * In the form as follows:
	 * id1=pr1|id2=pr2|...|id10=pr10
	 */

private:
	string topTenNode;

public:
	PRAggregateValueTop10Node() {
		/*ofstream sinfamily;
		sinfamily.open("/home/bcbsp/bc-bsp-0.1/logs/test/PRAggregateValueTop10Node.txt",
						ios::trunc);
				sinfamily << "-----construct PRAggregateValueTop10Node \n" << endl;\
				sinfamily.flush();
				sinfamily.close();
*/
		topTenNode = "";
	}

	PRAggregateValueTop10Node(string& _topTenNode) {
		topTenNode = _topTenNode;
	}

	PRAggregateValueTop10Node(const PRAggregateValueTop10Node& prAVT10N) {
		topTenNode = prAVT10N.getValue();
	}

	string getValue() const {
		return topTenNode;
		//just for test
		//return "aaaaaaaa";
	}
	void setValue(string& value) {
		topTenNode = value;
	}

	void initValue(string& s) {
		topTenNode = s;
	}

	void initValue(vector<BSPMessage>::iterator it_messages, int superStep,
			Vertex* vertex) {

		string id = vertex->getVertexID();
		string pr = vertex->getVertexValue();
		topTenNode = id + "=" + pr;
	}
	void initValue(vector<BSPMessage> messages, AggregationContext agg) {
		/*ofstream sinfamily;
		sinfamily.open("/home/bcbsp/bc-bsp-0.1/logs/test/aggregate.txt",ios::trunc);
		sinfamily << "-----into initValue \n" << endl;*/
		string id = agg.getVertex()->getVertexID();
		string pr = agg.getVertex()->getVertexValue();
		//sinfamily <<"vertex id is : "<<agg.getVertex()->getVertexValue()<<endl;
	//	sinfamily << "-----into initValue1 \n" << endl;
		topTenNode = id + "=" + pr;
		//sinfamily << "-----into initValue2 \n"<<topTenNode << endl;
		//sinfamily.flush();
		//sinfamily.close();
	}

	void initValue(vector<BSPMessage> messages) {

	}

	void initbeforeSuperStep(SuperStepContext* ssContext) {
		/*ofstream sinfamily;
		sinfamily.open(
				"/home/bcbsp/bc-bsp-0.1/logs/test/initBeforeSuperstep2.txt",
				ios::trunc);
		sinfamily << "---------pagerank initBeforeSuperStep   " << endl;*/

		int superStepCount = ssContext->getSuperstep();
		//sinfamily.flush();
		//sinfamily.close();
	}
	string toString() const {
		return topTenNode;
	}
	~PRAggregateValueTop10Node() {
	}
	;

};

}
