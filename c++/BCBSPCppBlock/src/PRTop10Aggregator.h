/*
 * PRTop10Aggregator.h
 *
 *  Created on: Mar 18, 2014
 *      Author: root
 */

#ifndef PRTOP10AGGREGATOR_H_
#define PRTOP10AGGREGATOR_H_
#include <string>
#include <vector>
#include <map>
#include <iterator>
#include <algorithm>
#include <string.h>
#include <stdexcept>

#include "Pipes.h"
#include "PRAggregateValueTop10Node.h"
#include "StringUtil.h"


using std::string;
using std::vector;
using std::map;
using std::iterator;
using std::sort;
using std::pair;

namespace BSPPipes {
//the CmpByValue cannot sort 20 numbers,so I write cmp
bool CmpByValue(pair<string,double> p1, pair<string,double> p2) {
	return p1.second < p2.second ? false : true;
}

bool cmp(pair<string,double> p1, pair<string,double> p2) {
	return p1.second > p2.second;
}


class PRTop10Aggregator : public BSPPipes::Aggregator {

public:

	AggregateValue* aggregate(vector<AggregateValue*> aggValues) {
		//ofstream sinfamily;
		//sinfamily.open("/usr/local/termite/bc-bsp-0.1/logs/sjztest.txt",
		//				ios::app);
		//sinfamily<<"aggValues size : "<<aggValues.size()<<endl;
		//sinfamily << "-----into aggregator \n" << endl;
		string topTenNode = (*aggValues[0]).getValue();
		if(topTenNode!=""){
		//sinfamily<<"top ten node :"<<(*aggValues[0]).getValue()<<endl;
		}
		//sinfamily<<"init value: "<<(*aggValues[1]).getValue()<<endl;
		if(""!=topTenNode){
		string pattern = "|";
		topTenNode=(*aggValues[1]).getValue()+pattern+topTenNode;
		//sinfamily<<"top ten node2 :"<<(*aggValues[0]).getValue()<<endl;
		vector<string> eachNode = StringUtil::stringSplit(topTenNode, pattern);
		vector<string>::iterator eachNodeIt = eachNode.begin();
		bool flag=false;
		//sinfamily<<"eachNode size:"<<eachNode.size()<<endl;
		vector<string>::iterator eachNodeIte = eachNode.begin();
		//put each node "key and value" to a vector<pair<string, double>>
		vector<pair<string, double> > vecNode;
		while(eachNodeIte != eachNode.end()) {
			string eachNodeStr = eachNodeIte->data();
			string pattern1 = "=";
			vector<string> node = StringUtil::stringSplit(eachNodeStr, pattern1);
			string key = node[0];
			double value = StringUtil::str2double(node[1]);
			vecNode.push_back(make_pair(key, value));
			//sinfamily << "key: "<<key<<"value: "<<value <<endl;
			++ eachNodeIte;
		}
		//sinfamily << "-----into aggregator00000  vecNode size: " <<vecNode.size()<< endl;
		//sort nodes by the value
		sort(vecNode.begin(), vecNode.end(), cmp);
		//sinfamily << "-----into aggregator1 \n" << endl;
		//build the aggValue
		//sinfamily<<"vecNode size :"<<vecNode.size()<<endl;
		//sinfamily << "-----into aggregator10 \n"<<vecNode[0].first << endl;
		string aggValue = vecNode[0].first + "=" + StringUtil::double2str(vecNode[0].second);
		//sinfamily << "-----into aggregator11  before aggValue "<<aggValue << endl;
		if(vecNode.size()>1){
		for(int i=1; i!=10&&i<vecNode.size(); ++i) {
			//sinfamily << "-----into aggregator11 \n"<< i<<vecNode[i].first << endl;
			string id = vecNode[i].first;
			//sinfamily << "-----into aggregator12 \n"<< i <<vecNode[i].second<< endl;
			double pr = vecNode[i].second;
			//sinfamily << "-----into aggregator13 \n"<< i << endl;
			aggValue = aggValue + "|" + id + "=" + StringUtil::double2str(pr);
		}
		}
		//sinfamily << "-----aggValue-------------------" <<aggValue<< "\n\n\n\n"<<endl;
		aggValues[0]->setValue(aggValue);//added by songjianze
		PRAggregateValueTop10Node result(aggValue);
		}
		else {
			string s = aggValues[1]->getValue();
			aggValues[0]->setValue(s);
		}
		//sinfamily.flush();
		//sinfamily.close();
		return aggValues[0];
	}

	~PRTop10Aggregator() {};

};


}



#endif /* PRTOP10AGGREGATOR_H_ */
