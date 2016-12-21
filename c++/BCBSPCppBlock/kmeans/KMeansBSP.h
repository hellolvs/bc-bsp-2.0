/*
 * KMeansBSP.h
 *
 *  Created on: Jun 10, 2014
 *      Author: root
 */

#ifndef KMEANSBSP_H_
#define KMEANSBSP_H_
#include <stdlib.h>

#include "../src/Pipes.h"

using namespace std;
namespace BSPPipes {
class KMeansBSP : public BSPPipes::BSP{
public:
	int k;
	int superstepcount;
	int dimension;
	string kCenters;
	void compute(vector<BSPMessage> BspMessage,
				BSPStaffContext* context){
		superstepcount=context->getCurrentSuperStepCounter();
		Vertex* vertex=context->getVertex();
		list<Edge*> edges=vertex->getAllEdges();
		string pattern="|";
				vector<string> centers=StringUtil::stringSplit(this->kCenters,pattern);
				int tag=0;
				int i=0;
				float mindistance=0;
				for(vector<string>::iterator it=centers.begin();it!=centers.end();it++,i++){
					string pattern1="=";
					vector<string> center=StringUtil::stringSplit((*it),pattern1);
					float distance=distanceof(center,edges);
					if(i!=0){
						if(mindistance>distance){
							tag=i;
							mindistance=distance;
						}
					}else{
						mindistance=distance;
					}
				}//for
				vertex->setVertexValue(StringUtil::int2str(tag));
	}

	//(x1-x2)^2+(y1-y2)^2+.....+(z1-z2)^2
		float distanceof(vector<string> c,list<Edge*> e){
			vector<string>::iterator it=c.begin();
			float result=0;
			for(list<Edge*>::iterator ite=e.begin();ite!=e.end();ite++,it++){
				float x = atof((*it).c_str());
				float y = atof((*ite)->getEdgeValue().c_str());
				result=(x-y)*(x-y)+result;
			}
			return result;
		}
	void initBeforeSuperStep(SuperStepContext context){
		this->superstepcount=context.getSuperstep();
		if (this->superstepcount == 0) {
			char* userDefine = getenv("userDefine");
			string arg = userDefine;
			string pattern = "/";
			vector<string> args = StringUtil::stringSplit(arg, pattern);
			this->k = atoi(args[0].c_str());
			//this->k = args[0];
			this->kCenters = args[1];
			string pattern1 = "|";
			vector<string> centers = StringUtil::stringSplit(this->kCenters,pattern1);
			string pattern2 = "=";
			vector<string> dimensions = StringUtil::stringSplit(centers[0],pattern2);
			this->dimension = dimensions.size();
		}else{
			//Calculate the newkCenters
			string temp=context.getAggregateValues().back()->getValue();
			string pattern="|";
			vector<string> args=StringUtil::stringSplit(temp,pattern);
			this->k=args.size()-1;
			string num=args.back();
			args.pop_back();
			string pattern1="=";
			vector<string> nums=StringUtil::stringSplit(num,pattern1);
			this->dimension=nums.size();

			vector<string>::iterator itee = nums.begin();//compute the new kcenters
			for (vector<string>::iterator it = args.begin(); it != args.end();
					it++, itee++) {
				vector<string> center = StringUtil::stringSplit((*it),
						pattern1);
				string arg;
				for (vector<string>::iterator centers = center.begin();
						centers != center.end(); centers++) {
					if (centers != center.begin()) {
						arg = arg + "=";
					}
					float x = atof((*centers).c_str());
					float y = atof((*itee).c_str());
					if(y!=0){
						x = x / y;
					}else{
						x=y;
					}
					ostringstream oss;
					oss << x;
					string str(oss.str());
					*centers = str;
					arg = arg + str;
				}
				*it = arg;

			}
			string kcenter;
			for (vector<string>::iterator it = args.begin(); it != args.end();
					it++) {

				if (it != args.begin()) {
					kcenter = kcenter + "|";
				}
				kcenter = kcenter + (*it);
			}
			this->kCenters=kcenter;

		}
	}
};
}




#endif /* KMEANSBSP_H_ */
