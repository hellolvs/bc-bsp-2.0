/*
 * KMAggregateValue.h
 *
 *  Created on: Jun 5, 2014
 *      Author: root
 */

#ifndef KMAGGREGATEVALUE_H_
#define KMAGGREGATEVALUE_H_
#include <string>


#include "../src/Pipes.h"

#include "../src/StringUtil.h"

using std::string;

namespace BSPPipes {

class KMAggregateValue: public BSPPipes::AggregateValue {
	/**
	 * First k records are coordinates sum of k classes for current super step,
	 * last 1 record are numbers of k classes for current super step.
	 * As follows: (k+1) rows totally.
	 * --------------------
	 * S11:S12:...:S1n|S21:S22:...:S2n|...|Sk1:Sk2: ... :Skn|N1:N2: ... :Nk
	 */

private:
	string kCenters;
	string contents;
	int k;
	int dimension;

public:
	KMAggregateValue() {
		ofstream sinfamily;
		sinfamily.open("/home/bcbsp/bc-bsp-0.1/logs/test/PRAggregateValueTop10Node.txt",
						ios::trunc);
				sinfamily << "-----construct PRAggregateValueTop10Node \n" << endl;\
				sinfamily.flush();
				sinfamily.close();

				kCenters = "";
				k=0;
				dimension=0;
	}

	/*KMAggregateValue(string& _s) {
		kCenters = _s;
	}

	KMAggregateValue(const PRAggregateValueTop10Node& prAVT10N) {
		kCenters = prAVT10N.getValue();
	}*/

	string getValue() const {
		return contents;
	}
	void setValue(string& value) {
		contents = value;
	}

	void initValue(string& s) {
		contents=s;
		/*
		string pattern="-";
		vector<string> temp=StringUtil::stringSplit(s,pattern);
		this->dimension=atoi(temp.back().c_str());
		temp.pop_back();
		this->k=atoi(temp.back().c_str());
		temp.pop_back();
		this->contents=temp.back();
		temp.pop_back();
		this->kCenters=temp.back();
	*/}

	void initValue(vector<BSPMessage> messages, AggregationContext agg) {
		this->contents="";
		string id = agg.getVertex()->getVertexID();
		string pr = agg.getVertex()->getVertexValue();
		list<Edge*> edges=agg.getVertex()->getAllEdges();
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
		for (int m = 0; m < this->k; m++) {
			if(m!=0){
				this->contents=this->contents+"|";
			}
			if (tag == m) {
				string coordinate;
				for(list<Edge*>::iterator ite=edges.begin();ite!=edges.end();ite++){
					if(ite!=edges.begin()){
						coordinate=coordinate+"=";
					}
					coordinate=coordinate+(*ite)->getEdgeValue();
				}
				this->contents=contents+coordinate;
			} else {
				for (int n = 0; n < this->dimension; n++)
				{
					if(n!=0){
						this->contents=this->contents+"=";
					}
					this->contents=this->contents+"0";
				}
			}
		}//for
		string num;
		for(int l=0;l<this->k;l++){
			if(l!=0){
				num=num+"=";
			}
			if(l==tag){
				num=num+"1";
			}else{
				num=num+"0";
			}
		}//end of for
		this->contents=this->contents+"|"+num;
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

	void initValue(vector<BSPMessage> messages) {

	}

	void initbeforeSuperStep(SuperStepContext* ssContext) {
		int superStepCount = ssContext->getSuperstep();
		//test


		if(ssContext->getSuperstep()==0){
			char* userDefine=getenv("userDefine");
			string arg=userDefine;
			string pattern="/";
			vector<string> args=StringUtil::stringSplit(arg, pattern);
			this->k=atoi( args[0].c_str() );
			//this->k=args[0];
			this->kCenters=args[1];
			string pattern1="|";
			vector<string> centers=StringUtil::stringSplit(this->kCenters,pattern1);
			string pattern2="=";
			vector<string> dimensions=StringUtil::stringSplit(centers[0],pattern2);
			this->dimension=dimensions.size();

		}else{
			string temp=ssContext->getAggregateValues().back()->getValue();
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
			string kcenters;
			for (vector<string>::iterator it = args.begin(); it != args.end();
					it++) {
				if (it != args.begin()) {
					kcenters = kcenters + "|";
				}
				kcenters = kcenters + (*it);
			}
			this->kCenters=kcenters;

		}
	}
	string toString() const {
		return kCenters+"-"+contents+"-"+StringUtil::int2str(k)+"-"+StringUtil::int2str(dimension);
	}
	~KMAggregateValue() {
	}
	;

};

}



#endif /* KMAGGREGATEVALUE_H_ */
