/*
 * KMAggregator.h
 *
 *  Created on: Jun 5, 2014
 *      Author: root
 */

#ifndef KMAGGREGATOR_H_
#define KMAGGREGATOR_H_
#include <vector>
using namespace std;


namespace BSPPipes {
class KMAggregator : public BSPPipes::Aggregator {
public:
	AggregateValue* aggregate(vector<AggregateValue*> aggValues){
		string contents;
		vector<vector<string> > content;
		string pattern="|";
		string pattern1="=";
		for(vector<AggregateValue*>::iterator it=aggValues.begin();it!=aggValues.end();it++){
			if(it==aggValues.begin()){
				contents=(*it)->getValue();
				vector<string> temp=StringUtil::stringSplit(contents,pattern);
				for(vector<string>::iterator ite=temp.begin();ite!=temp.end();ite++){
					content.push_back(StringUtil::stringSplit((*ite),pattern1));
				}
			}else{
				contents=(*it)->getValue();
				vector<vector<string> > ncentent;
				vector<string> temp = StringUtil::stringSplit(contents,
						pattern);
				for (vector<string>::iterator ite = temp.begin();
						ite != temp.end(); ite++) {
					ncentent.push_back(StringUtil::stringSplit((*ite), pattern1));
				}
				content=sum(content,ncentent);
			}
		}//for
		contents.clear();
		for(vector<vector<string> >::iterator cit=content.begin();cit!=content.end();cit++){
			string coordinate;
			if(cit!=content.begin()){
				contents=contents+"|";
			}
			for(vector<string> ::iterator cite=(*cit).begin();cite!=(*cit).end();cite++){
				if(cite!=(*cit).begin()){
					coordinate=coordinate+"=";
				}
				coordinate=coordinate+(*cite);
			}
			contents=contents+coordinate;
			coordinate.clear();
		}
		AggregateValue* aggV=aggValues.front();
		aggV->setValue(contents);
		return aggV;
	}
	vector<vector<string> > sum(vector<vector<string> > a,vector<vector<string> > b){
		vector<vector<string> >::iterator ite=b.begin();
		for(vector<vector<string> >::iterator it=a.begin();it!=a.end();it++,ite++){
			*it=sum1(*(it),(*ite));
		}
		return a;
	}
	vector<string> sum1(vector<string> a,vector<string> b){
		vector<string>::iterator ite=b.begin();
		for(vector<string> ::iterator it=a.begin();it!=a.end()&ite!=b.end();it++,ite++){
			float x = atof((*it).c_str());
			float y = atof((*ite).c_str());
			x=x+y;
			ostringstream oss;
			oss << x;
			string str(oss.str());
			*it=str;
		}
		return a;
	}

};
}
#endif /* KMAGGREGATOR_H_ */
