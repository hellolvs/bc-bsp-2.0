/*
 * StringUtil.h
 *
 *  Created on: Dec 23, 2013
 *      Author: root
 */

#ifndef STRINGUTIL_H_
#define STRINGUTIL_H_

#pragma once
#include <string>
#include <vector>
#include <sstream>
using std::string;
using std::vector;
using std::stringstream;

namespace BSPPipes {

class StringUtil {

public:
	//字符串分割函数
	static vector<string> stringSplit(string& str,string& pattern) {
		string::size_type pos;
		vector<string> result;
		string tmp=str;
		tmp+=pattern;//扩展字符串以方便操作

		for(string::size_type i=0; i<tmp.size(); i++)
		{
			pos=tmp.find(pattern,i);
			if(pos<tmp.size())
			{
				string s=tmp.substr(i,pos-i);
				result.push_back(s);
				i=pos+pattern.size()-1;
			}
		}
		return result;
	}

	template <class T>
	int getArrayLen(T& array){
		return (sizeof(array) / sizeof(array[0]));
	}

	static string int2str(int num) {
		stringstream stream;
		string str;
		stream << num;
		str = stream.str();
		return str;
	}

	static int str2int(string num) {
		int a = atoi(num.c_str());
		return a;
	}

	static string float2str(float num){
		stringstream stream;
		string str;
		stream << num;
		str = stream.str();
		return str;
	}

	static float str2float(string& str){
		stringstream stream;
		float f;
		stream << str;
		stream >> f;
		return f;
	}

	static string double2str(double num){
		stringstream stream;
		string str;
		stream << num;
		str = stream.str();
		return str;
	}

	static double str2double(string& str){
		stringstream stream;
		double d;
		stream << str;
		stream >> d;
		return d;
	}




};

}


#endif /* STRINGUTIL_H_ */
