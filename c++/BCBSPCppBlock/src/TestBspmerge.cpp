/*
 * TestBspmerge.cpp
 *
 *  Created on: Jul 21, 2014
 *      Author: root
 */

#include "TestBspmerge.h"
#include "Pipes.h"
//#include "BspPipes.cpp"

//#include <string>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <strstream>

#include "PRVertex.h"
#include "PREdge.h"
#include "RecordParseDefault.h"


/*
#include <cppunit/Message.h>
#include <cppunit/AdditionalMessage.h>
#include <cppunit/SourceLine.h>
#include <cppunit/Asserter.h>
#include <cppunit/Portability.h>
#include <cppunit/Exception.h>
#include <cppunit/Asserter.h>
#include <cppunit/portability/Stream.h>
#include <cppunit/TestAssert.h>
*/
#include <cppunit/Asserter.h>



using namespace std;
using namespace CppUnit;

using CppUnit::Asserter;

namespace BSPPipes {


CPPUNIT_TEST_SUITE_REGISTRATION(TestBspmerge);
//CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(TestBspmerge , "message");

void TestBspmerge::setUp()
{

}

/*清除动作*/
void TestBspmerge::tearDown()
{

}

void TestBspmerge::testBspmessageConstructor(){
	string a="aaa";
	string b="bbb";
	string c= "ccc";
	int x = 5;
	BSPMessage message(x,a,b,c);
	const char* s1=a.c_str();
	const char* s2=message.getDstVertexID().c_str();
	const char* s3=b.c_str();
	const char* s4=message.getTag().c_str();
	const char* s5=c.c_str();
	const char* s6=message.getdata().c_str();

	CPPUNIT_ASSERT_MESSAGE("BSPMessage state",!strcmp(s3,s4)&&
			!strcmp(s1,s2) && !strcmp(s5,s6) && x==message.getDstPartition() );
}

void TestBspmerge::testBspmessageConstructor1(){
	string a="aaa";
	string b="bbb";
	BSPMessage message(a,b);
	const char* s1=a.c_str();
	const char* s2=message.getDstVertexID().c_str();
	const char* s3=b.c_str();
	const char* s4=message.getTag().c_str();
	const char* s6=message.getdata().c_str();
	//CppUnit::Asserter::failIf("BSPMessage state",!strcmp(s1,s2) &&!strcmp(s6,s3) && 0==message.getDstPartition());
	CPPUNIT_ASSERT_MESSAGE("BSPMessage state",!strcmp(s1,s2) &&!strcmp(s6,s3) && 0==message.getDstPartition());
}

void TestBspmerge::testBspmessageConstructor2(){
	string a="aaa";
	string b="bbb";
	string c= "ccc";
	//cout<<"testBspmessageConstructor2"<<endl;
	int x = 5;
	BSPMessage message(x,a,b,c);
	BSPMessage message2(message);
	const char* s1=message2.getDstVertexID().c_str();
	const char* s2=message.getDstVertexID().c_str();
	const char* s3=message2.getTag().c_str();
	const char* s4=message.getTag().c_str();
	const char* s5=message2.getdata().c_str();
	const char* s6=message.getdata().c_str();
	CPPUNIT_ASSERT_MESSAGE("BSPMessage state",!strcmp(s3,s4)&&
			!strcmp(s1,s2) && !strcmp(s5,s6) && x==message2.getDstPartition() );
}

void TestBspmerge::testBspmessageintostring() {
	string a = "aaa";
	string b = "bbb";
	string c = "ccc";
	int x = 5;
	BSPMessage message(x, a, b, c);
	string result=message.intoString();
	const char* s1=result.c_str();
	    strstream ss;
	    string s;
	    ss << x;
	    ss >> s;
	string tmp=s+":"+a+":"+b+":"+c;
	const char* s2=tmp.c_str();
	CPPUNIT_ASSERT_MESSAGE("BSPMessage intostring",!strcmp(s1,s2));

}

void TestBspmerge::testBspmessagefromstring() {
	string a="0:1:2";
	BSPMessage message;
	message.fromString(a);
	string s1=message.getDstVertexID();
	string s2=message.getdata();
	strstream ss;
	int vertexid,data;
	ss<<s1;
	ss>>vertexid;
	ss.clear();
	ss<<s2;
	ss>>data;
	CPPUNIT_ASSERT_MESSAGE("BSPMessage intostring",1==vertexid && 2==data);

}

/*void TestBspmerge::testGraphdataaddforall() {
	GraphData* graphdata;
	graphdata = new GraphDataForMem();
	RecordParseDefault recordParse;
	for(int i=1;i<4;i++){
			string vertexid;
			strstream sss;
			sss<<i;
			sss>>vertexid;
			sss.clear();
			list<PREdge> edgeslist;
			for(int j=1;j<3;j++){
				string x;
				int y;
				y=i*10+j;
				strstream ss;
				ss<<y;
				ss>>x;
				ss.clear();
				cout<<"j:"<<j<<endl;
				PREdge e(x,x);
				edgeslist.push_back(e);
			}
			PREdge e1(vertexid,vertexid);
			PRVertex p(vertexid,vertexid,e1);
			Vertex* v=&p;
			graphdata->addForAll(v);
		}
	CPPUNIT_ASSERT_MESSAGE("Graphdataaddforall",3==graphdata->sizeForAll());
}

void TestBspmerge::testGraphdataget() {
	cout << "testGraphdataget" << endl;
	GraphData* graphdata = new GraphDataForMem();
	//graphdata = new GraphDataForMem();
	for (int i = 1; i < 4; i++) {
		string vertexid;
		strstream sss;
		sss << i;
		sss >> vertexid;
		//cout<<"vertexid11:"<<vertexid<<endl;
		sss.clear();
		PREdge e1(vertexid, vertexid);
		PRVertex p(vertexid, vertexid, e1);
		//cout<<"value:"<<p.getVertexValue()<<endl;
		Vertex* v = &p;
		graphdata->addForAll(v);
		graphdata->set(1, v, true);
	}
	Vertex* v = graphdata->get();
	//cout<<"end of get"<<endl;
	//cout<<"active number:"<<graphdata->getActiveCounter()<<endl;
	if (v == NULL) {
		cout << "null!" << endl;
	}
	CPPUNIT_ASSERT_MESSAGE("Graphdata get()", v!=NULL);
}

void TestBspmerge::testGraphdatasize(){

};

void TestBspmerge::testGraphdatasizeforall() {
	GraphData* graphdata;
	graphdata = new GraphDataForMem();
	for (int i = 1; i < 4; i++) {
		string vertexid;
		strstream sss;
		sss << i;
		sss >> vertexid;
		//cout<<"vertexid11:"<<vertexid<<endl;
		sss.clear();
		PREdge e1(vertexid, vertexid);
		PRVertex p(vertexid, vertexid, e1);
		//cout<<"value:"<<p.getVertexValue()<<endl;
		Vertex* v = &p;
		graphdata->addForAll(v);
		graphdata->set(1, v, true);
	}
	//cout<<"graph data size: "<< graphdata->sizeForAll()<<endl;
	int i = graphdata->sizeForAll();
	CPPUNIT_ASSERT_MESSAGE("Graphdata sizeforall()",3==i);
}

void TestBspmerge::testGraphdataclean() {
	GraphData* graphdata= new GraphDataForMem();
	//graphdata = new GraphDataForMem();
	for (int i = 1; i < 4; i++) {
		string vertexid;
		strstream sss;
		sss << i;
		sss >> vertexid;
		//cout<<"vertexid11:"<<vertexid<<endl;
		sss.clear();
		PREdge e1(vertexid, vertexid);
		PRVertex p(vertexid, vertexid, e1);
		//cout<<"value:"<<p.getVertexValue()<<endl;
		Vertex* v = &p;
		graphdata->addForAll(v);
		graphdata->set(1, v, true);
	}
	graphdata->clean();
	CPPUNIT_ASSERT_MESSAGE("Graphdata clean()", 0 == graphdata->sizeForAll());
}


void TestBspmerge::testGraphdatagetActiveCounter() {
	GraphData* graphdata;
	graphdata = new GraphDataForMem();
	for (int i = 1; i < 4; i++) {
		string vertexid;
		strstream sss;
		sss << i;
		sss >> vertexid;
		//cout<<"vertexid11:"<<vertexid<<endl;
		sss.clear();
		PREdge e1(vertexid, vertexid);
		PRVertex p(vertexid, vertexid, e1);
		//cout<<"value:"<<p.getVertexValue()<<endl;
		Vertex* v = &p;
		graphdata->addForAll(v);
		graphdata->set(1, v, true);
	}
	CPPUNIT_ASSERT_MESSAGE("Graphdata getActiveCounter()",
			3 == graphdata->getActiveCounter());
}


void TestBspmerge::testGraphdatagetEdgeSize() {
	GraphData* graphdata;
	graphdata = new GraphDataForMem();
	cout<<graphdata->getEdgeSize()<<endl;
	cout<<"vertex size song :"<<graphdata->sizeForAll()<<endl;
	for (int i = 1; i < 4; i++) {
		string vertexid;
		strstream sss;
		sss << i;
		sss >> vertexid;
		//cout<<"vertexid11:"<<vertexid<<endl;
		sss.clear();
		PREdge e1(vertexid, vertexid);
		PRVertex p(vertexid, vertexid, e1);
		cout <<"edge nums : "<<p.getEdgesNum()<<endl;
		//cout<<"value:"<<p.getVertexValue()<<endl;
		Vertex* v = &p;
		graphdata->addForAll(v);
		cout << "edge size111 : "<<graphdata->getEdgeSize()<<endl;
		graphdata->set(1, v, true);
	}
	cout << "edge size : "<<graphdata->getEdgeSize()<<endl;
	CPPUNIT_ASSERT_MESSAGE("Graphdata getEdgeSize()",
			3 == graphdata->getEdgeSize());
}

void TestBspmerge::testConstructor()
{

}*/

void TestBspmerge::testOptorAdd()
{

}

/*Test_Bspmerge::Test_Bspmerge() {
	// TODO Auto-generated constructor stub

}

Test_Bspmerge::~Test_Bspmerge() {
	// TODO Auto-generated destructor stub
}*/

} /* namespace BSPPipes */
