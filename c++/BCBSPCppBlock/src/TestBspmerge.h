/*
 * TestBspmerge.h
 *
 *  Created on: Jul 21, 2014
 *      Author: root
 */

#ifndef TESTBSPMERGE_H_
#define TESTBSPMERGE_H_
#include "cppunit/extensions/HelperMacros.h"
#include <cppunit/extensions/HelperMacros.h>

#include <cppunit/Message.h>
#include <cppunit/Asserter.h>
#include <cppunit/SourceLine.h>
#include <cppunit/TestAssert.h>
#include <cppunit/extensions/TestNamer.h>

namespace BSPPipes {

class TestBspmerge: public CppUnit::TestFixture {

	/*声明一个TestSuite*/
	CPPUNIT_TEST_SUITE(TestBspmerge);
	CPPUNIT_TEST(testBspmessageConstructor);
	CPPUNIT_TEST(testBspmessageConstructor1);
	CPPUNIT_TEST(testBspmessageConstructor2);
	CPPUNIT_TEST(testBspmessageintostring);
	CPPUNIT_TEST(testBspmessagefromstring);
	//CPPUNIT_TEST(testGraphdataaddforall);
//	CPPUNIT_TEST(testGraphdataget);
	//CPPUNIT_TEST(testGraphdatasizeforall);
	//CPPUNIT_TEST(testGraphdataclean);
	//CPPUNIT_TEST(testGraphdatagetActiveCounter);
	//CPPUNIT_TEST(testGraphdatagetEdgeSize);
	CPPUNIT_TEST_SUITE_END();
public:

	void setUp();
	/*清除动作 */
	void tearDown();
	void testBspmessageConstructor();
	void testBspmessageConstructor1();
	void testBspmessageConstructor2();
	void testBspmessageintostring();
	void testBspmessagefromstring();
/*	void testGraphdataaddforall();
	void testGraphdataget();
	void testGraphdatasize();
	void testGraphdatasizeforall();
	void testGraphdataclean();
	void testGraphdatagetActiveCounter();
	void testGraphdatagetEdgeSize();
	void testConstructor();*/
	//void testOptorEqual();
	// void testOptorNotEqual();
	void testOptorAdd();
};

} /* namespace BSPPipes */
#endif /* TESTBSPMERGE_H_ */
