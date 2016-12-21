/*
 * testcppunit.cpp
 *
 *  Created on: Jul 21, 2014
 *      Author: root
 */


#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include "TestBspmerge.h"

void test(){
	CppUnit::TextUi::TestRunner runner;

		    /*从注册的TestSuite获取特定的TestSuite,
		没有参数的话则获取未命名的TestSuite*/
		    CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();

		    /*添加这个TestSuite到TestRunner中*/
		    runner.addTest(registry.makeTest());

		    /*运行测试*/
		    runner.run();
}


