/*
 * Main.cc
 *
 *  Created on: Nov 29, 2013
 *      Author: root
 */

#include <iostream>
#include "Pipes.h"
#include "TemplateFactory.h"
#include "PREdge.h"
#include "PRVertex.h"
#include "Pagerank.cpp"
#include "PagerankAggregateContainer.h"
#include "RecordParseDefault.h"

#include "../kmeans/KMAggregateValue.h"
#include "../kmeans/KMAggregator.h"
#include "../kmeans/KMAggregateContainer.h"
#include "../kmeans/KMeansBSP.h"
#include "../kmeans/KMEdge.h"
#include "../kmeans/KMVertex.h"
#include "../kmeans/RecordParseKmeans.h"

#include <vector>
using namespace std;
using namespace BSPPipes;



int main(int argc, char *argv[]){
	//PRVertex p;
	//return BSPPipes::runTask(BSPPipes::TemplateFactory<KMVertex,KMEdge,KMeansBSP,KMAggregateContainer,RecordParseKmeans>());
	return BSPPipes::runTask(BSPPipes::TemplateFactory<PRVertex,PREdge,Pagerank,PagerankAggregateContainer,RecordParseDefault>());
	return 0;
};



