#ifndef BSP_PIPES_HH
#define BSP_PIPES_HH

#ifdef SWIG
%module (directors="1") BSPPipes
%include "std_string.i"
%feature("director") AggregateValue;
%feature("director") Aggregator;
%feature("director") BSP;
%feature("director") Edge;
%feature("director") Vertex;
%feature("director") Partitioner;
%feature("director") RecordParse;
%feature("director") Factory;
#else

#include <string>
#include <list>
#include <vector>
#include <stdint.h>
#include <map>
#include <iterator>
#include <algorithm>
#include <string.h>
// for test
#include <fstream>
#include <stdio.h>

using std::string;
using std::vector;
using std::iterator;

using namespace std;
#endif

namespace BSPPipes {

//

class BSPMessage {
private:
	const static long serialVersionUID = 1L;

private:
	int dstPartition;
	string dstVertexID;
	string tag;
	string data;

public:
	BSPMessage();

	BSPMessage(int _dstPartition, const string& _dstVertexID,
			const string& _tag, const string& _data);

	BSPMessage(string& _dstVertexID, string& _data);

	BSPMessage(int _dstPartition, const string& _dstVertexID,
			const string& _data);

	BSPMessage(const BSPMessage& msg);

	void setDstPartition(int partitionID);

	int getDstPartition() const;

	string getDstVertexID() const;

	string getTag() const;

	string getdata() const;

	string intoString();

	void fromString(string& msgData);

	~BSPMessage() {
	}
	;

};

class Edge {

public:
	virtual void setVertexID(const string& vertexID) = 0;
	virtual string getVertexID() const = 0;
	virtual void setEdgeValue(const string& edgeValue) = 0;
	virtual string getEdgeValue() const = 0;
	virtual string intoString() = 0;
	virtual void fromString(string& edgeData) = 0;
	virtual ~Edge() {
	}
	;

};

class Vertex {

public:
	Vertex() {
	}
	;
	virtual void setVertexID(const string& vertexID) = 0;
	virtual string getVertexID() const = 0;
	virtual void setVertexValue(const string& vertexValue) = 0;
	virtual string getVertexValue() const = 0;
	virtual void addEdge(const string& outgoingID, const string& edgeValue) = 0;
	virtual list<Edge*> getAllEdges() const = 0;
	virtual void removeEdge(const string& outgoingID,
			const string& edgeValue) = 0;
	virtual void updateEdge(const string& outgoingID,
			const string& edgeValue) = 0;
	virtual int getEdgesNum() const = 0;
	virtual string intoString() = 0;
	virtual void fromString(string& vertexData) = 0;
	virtual ~Vertex() {
	}
	;
};
class AggregationContext;
class SuperStepContext;

class AggregateValue {

public:
	AggregateValue() {
	};
	virtual string getValue() const = 0;
	virtual string toString() const = 0;
	virtual void setValue(string& v) = 0;
	virtual void initValue(string& s) =0;
	virtual void initValue(vector<BSPMessage> messages,
			AggregationContext agg) = 0;
	virtual void initbeforeSuperStep(SuperStepContext* context) = 0;
	virtual ~AggregateValue() {
	};

};

class AggregationContext {

private:

	int superstep;
	vector<Vertex*> vertex;
	vector<AggregateValue*> AggregateResult;
public:
	AggregationContext(Vertex* v, int superStep) {
		this->superstep = superStep;
		this->vertex.push_back(v);
	}

	void publishAggregateValues(vector<AggregateValue*> AggV) {
		vector<AggregateValue*>::iterator iteR = AggregateResult.begin();
		vector<AggregateValue*>::iterator iteV = AggV.begin();
		while (iteV != AggV.end()) {
			//(*iteR)=(*iteV);
			AggregateResult.push_back(*iteV);
			iteR++;
			iteV++;
		}

	}

	const vector<AggregateValue*> getAggregateResult() const {
		return AggregateResult;
	}

	int getSuperstep() const {
		return superstep;
	}

	const Vertex* getVertex() const {
		return vertex.back();
	}

	void clearVertex() {
		vertex.clear();
	}
};

//template <class T>
//class Aggregator{

//public:
//	Aggregator() {};
//virtual T aggregate(vector<T> aggValues) = 0;
//virtual ~Aggregator() {};

//};

class Aggregator {

public:
	Aggregator() {
	}
	;
	virtual AggregateValue* aggregate(vector<AggregateValue*> aggValues) = 0;

	virtual ~Aggregator() {
	}
	;

};

class AggregatorContainer {
public:
	AggregatorContainer() {
	};
	virtual void loadAggregators()=0;
	virtual void loadAggregateValues()=0;
	virtual vector<Aggregator*> getAggregators() =0;
	virtual vector<AggregateValue*> getAggregateValues()=0;
	virtual vector<AggregateValue*> getCurrentAggregateValues()=0;
	virtual vector<AggregateValue*> getAggregateResults() =0;
	virtual string getTest()=0;//FOR TEST
	virtual ~AggregatorContainer() {
	};
};

class AggregatorOperator {
private:
public:
	vector<AggregatorContainer*> AggC;
	AggregatorOperator(AggregatorContainer* Aggc) {
		AggC.push_back(Aggc);
		vector<AggregatorContainer*>::iterator p = AggC.begin();
		(*p)->loadAggregators();
		(*p)->loadAggregateValues();
	}
	AggregatorContainer* getAggC() {
		return AggC.back();
	}

	void initBeforeSuperstep(SuperStepContext* sscontext) {
		vector<AggregateValue*> AggR=getAggC()->getCurrentAggregateValues();
		vector<AggregateValue*>::iterator iteCV =AggR.begin();
		vector<AggregateValue*>::iterator itettt=getAggC()->getCurrentAggregateValues().begin();
		AggregateValue* Aggv = (*iteCV);
		while (iteCV != (AggR.end())) {
			(*iteCV)->initbeforeSuperStep(sscontext);
			iteCV++;
		}		//while
	}

	void aggregate(vector<BSPMessage> messages, Vertex* vertex, int superstep) {
		vector<Aggregator*> Aggt = getAggC()->getAggregators();
		vector<Aggregator*>::iterator ite = Aggt.begin();
		vector<AggregateValue*> aggV = getAggC()->getAggregateValues();
		vector<AggregateValue*>::iterator iteV = aggV.begin();
		vector<AggregateValue*> aggCV = getAggC()->getCurrentAggregateValues();
		vector<AggregateValue*>::iterator iteCV = aggCV.begin();
		while (ite != Aggt.end()) {
			vector<AggregateValue*> aggValues;
			AggregationContext aggContext(vertex, superstep);
			aggContext.publishAggregateValues(this->getAggC()->getAggregateResults());
			(*iteCV)->initValue(messages, aggContext);
			aggValues.push_back((*iteV));
			aggValues.push_back((*iteCV));
			if(!strcmp((*iteV)->getValue().c_str(),"")){
				string tmp=(*iteCV)->getValue();
				(*iteV)->setValue(tmp);
			}else{
			(*ite)->aggregate(aggValues);
			}
			aggContext.clearVertex();
			ite++;
			iteCV++;
			iteV++;
		}		//end of while
	}

	vector<string> encapsulateAggregateValues() {
		int aggSize = this->getAggC()->getAggregateValues().size();
		vector<string> aggValues(aggSize);
		vector<AggregateValue*>::iterator iteV =
				this->getAggC()->getAggregateValues().begin();
		while (iteV != this->getAggC()->getAggregateValues().end()) {
			aggValues.push_back((*iteV)->toString());
			iteV++;
		}
		return aggValues;

	}
	void decapsulateAggregateValues(vector<string> aggValues) {
		vector<string>::iterator iteS = aggValues.begin();
		vector<AggregateValue*>::iterator iteR =
				this->getAggC()->getAggregateResults().begin();
		while (iteS != aggValues.end()) {
			(*iteR)->setValue((*iteS));
			iteS++;
			iteR++;
		}
	}

};

class GraphData {

public:
	virtual void addForAll(Vertex* vertex) = 0;
	//??Because every staff owns a graph data, a graph data needs some properties of the staff.
	virtual void initialize() = 0;
	//Get the size of the graph data.
	virtual int size() = 0;
	//Get the Vertex for a given index.
	virtual Vertex* get() = 0;
	virtual void set(int index, Vertex* vertex, bool activeFlag) = 0;
	virtual int sizeForAll() = 0;
	virtual Vertex* getForAll(int index) = 0;
	virtual bool getActiveFlagForAll(int index) = 0;
	virtual void finishAdd() = 0;
	virtual void clean() = 0;
	virtual long getActiveCounter() = 0;
	virtual void showMemoryInfo() = 0;
	virtual int getEdgeSize() = 0;
};

class Partitoner {

};

class RecordParse {
public:
	RecordParse() {
	}
	;
	virtual Vertex* recordParse(string& key, string& value) const = 0;
	virtual string getVertexID(string& key) const = 0;
	virtual ~RecordParse() {
	}
	;

};

class SuperStepContext {
private:
	int superstep;
	vector<AggregateValue*> aggregateValues;
public:
	SuperStepContext(int superStep, vector<AggregateValue*> AggV) {
		this->superstep = superStep;
		vector<AggregateValue*>::iterator iteR = aggregateValues.begin();
		vector<AggregateValue*>::iterator iteV = AggV.begin();
		while (iteV != AggV.end()) {
			(*iteR) = (*iteV);
			iteR++;
			iteV++;
		}
	}

	SuperStepContext(int superStep) {
		superstep = superStep;
	}

	void publishAggregateValues(vector<AggregateValue*> AggV) {
		if (!AggV.empty()) {
			vector<AggregateValue*>::iterator iteR = aggregateValues.begin();
			vector<AggregateValue*>::iterator iteV = AggV.begin();
			while (iteV != AggV.end()) {
				if (NULL == (*iteV)) {
				} else {
					aggregateValues.push_back((*iteV));

				}
				iteR++;
				iteV++;
			}
		} else {
			aggregateValues.clear();
		}
	}

	const vector<AggregateValue*> getAggregateValues() const {
		return aggregateValues;
	}

	int getSuperstep() const {
		return superstep;
	}

};

class BSPStaffContextInterface {

public:
	Vertex* v;
	virtual Vertex* getVertex() const = 0;
	virtual void updateVertex(Vertex* vertex) = 0;
	virtual int getOutgoingEdgesNum() const = 0;
	virtual list<Edge*> getOutgoingEdges() const = 0;
	virtual bool removeEdge(const string& vertexID,
			const string& edgeValue) = 0;
	virtual bool updateEdge(const string& vertexID,
			const string& edgeValue) = 0;
	virtual int getCurrentSuperStepCounter() const = 0;
	virtual void sendMessage(BSPMessage msg) = 0;
	virtual AggregateValue* getAggregateValue(string& name) const = 0;
	virtual void voltToHalt() = 0;
	virtual ~BSPStaffContextInterface() {
	}
	;
};

class BSPStaffContext: public BSPPipes::BSPStaffContextInterface {

private:
	Vertex* vertex;
	list<Edge*> outgoingEdgesClone;
	int currentSuperStepCounter;
	vector<BSPMessage> messagesCache;
	map<string, AggregateValue*> aggregateValues;
	bool activeFlag;

public:
	BSPStaffContext(Vertex* aVertex, int _currentSuperStepCounter) {
		vertex = aVertex;
		outgoingEdgesClone = vertex->getAllEdges();
		currentSuperStepCounter = _currentSuperStepCounter;
		activeFlag = true;
	}

	//Override
	Vertex* getVertex() const {
		return vertex;
	}

	//Override
	void updateVertex(Vertex* _vertex) {
		vertex = _vertex;
	}

	//Override
	int getOutgoingEdgesNum() const {
		return vertex->getEdgesNum();
	}

	list<Edge*> getOutgoingEdges() const {
		return vertex->getAllEdges();
	}

	//Override
	bool removeEdge(const string& vertexID, const string& edgeValue) {
		vertex->removeEdge(vertexID, edgeValue);
		return true;
	}

	//Override
	bool updateEdge(const string& vertexID, const string& edgeValue) {
		vertex->updateEdge(vertexID, edgeValue);
		return true;
	}

	//Override
	int getCurrentSuperStepCounter() const {
		return currentSuperStepCounter;
	}

	//Override
	void sendMessage(BSPMessage msg) {
		messagesCache.push_back(msg);
	}

	vector<BSPMessage> getMessagesCache() const {
		return messagesCache;
	}

	//Override
	void addAggregateValues(string& key, AggregateValue* value) {
		aggregateValues.insert(make_pair(key, value));
	}

	map<string, AggregateValue*> getAggregateValues() const {
		return aggregateValues;
	}

	//Override
	AggregateValue* getAggregateValue(string& key) const {
		map<string, AggregateValue*>::const_iterator iter =
				aggregateValues.find(key);
		if (iter != aggregateValues.end()) {
			return iter->second;
		} else {
			return NULL;
		}
	}

	//Override
	void voltToHalt() {
		activeFlag = false;
	}

	bool getActiveFLag() const {
		return activeFlag;
	}

};

class BSP {
public:
	//vector<BSPMessage> bsp;
	//virtual void compute() =0;
	virtual void compute(vector<BSPMessage> BspMessage,
			BSPStaffContext* context) =0;
	virtual void initBeforeSuperStep(SuperStepContext context) = 0;
	//virtual void setid(int i)=0;
	//virtual int getid()=0;
	virtual ~BSP() {
	}
	;

};

/**
 * User code to decide where each key should be sent.
 */
class Partitioner {
public:
	virtual int partition(const std::string& key, int numOfStaffs) = 0;
	virtual ~Partitioner() {
	}
};

/**
 * A factory to create the necessary application objects.
 */
class Factory {
public:
	/**
	 * Create an application partitioner object.
	 * @return the new partitioner or NULL, if the default partitioner should be
	 *     used.
	 */
	virtual BSP* CreatBsp() const {

	}
	virtual Vertex* CreatVertex() const {

	}
	virtual Edge* CreatEdge() const {

	}
	virtual AggregatorContainer* CreateAggC() const {

	}
	virtual Partitioner* createPartitioner(int numPartition) const {
		return NULL;
	}
	virtual RecordParse* creatRecordParse() const{

	}
	virtual ~Factory() {
	}
};

template<class b, class v, class e>
class TemaplateFactory: Factory {
public:
	TemaplateFactory() {
	}
	;
	BSP* CreatBsp() const {
		return new b();
	}
	Vertex* CreatVertex() const {
		return new v();
	}
	Edge* CreatEdge() const {
		return new e();
	}

	virtual ~TemaplateFactory() {
	}
	;
};

/**
 * Run the assigned task in the framework.
 * The user's main function should set the various functions using the
 * set* functions above and then call this.
 * @return true, if the task succeeded.
 */
bool runTask(const Factory& factory);

}

#endif
