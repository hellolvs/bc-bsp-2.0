#include "Pipes.h"
#include "SerialUtils.h"
#include "StringUtils.h"

#include <map>
#include <vector>
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <pthread.h>
#include <fstream>
#include "RecordParseDefault.h"

#include "StringUtil.h"
#include "Pagerank.cpp"

using std::map;
using std::string;
using std::vector;

using namespace std;
using namespace BspUtils;

namespace BSPPipes {

//Constants
const string SPLIT_FLAG = ":";
const string KV_SPLIT_FLAG = "\t";
const string SPACE_SPLIT_FLAG = " ";
pthread_mutex_t messageDone = PTHREAD_MUTEX_INITIALIZER;/*初始化互斥锁*/
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;/*初始化条件变量*/
ofstream command;
ofstream superStepRecord;
ofstream testforlog;
bool isReceived;
bool doneASuperStep;
string ClogDir;
char* ptype;
//list<string> messages;//for receive message


class GraphDataFactory {

};

class GraphDataInterface {
public:
	virtual ~GraphDataInterface() {

	}
};

class GraphDataForMem: public BSPPipes::GraphData {

private:
	vector<Vertex*> vertexList;
	vector<bool> activeFlages;
	int totalVerticesNum;
	int totalEdgesNum;
	int globalIndex;

public:
	void initialize() {
	}

	void addForAll(Vertex* vertex) {
		vertexList.push_back(vertex);
		activeFlages.push_back(true);
		totalEdgesNum = totalEdgesNum + vertex->getEdgesNum();

//		cout << "---" << endl;
//		vector<Vertex*>::iterator ite = vertexList.begin();
//		while(ite != vertexList.end()) {
//			cout << "vertex  " << (*ite)->intoString() << endl;
//			++ ite;
//		}

	}

	Vertex* get() {
		Vertex* vertex = NULL;
		for (int globalIndex = 0; globalIndex < totalVerticesNum;
				globalIndex++) {
			if (activeFlages.at(globalIndex)) {
				vertex = vertexList.at(globalIndex);
			}
		}

		return vertex;
	}

	Vertex* getForAll(int index) {
		return vertexList.at(index);
	}

	void set(int index, Vertex* vertex, bool activeFlag) {
		vertexList[index] = vertex;
		//activeFlages[index] = activeFlages;
		globalIndex++;
		if (globalIndex >= totalVerticesNum) {
			reserGlobalIndex();
		}
	}

	int size() {
		updataTotalVertices();
		return (int) getActiveCounter();
	}

	int sizeForAll() {
		updataTotalVertices();
		return totalVerticesNum;
	}

	void clean() {
		vertexList.clear();
		activeFlages.clear();
		totalVerticesNum = 0;
		updataTotalVertices();
		reserGlobalIndex();
	}

	void finishAdd() {
		// For memory-only version, do nothing.
	}

	long getActiveCounter() {
		long counter = 0;

		for (int i = 0; i < activeFlages.size(); i++) {
			if (activeFlages.at(i)) {
				counter++;
			}
		}

		return counter;
	}

private:
	void reserGlobalIndex() {
		globalIndex = 0;
	}

	void updataTotalVertices() {
		totalVerticesNum = vertexList.size();
	}

	void vectorSet(int index) {
	}

public:
	bool getActiveFlagForAll(int index) {
		return activeFlages.at(index);
	}

	void showMemoryInfo() {
	}

	int getEdgeSize() {
		return totalEdgesNum;
	}
	~GraphDataForMem() {

	}

};


//class BspPipes::BSPMessage
//没有初始化分区号，以后存在隐患，而且这个构造函数并不实用

BSPMessage::BSPMessage() {
	dstVertexID = "";
	tag = "";
	data = "";
}

BSPMessage::BSPMessage(int _dstPartition, const string& _dstVertexID,
		const string& _tag, const string& _data) {
	dstPartition = _dstPartition;
	dstVertexID = _dstVertexID;
	tag = _tag;
	data = _data;
}

//没有初始化分区号，以后存在隐患，而且这个构造函数并不实用
BSPMessage::BSPMessage(string& _dstVertexID, string& _data) {
	dstPartition=0;
	dstVertexID = _dstVertexID;
	data = _data;
	tag = "";
}

BSPMessage::BSPMessage(int _dstPartition, const string& _dstVertexID,
		const string& _data) {
	dstPartition = _dstPartition;
	dstVertexID = _dstVertexID;
	data = _data;
	tag = "";
}

BSPMessage::BSPMessage(const BSPMessage& msg) {
	dstPartition = msg.getDstPartition();
	dstVertexID = msg.getDstVertexID();
	tag = msg.getTag();
	data = msg.getdata();
}

void BSPMessage::setDstPartition(int partitionID) {
	dstPartition = partitionID;
}

int BSPMessage::getDstPartition() const {
	return dstPartition;
}

string BSPMessage::getDstVertexID() const {
	return dstVertexID;
}

string BSPMessage::getTag() const {
	return tag;
}

string BSPMessage::getdata() const {
	return data;
}

string BSPMessage::intoString() {
	string dstPar = StringUtil::int2str(dstPartition);
	string result;
	if (tag == "") {
		result = dstPar + SPLIT_FLAG + dstVertexID + SPLIT_FLAG + data;
	} else {
		result = dstPar + SPLIT_FLAG + dstVertexID + SPLIT_FLAG + tag
				+ SPLIT_FLAG + data;
	}
	//superStepRecord<<"string bspMessage is	"<<result<<endl;
	return result;
}

void BSPMessage::fromString(string& msgData) {
	//superStepRecord<<"----in fromString-----"<<endl;
	string pattern = SPLIT_FLAG;
	vector<string> vec = StringUtil::stringSplit(msgData, pattern);
	//superStepRecord<<"vecSize:	"<<vec.size()<<endl;
	//dstPartition = StringUtil::str2int(vec[0]);
	dstVertexID = vec[1];
	//tag = vec[1];
	data = vec[2];
	//superStepRecord<<"verID:"<<dstVertexID<<"data:"<<data<<endl;
}


class DownwardProtocol {
public:
	virtual void start(int protocol) = 0;
	//virtual void runASuperStep() = 0;
	//virtual void startASuperStep() = 0;
	virtual void saveMessage(vector<string> messages) = 0;
	virtual void sendMessage(string message) = 0;
	virtual void sendKeyValue(string key, string value) = 0;
	/**
	 * Send vetex key , staff num and get partition Id
	 */
	virtual void sendKey(string key, int num) = 0;
	virtual void sendNewAggregateValue(string aggregateValue) = 0;
	virtual void addNewAggregateValues(vector<string> aggV)=0;
	virtual void saveResult() = 0;
	//virtual void saveResult(GraphData* graph) = 0;
	virtual void endOfInput() = 0;
	// virtual void close() = 0;
	virtual void abort() = 0;
	virtual void messageReceivedOver() = 0;
	virtual void cleanMessage()=0;
	virtual void saveStaffId(string sid) = 0;
	virtual ~DownwardProtocol() {
	}
};

class UpwardProtocol {
public:
	//send new message to java
	virtual void sendNewMessage(vector<BSPMessage> msg) = 0;
	virtual void sendMessage(vector<string> msg) = 0;
	//send new aggregate value to java
	virtual void setAggregateValue(vector<string> newAggregateValue) = 0;
	virtual void currentSuperStepDone() = 0;
	virtual void getMessages(string vertexId) = 0;
	//virtual void saveResult(GraphDataInterface &graphData) = 0;
	virtual void saveResult(GraphData* graphData) = 0;
	virtual ~UpwardProtocol() {
	}
};

class Protocol {
public:
	virtual void nextEvent() = 0;
	virtual UpwardProtocol* getUplink() = 0;
	virtual ~Protocol() {
	}
};

enum MESSAGE_TYPE {
	COMMAND_START = 0,
	FLAG_GRAPHDATA = 1,
	FLAG_MESSAGE = 2,
	COMMAND_MESSAGE = 3,
	COMMAND_AGGREGATE_VALUE = 4,
	FLAG_AGGREGATE_VALUE = 5,
	COMMAND_RUN_SUPERSTEP = 6,
	COMMAND_PARTITION = 7,
	FLAG_PARTITION = 8,
	COMMAND_SAVE_RESULT = 9,
	STAFFID=50,
	COMMAND_ABORT = 51,
	COMMAND_DONE = 52,
	COMMAND_CLOSE = 53
};

class BinaryUpwardProtocol: public UpwardProtocol {
private:
	FileOutStream* stream;
public:
	BinaryUpwardProtocol(FILE* _stream) {
		stream = new FileOutStream();
		BSP_ASSERT(stream->open(_stream), "problem opening stream");
	}

	virtual void sendNewMessage(vector<BSPMessage> msg) {
		serializeInt(FLAG_MESSAGE, *stream);
		serializeInt(msg.size(), *stream);
		for (vector<BSPMessage>::iterator it = msg.begin(); it != msg.end();
				++it) {
			string temp = it->intoString();
			serializeString(temp, *stream);
		}
		stream->flush();
	}
	virtual void sendMessage(vector<string> msg) {
		superStepRecord<<"sendMessage num  "<<msg.size()<<endl;
		serializeInt(FLAG_MESSAGE, *stream);
		serializeInt(msg.size(), *stream);
		for (vector<string>::iterator it = msg.begin(); it != msg.end(); ++it) {
			serializeString(*it, *stream);
		}
		stream->flush();
	}
	virtual void getMessages(string vertexId) {
		serializeInt(COMMAND_MESSAGE, *stream);
		serializeString(vertexId, *stream);
		stream->flush();
	}
	virtual void setAggregateValue(vector<string> newAggregateValue) {
		serializeInt(FLAG_AGGREGATE_VALUE, *stream);
		serializeInt(newAggregateValue.size(), *stream);
		for (vector<string>::iterator it = newAggregateValue.begin();
				it != newAggregateValue.end(); ++it) {
			serializeString(*it, *stream);
		}
		stream->flush();
	}
	virtual void currentSuperStepDone() {
		serializeInt(COMMAND_DONE, *stream);
		stream->flush();
	}

	virtual void saveResult() {
		serializeInt(FLAG_GRAPHDATA, *stream);
		stream->flush();
	}
	virtual void saveResult(GraphData* graphData) {
				serializeInt(FLAG_GRAPHDATA, *stream);
				stream->flush();
				serializeInt(graphData->size(), *stream);
				command<<"FLAG_GRAPHDATA1"<<endl;
				for (int i = 0; i < graphData->size(); i++) {
					string vertex;
					string edge;
					string vertexEdge;

					vertex = graphData->getForAll(i)->getVertexID() + SPLIT_FLAG
							+ graphData->getForAll(i)->getVertexValue();

					list<Edge*> edges = graphData->getForAll(i)->getAllEdges();
					list<Edge*>::iterator it;
					for (it = edges.begin(); it != edges.end(); it++) {
						edge = edge + (*it)->intoString() + SPACE_SPLIT_FLAG;
					}
					vertexEdge = vertex + SPACE_SPLIT_FLAG + edge;
					serializeString(vertexEdge, *stream);
					stream->flush();
				}
				command<<" after FLAG_GRAPHDATA1"<<endl;
			}
	~BinaryUpwardProtocol() {
		delete stream;
	}
};

class BinaryProtocol: public Protocol {
private:
	FileInStream* downStream;
	DownwardProtocol* handler;
	BinaryUpwardProtocol * uplink;
	string key;
	string value;

public:
	BinaryProtocol(FILE* down, DownwardProtocol* _handler, FILE* up) {
		downStream = new FileInStream();
		downStream->open(down);
		uplink = new BinaryUpwardProtocol(up);
		handler = _handler;
	}

	UpwardProtocol* getUplink() {
		return uplink;
	}
	virtual void nextEvent() {
		int32_t cmd;
		command << "before cmd" << endl;
		cmd = deserializeInt(*downStream);
		command << "after cmd and cmd is " << cmd << endl;
		switch (cmd) {
		case COMMAND_START: {
			command << "COMMAND_START" << endl;
			command.flush();
			int32_t prot;
			prot = deserializeInt(*downStream);
			handler->start(prot);
			break;
		}
		case FLAG_GRAPHDATA: {
			command << "FLAG_GRAPHDATA" << endl;
			string key;
			string value;
			deserializeString(key, *downStream);
			deserializeString(value, *downStream);
			handler->sendKeyValue(key, value);
			break;
		}
		case FLAG_MESSAGE: {
			command << "FLAG_MESSAGE" << endl;
			int32_t size;
			size = deserializeInt(*downStream);
			command << "size is " << size << endl;
			command.flush();
			string message;
			for (int i = 0; i < size; i++) {
				deserializeString(message, *downStream);
				handler->sendMessage(message);
			}
			command << "received over" << endl;
			handler->messageReceivedOver();
			command << "after handler->messageReceivedOver();" << endl;
			command.flush();
			break;
		}
		case COMMAND_PARTITION: {
			command << "COMMAND_PARTITION" << endl;
			string key;
			int32_t num;
			deserializeString(key, *downStream);
			num = deserializeInt(*downStream);
			handler->sendKey(key, num);
			break;
		}
		case FLAG_AGGREGATE_VALUE: {
			command << "FLAG_AGGREGATE_VALUE" << endl;
			int32_t size;
			size = deserializeInt(*downStream);
			string aggregateValue;
			vector<string>* aggV=new vector<string>;
			for (int i = 0; i < size; i++) {
				deserializeString(aggregateValue, *downStream);
				command<<"get aggregate Value :"<<aggregateValue<<endl;
				aggV->push_back(aggregateValue);
				handler->sendNewAggregateValue(aggregateValue);
			}
			handler->addNewAggregateValues(*aggV);
			break;
		}
		case COMMAND_RUN_SUPERSTEP: {
			command << "COMMAND_RUN_SUPERSTEP" << endl;
			command << "in superstep" << endl;
			doneASuperStep = true;
			//handler->startASuperStep();
			command << "start is over" << endl;
			command.flush();
			break;
		}
		case COMMAND_SAVE_RESULT: {
			command << "COMMAND_SAVE_RESULT" << endl;
			command.flush();
			handler->saveResult();
			break;
		}
		case COMMAND_CLOSE:{
			command << "COMMAND_CLOSE" << endl;
			command.flush();
			handler->endOfInput();
			break;
		}
		case COMMAND_ABORT:{
			command << "COMMAND_ABORT" << endl;
			command.flush();
			handler->abort();
			break;
		}
		case STAFFID:{
			command<<"COMMAND_STAFFID"<<endl;
			command.flush();
			string staffid;
			deserializeString(staffid, *downStream);
			handler->saveStaffId(staffid);
			break;
		}
		default:
			command << "Unknown binary command " + toString(cmd) << endl;
			command.flush();
			//BSP_ASSERT(false, "Unknown binary command " + toString(cmd))
			;
		}
	}

	virtual ~BinaryProtocol() {
		delete downStream;
		delete uplink;
		command.close();
	}
};

class TaskContextImpl: public DownwardProtocol {
private:
	bool done;
	bool isNewMessage; // to determine the message is new from java
	bool isNewAggregateValue; // to determine the aggregate value is new from java
	bool isGraphDataLoaded; // to determine the graph data is loaded over from java
	Protocol* protocol;
	UpwardProtocol *uplink;
	Partitioner* partitioner;
	const Factory* factory;
	pthread_mutex_t mutexDone;
	GraphData* graphData;
	BSP* bsp;
	list<BSPMessage> messagesCache;
	vector<string> outgoingMessage;
	BSPStaffContext* staffContext;
	vector<BSPMessage> BSPMessages;
	vector<string> messages;
	int currentSuperStep;
	string staffId;
	vector<vector<string> > aggregateValues;
	RecordParse* recordParse;
	//pthread_mutex_t messageDone;
	//pthread_cond_t cond;
//	pthread_t runASuperStepThread;

public:

	TaskContextImpl(const Factory& _factory) {
		uplink = NULL;
		done = false;
		factory = &_factory;
		partitioner = NULL;
		protocol = NULL;
		isNewMessage = false;
		isNewAggregateValue = false;
		isGraphDataLoaded = false;
		isReceived = false;
		staffContext = NULL;
		graphData = new GraphDataForMem();
		bsp = NULL;
		currentSuperStep = 0;
		pthread_mutex_init(&mutexDone, NULL);

		//pthread_mutex_init(&mutexDone, NULL);
		//messageDone = PTHREAD_MUTEX_INITIALIZER;/*初始化互斥锁*/;
		//cond = PTHREAD_COND_INITIALIZER;/*初始化条件变量*/

		//command.open("/root/command.txt", ios::app);
		//command << "new TaskContextImpl over" << endl;
		//command.flush();

	}

	const Factory* getFactory() const {
		return factory;
	}

	void setProtocol(Protocol* _protocol, UpwardProtocol* _uplink) {

		protocol = _protocol;
		uplink = _uplink;
	}

	virtual void start(int protocol) {
		if (protocol != 0) {
			throw Error(
					"Protocol version " + toString(protocol)
							+ " not supported");
		}
		command << "in start method" << endl;
		command.flush();
	}

	virtual void sendKey(string key, int num) {

		command << "in sendKey method   " << endl;
	}

	virtual int getCurrentSuperStep() {
		return currentSuperStep;
	}
	virtual void setCurrentSuperStep(int superStep){
		currentSuperStep=superStep;
	}
	virtual void saveMessage(vector<string> message) {
		messages = message;
		isReceived = true;
		command << "thread 2 lock " << __LINE__ << endl;
		pthread_mutex_lock(&messageDone);/*锁住互斥量*/
		command << "thread 2 notify " << __LINE__ << endl;
		pthread_cond_signal(&cond);/*条件改变，发送信号，通知t_b进程*/
		pthread_mutex_unlock(&messageDone);
		command << "thread 2 unlock " << __LINE__ << endl;

	}
	//save message from java after request certain message use vertexid;
	virtual void sendMessage(string message) {
		superStepRecord<<"get a"<<message<<endl;
		string s=message;
		BSPMessage bspMsg;
		superStepRecord<<"in sendMessage"<<endl;
		superStepRecord<<"message"<<endl;
		superStepRecord<<"string into BSPMessage"<<s<<endl;
		bspMsg.fromString(s);
		BSPMessages.push_back(bspMsg);
		superStepRecord<<"BSPMessages Size is	"<<BSPMessages.size()<<endl;
		for (int k = 0; k < BSPMessages.size(); k++) {
			superStepRecord << "[sendMessage] vertexID	"
					<< BSPMessages[k].getDstVertexID() << endl;
			superStepRecord << "[sendMessage] Value	" << BSPMessages[k].getdata()
					<< endl;
		}
		messages.push_back(s);
		superStepRecord<<"messages Size	"<<messages.size()<<endl;
	}

	virtual vector<string> *getMessages(string vertexId) {
		superStepRecord<<"[getMessages]	 get vertex"<<vertexId<<"from java"<<endl;
		uplink->getMessages(vertexId);
		if (isReceived) {
			isReceived = false;
			return &messages;
		} else {
			command << "thread 1 lock " << __LINE__ << endl;
			pthread_mutex_lock(&messageDone);
			command << "thread 1 wait " << __LINE__ << endl;
			pthread_cond_wait(&cond, &messageDone);/*解锁mutex，并等待cond改变*/
			command << "message have receive over" << endl;
			pthread_mutex_unlock(&messageDone);
			command << "thread 1 unlock " << __LINE__ << endl;
			command.flush();
			command<<"[getMessages2]  message num"<<messages.size()<<endl;

			/*for (int i = 0; i < messages.size(); i++) {
				command << "received2 message	" << messages[i] << endl;
			}*/

			/*
			list<string>::iterator ite = messages.begin();
			while (ite != messages.end()) {
				superStepRecord << "received2 message	:" << (*ite)
						<< endl;
				ite++;
			}
			*/
			isReceived=false;
			return &messages;
		}

		//return messages;
		//while(!isNewMessage);
	}
	virtual void cleanMessage(){
		messages.clear();
	}

	virtual void messageReceivedOver() {
		isReceived = true;
		command << "thread 2 lock " << __LINE__ << endl;
		pthread_mutex_lock(&messageDone);/*锁住互斥量*/
		command << "thread 2 notify " << __LINE__ << endl;
		pthread_cond_signal(&cond);/*条件改变，发送信号，通知t_b进程*/
		pthread_mutex_unlock(&messageDone);
		command << "thread 2 unlock " << __LINE__ << endl;
	}
	virtual void sendKeyValue(string key, string value) {
		if(recordParse==NULL){
			recordParse=factory->creatRecordParse();
		}
		//Vertex* vertex = new PRVertex();
		Vertex* vertex;
		RecordParseDefault recordPars;
		vertex = recordParse->recordParse(key, value);
		//vertex = recordPars.recordParse(key, value);
		graphData->addForAll(vertex);
		command << "graphData.size::" << graphData->size() << "---------"
				<< endl;

	}



	virtual void sendNewAggregateValue(string aggregateValue) {
		command<<"in sendNewAggregateValue method   "<<endl;
	}
	virtual void addNewAggregateValues(vector<string> aggV){
		command<<"in addNewAggregateValues"<<endl;
		aggregateValues.push_back(aggV);
	}

	virtual UpwardProtocol* getUpLink() {
		return uplink;
	}

	virtual void saveResult() {
		command<<" textimpl saveResult"<<endl;
		this->uplink->saveResult(graphData);
		allDone();
	}

	virtual void endOfInput() {
		isGraphDataLoaded = true;
	}

	virtual void allDone() {
		pthread_mutex_lock(&mutexDone);
		done = true;
		pthread_mutex_unlock(&mutexDone);
	}

	virtual bool isDone() {
		pthread_mutex_lock(&mutexDone);
		bool doneCopy = done;
		pthread_mutex_unlock(&mutexDone);
		return doneCopy;
	}

	virtual void abort() {
		pthread_mutex_lock(&mutexDone);
		done = true;
		pthread_mutex_unlock(&mutexDone);
	}

	void waitForTask() {
		while (!done) {
			command << "nextEvent.." << endl;
			command.flush();
			protocol->nextEvent();
		}
	}

	virtual ~TaskContextImpl() {
		delete partitioner;
		pthread_mutex_destroy(&mutexDone);
	}
	bool isLoadOver() {
		if (isGraphDataLoaded == true)
			return true;
		return false;
	}

	GraphData* getGraphData() const {
		return graphData;
	}
	vector<BSPMessage> getBSPMessages(){
		return BSPMessages;
	}
	void cleanBSPMessages(){
		BSPMessages.clear();
	}
	void initBeforeSuperStep(int superStep){

	};
	void aggregate(vector<BSPMessage> messages,Vertex* vertex,int superStep){

	}
	void saveStaffId(string sid){
		this->staffId=sid;
		command<<"stafffid is: "+sid<<endl;
		command.flush();
	}
	vector<vector<string> > getAggregateValues(){
		return this->aggregateValues;
	}
	void cleanAggregateValues(){
		this->aggregateValues.clear();
	}
};
/*
 * local compute thread
 *
 */
void * runASuperStep(void *ptr) {
	vector<BSPMessage> outgoingqueue;
	TaskContextImpl* tci_pt = (TaskContextImpl*) ptr;
	const Factory* ft = tci_pt->getFactory();
	UpwardProtocol *uplink;
	if (strcmp(ptype, "WorkerManager") == 0) {
		//for workermanager aggregate
		while (!tci_pt->isDone()) {
			if (doneASuperStep) {
				command << "in runAsuperStep ptype is workermanager" << endl;
				uplink = tci_pt->getUpLink();
				AggregatorContainer* ACP = ft->CreateAggC();
				ACP->loadAggregateValues();
				ACP->loadAggregators();
				vector<Aggregator*> Aggregators = ACP->getAggregators();
				vector<AggregateValue*> AggValues = ACP->getAggregateValues();
				vector<AggregateValue*> AggCurrentValues =
						ACP->getCurrentAggregateValues();
				vector<vector<string> > aggregateValues =
						tci_pt->getAggregateValues();
				vector<vector<string> >::iterator ite = aggregateValues.begin();
				vector<Aggregator*>::iterator iteor = Aggregators.begin();
				vector<AggregateValue*>::iterator iteV = AggValues.begin();
				vector<AggregateValue*>::iterator iteCV =
						AggCurrentValues.begin();
				int i = 0;
				while (iteor != Aggregators.end()) {
					(*iteV)->initValue((*ite).at(i));
					vector<AggregateValue*> aggVs;
					aggVs.push_back((*iteV));
					aggVs.push_back((*iteCV));
					ite++;
					while (ite != aggregateValues.end()) {
						(*iteCV)->initValue((*ite).at(i));
						(*iteor)->aggregate(aggVs);
						ite++;

					}
					i++;
					iteCV++;
					iteV++;
					iteor++;
				}
				vector<AggregateValue*>::iterator iteV1 = AggValues.begin();
				while (iteV1 != AggValues.end()) {
					command << "aggregate result is: " << (*iteV1)->getValue()
							<< endl;
					iteV1++;
				}
				vector<string> aggVs;
				vector<AggregateValue*> AggTemp = ACP->getAggregateValues();
				vector<AggregateValue*>::iterator iteAVT = AggTemp.begin();
				while (iteAVT != AggTemp.end()) {
					aggVs.push_back((*iteAVT)->getValue());
					iteAVT++;
				}
				uplink->setAggregateValue(aggVs);
				tci_pt->cleanAggregateValues();
				uplink->currentSuperStepDone();
				command << "current super step done" << endl;
				command.flush();
				doneASuperStep = false;
			} else {
				sleep(1);
			}

		}
	}
	else{
		//local compute
	GraphDataForMem graph;
	while (!tci_pt->isDone()) {
		if (doneASuperStep) {
			uplink = tci_pt->getUpLink();
			//vector<string> messages;
			int superstepcount = tci_pt->getCurrentSuperStep();

			superStepRecord << "-------runASuperStep method started  " << endl;
			superStepRecord << "============CurrentSuperStep	" << superstepcount <<"==========================="<< endl;
			superStepRecord.flush();
			//aggregate
			AggregatorContainer* ACP=ft->CreateAggC();
			AggregatorOperator AO=AggregatorOperator(ACP);
			if (tci_pt->isLoadOver()) {
				superStepRecord << "loadover"<<endl;
				vector<BSPMessage> bspMsg;
				BSP* bsp = ft->CreatBsp();
				vector<AggregateValue*> aggResult=AO.getAggC()->getAggregateResults();
					if (superstepcount != 0) {
						vector<AggregateValue*>::iterator ite =
								aggResult.begin();
						vector<vector<string> > aggregateValues =
								tci_pt->getAggregateValues();
						vector<vector<string> >::iterator it =
								aggregateValues.begin();
						int count = 0;
						while (ite != aggResult.end()) {
							(*ite)->initValue((*it).at(count));
							superStepRecord << "aggregateResult:"
									<< (*it).at(count) << endl;
							ite++;
							count++;
						}
				}
				SuperStepContext ssContext(superstepcount);
				ssContext.publishAggregateValues(AO.getAggC()->getAggregateResults());
				bsp->initBeforeSuperStep(ssContext);
				AO.initBeforeSuperstep(&ssContext);
				tci_pt->initBeforeSuperStep(superstepcount);
				int tmpCounter = tci_pt->getGraphData()->sizeForAll();

				if (tmpCounter != 0) {
					superStepRecord << "loadover2,temCounter:"<< tmpCounter <<endl;
					for (int i = 0; i < tmpCounter; i++) {
						//superStepRecord << "loadover2,i:"<< i <<endl;
						Vertex* vertex = tci_pt->getGraphData()->getForAll(i);
						if (superstepcount != 0) {
							vector<string>* smessages=(tci_pt->getMessages(vertex->getVertexID()));
							BSPMessage msgTemp;
							 for(int j=0; j<(*smessages).size(); j++){
							 msgTemp.fromString((*smessages)[j]);
							 bspMsg.push_back(msgTemp);
							 }
							 (*smessages).clear();
							 vector<BSPMessage> bspmessages=tci_pt->getBSPMessages();
							 bspmessages.clear();
							 tci_pt->cleanBSPMessages();

						}
						bool activeFlag =tci_pt->getGraphData()->getActiveFlagForAll(i);
						AO.aggregate(bspMsg,vertex,superstepcount); //compute aggregateValue
						if (!activeFlag && (bspMsg.size() == 0)) {
							continue;
						}
						BSPStaffContext context(vertex, superstepcount);
						BSPStaffContext* staffContext = &context;
						bsp->compute(bspMsg, staffContext);  //local compute
						//Vertex* ver=context.getVertex();
						vector<BSPMessage> messageCache =staffContext->getMessagesCache();
						vector<BSPMessage>::iterator ite = messageCache.begin();
						uplink->sendNewMessage(messageCache); // send to java
						bspMsg.clear();
					}
				}
				superStepRecord << "after all compute"<<endl;
				vector<string> aggVs;
				vector<AggregateValue*> AggTemp=AO.getAggC()->getAggregateValues();
				vector<AggregateValue*>::iterator iteV =AggTemp.begin();
				while(iteV!=AggTemp.end()){
					aggVs.push_back((*iteV)->getValue());
				iteV++;
				}
				uplink->setAggregateValue(aggVs);
				superStepRecord<<"after send aggvalue: "<< aggVs.at(0)<<endl;
				uplink->currentSuperStepDone();
				superStepRecord << "-------------current supter step done---------------" << endl;
				tci_pt->setCurrentSuperStep(++superstepcount);
				tci_pt->cleanAggregateValues();
				superStepRecord.flush();

			}
			doneASuperStep = false;
		} else {
			sleep(1);
		}
	}//while
	}
	return NULL;

};

/**
 * Ping the parent every 5 seconds to know if it is alive
 */
void* ping(void* ptr) {
	TaskContextImpl* context = (TaskContextImpl*) ptr;
	char* portStr = getenv("bcbsp.pipes.command.port");
	int MAX_RETRIES = 3;
	int remaining_retries = MAX_RETRIES;
	ofstream command;
	command.open("~/ping.txt", ios::trunc); //ios::trunc表示在打开文件前将文件清空，由于是写入，文件不存在则创建

	while (!context->isDone()) {
		try {
			sleep(5);
			int sock = -1;
			if (portStr) {
				sock = socket(PF_INET, SOCK_STREAM, 0);
				BSP_ASSERT(sock != -1,
						string("problem creating socket: ") + strerror(errno));
				sockaddr_in addr;
				addr.sin_family = AF_INET;
				addr.sin_port = htons(toInt(portStr));
				addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
				if (connect(sock, (sockaddr*) &addr, sizeof(addr)) == 0) {
					command << "ping success!!" << endl;

				} else {
					command
							<< string("problem connecting command socket: ")
									+ strerror(errno) << endl;
				}
				command.flush();
				BSP_ASSERT(connect(sock, (sockaddr* ) &addr, sizeof(addr)) == 0,
						string("problem connecting command socket: ") + strerror(errno));

			}
			if (sock != -1) {
				int result = shutdown(sock, SHUT_RDWR);
				if (result != 0) {
					command << "problem shutting socket" << endl;
				}
				BSP_ASSERT(result == 0, "problem shutting socket");
				result = close(sock);
				if (result != 0) {
					command << "problem closing socket" << endl;
				}
				BSP_ASSERT(result == 0, "problem closing socket");
				command.flush();
			}
			remaining_retries = MAX_RETRIES;
		} catch (Error& err) {
			if (!context->isDone()) {
				command
						<< string("Hadoop Pipes Exception: in ping")
								+ err.getMessage().c_str() << endl;
				command.flush();
				fprintf(stderr, "Hadoop Pipes Exception: in ping %s\n",
						err.getMessage().c_str());
				remaining_retries -= 1;
				if (remaining_retries == 0) {
					exit(1);
				}
			} else {
				return NULL;
			}
		}
	}
	return NULL;
}

/**
 * Run the assigned task in the framework.
 * The user's main function should set the various functions using the
 * set* functions above and then call this.
 * @return true, if the task succeeded.
 */
bool runTask(const Factory& factory) {
	ofstream runT;

	try {
		TaskContextImpl* context = new TaskContextImpl(factory);
		Protocol* connection;
		char* portStr = getenv("bcbsp.pipes.command.port");
		char* processType=getenv("processType");
		ptype=processType;
		char* bcbspLogDir=getenv("bcbsp.log.dir");
		string logdir=bcbspLogDir;
		ClogDir=bcbspLogDir;
		string open;
		if(strcmp(processType,"staff")==0){
			char* staffId=getenv("staffID");
			string sid=staffId;
			string spath=logdir+"/userlogs/"+sid;
			string tpath=logdir;
			string open=spath+"/runT.txt";
			runT.open(open.c_str(), ios::trunc);
			open=spath+"/command.txt";
			command.open(open.c_str(), ios::trunc);
			open=spath+"/superStep.txt";
			superStepRecord.open(open.c_str(), ios::trunc);
			command<<"c++ get staffID is: "<<staffId<<endl;

			open=logdir+"/userlogs/"+sid+"/stafftest.txt";
			testforlog.open(open.c_str(), ios::trunc);
			command<<"open staff logfile"<<endl;
		}
		else if(strcmp(processType,"WorkerManager")==0){
			string opendir=logdir+"/runT.txt";
			runT.open(opendir.c_str(), ios::trunc);
			opendir = logdir + "/command.txt";
			command.open(opendir.c_str(), ios::trunc);
			command<<"creat workermanager c++ successed"<<endl;
			opendir = logdir + "/superStep.txt";
			superStepRecord.open(opendir.c_str(), ios::trunc);
		}

		runT << "port is " << portStr << endl;
		int sock = -1;
		FILE* stream = NULL;
		FILE* outStream = NULL;
		char *bufin = NULL;
		char *bufout = NULL;
		if (portStr) {
			sock = socket(PF_INET, SOCK_STREAM, 0);
			//test----problem when creating socket
			if (sock == -1) {

				runT << "error:problem creating socket" << endl;
				runT.flush();
			}
			//test----the socket is created successfully
			if (sock != -1) {

				runT << "sock!= -1, portStr= " << portStr;
				runT.flush();

			}

			sockaddr_in addr;
			addr.sin_family = AF_INET;
			addr.sin_port = htons(toInt(portStr));
			addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

			int a = connect(sock, (sockaddr*) &addr, sizeof(addr)); //connect to the server!
			if (a == 0) {
				runT << "ip=" << inet_ntoa(addr.sin_addr) << endl;
				runT << "success!" << endl;
				runT.flush();
			} else {

				runT << "ip=" << inet_ntoa(addr.sin_addr) << endl;
				runT << "failure!" << endl;
				runT.flush();
			}

			stream = fdopen(sock, "r");
			outStream = fdopen(sock, "w");

			// increase buffer size
			int bufsize = 128 * 1024;
			int setbuf;
			bufin = new char[bufsize];
			bufout = new char[bufsize];
			setbuf = setvbuf(stream, bufin, _IOFBF, bufsize);
			runT<< string("problem with setvbuf for outStream: ")
							+ strerror(errno) << endl;
			BSP_ASSERT(setbuf == 0,
					string("problem with setvbuf for inStream: ") + strerror(errno));
			setbuf = setvbuf(outStream, bufout, _IOFBF, bufsize);
			runT<< string("problem with setvbuf for outStream: ")
							+ strerror(errno) << endl;
			BSP_ASSERT(setbuf == 0,
					string("problem with setvbuf for outStream: ") + strerror(errno));

			connection = new BinaryProtocol(stream, context, outStream);
		}
		context->setProtocol(connection, connection->getUplink());
		//pthread_t pingThread;
		//pthread_create(&pingThread, NULL, ping, (void*) (context));

		pthread_t runASuperStepThread;
		pthread_create(&runASuperStepThread, NULL, runASuperStep,
				(void *) context);
		runT << "before waitForTask" << endl;
		runT.flush();
		context->waitForTask();
		runT << "ater waitForTask" << endl;
		//pthread_join(pingThread, NULL);
		runT << "ping thread down" << endl;
		pthread_join(runASuperStepThread, NULL);
		runT << "superStep thread down" << endl;
		runT.flush();
		delete context;
		delete connection;
		if (stream != NULL) {
			fflush(stream);
		}
		if (outStream != NULL) {
			fflush(outStream);
		}
		fflush(stdout);
		if (sock != -1) {
			int result = shutdown(sock, SHUT_RDWR);
			if (result != 0) {
				runT << "problem shutting socket" << endl;
			}
			BSP_ASSERT(result == 0, "problem shutting socket");
			result = close(sock);
			BSP_ASSERT(result == 0, "problem closing socket");
			if (result != 0) {
				runT << "problem shutting socket" << endl;
			}
		}
		if (stream != NULL) {
			fclose(stream);
		}
		if (outStream != NULL) {
			fclose(outStream);
		}
		delete bufin;
		delete bufout;
		runT << "the c++ staff success" << endl;
		command.close();
		superStepRecord.close();
		runT.close();
		return true;
	} catch (Error& err) {
		runT << string("Bsp Pipes Exception:") + err.getMessage().c_str()
				<< endl;
		runT.flush();
		fprintf(stderr, "Bsp Pipes Exception: %s\n", err.getMessage().c_str());
		return false;
	}
}
}

