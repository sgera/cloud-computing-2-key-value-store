	/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#define DEBUGLOGMP2 1

#define RESPONSE_EXPIRY_TIME 30


class RequestResponseState {
public:
	string key;
	string value;
	int requestTime;
	int responseCount;
	MessageType requestType;	//Allowed values CREATE/UPDATE/DELETE

	RequestResponseState(string key, string value, MessageType requestType, int requestTime, int responseCount) {
		this->key = key;
		this->value = value;
		this->requestType = requestType;
		this->requestTime = requestTime;
		this->responseCount = responseCount;
	}
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	//transID, RequestResponseState)
	map<int, RequestResponseState> requestResponseStateMap;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	//TODO: Who calls this?
	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	// send message over emulNet
	void sendMessage(Address* toAddr, Message& message);

	~MP2Node();

	private:
	int getNodeIndexInVector(vector<Node>& ring, Node& currNode);
	Node getNextNode(vector<Node>& ring, Node& currNode);
	Node getPrevNode(vector<Node>& ring, Node& currNode);
	bool compareNodeVectors(vector<Node> vec1, vector<Node> vec2);
	vector<Node> getIntersection(vector<Node> v1, vector<Node> v2);
	vector<Node> getElementsInVec1NotInVec2(vector<Node> vec1, vector<Node> vec2);
	void printNodeVector(const vector<Node>& vec, string vecName);
};

#endif /* MP2NODE_H_ */
