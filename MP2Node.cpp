/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

//Forward decl
bool areMemListsEqual(vector<Node>& vec1, vector<Node>& vec2); 

void printNodeVector(const vector<Node>& vec, string vecName) {
#ifdef DEBUGLOGMP2
	for(const Node& node : vec) {
		std::cout << "Printing vector: " + vecName << endl;
		std::cout << "Node " + const_cast<Node&>(node).getAddress()->getAddress() << "\t";
	}
#endif
}

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	vector<Node> curMemList;

	//Step 1. Get the current membership list from Membership Protocol / MP1
	curMemList = getMembershipList();

	//Step 2: Construct the ring
 	//Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end(), [](Node node1, Node node2) {
		return node1.getHashCode() < node2.getHashCode();
	});

	//Step 3: Run the stabilization protocol IF REQUIRED
	bool change = areMemListsEqual(curMemList, ring);

	//Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(!ht->isEmpty() && change) {
		//stabilizationProtocol();
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

void MP2Node::sendMessage(Address* toAddr, Message& message) {
	string data = message.toString();
	emulNet->ENsend(&memberNode->addr, toAddr, data);
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	Message message(++g_transID, memberNode->addr, MessageType::CREATE, key, value);
	
	//Keep global state for tracking QUORUMS
	RequestResponseState requestResponseState(key, value, par->getcurrtime(), 0);
	requestResponseStateMap.emplace(g_transID, requestResponseState);
	
	vector<Node> replicas = findNodes(key);
	for(Node& node : replicas) {
		sendMessage(&node.nodeAddress, message);
	}
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	Message message(++g_transID, memberNode->addr, MessageType::READ, key);
	
	//TODO: Keep state of this transaction ID and read request.
	//Log when QUORUM read-replies are recieved for this transaction ID.
	vector<Node> replicas = findNodes(key);
	for(Node& node : replicas) {
		sendMessage(&node.nodeAddress, message);
	}
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	Message message(++g_transID, memberNode->addr, MessageType::UPDATE, key, value);
	
	//TODO: Keep state of this transaction ID and update request.
	//Log when QUORUM acknowledgements are recieved for this transaction ID.
	vector<Node> replicas = findNodes(key);
	for(Node& node : replicas) {
		sendMessage(&node.nodeAddress, message);
	}
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	Message message(++g_transID, memberNode->addr, MessageType::DELETE, key);
	
	//TODO: Keep state of this transaction ID and delete request.
	//Log when QUORUM acknowledgements are recieved for this transaction ID.
	vector<Node> replicas = findNodes(key);
	for(Node& node : replicas) {
		sendMessage(&node.nodeAddress, message);
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	//TODO: Find use of replicaType
	bool result = ht->create(key, value);

#ifdef DEBUGLOGMP2
	if(result) {
		log->logCreateSuccess(&memberNode->addr, false, 0, key, value);
	}
	else {
		log->logCreateFail(&memberNode->addr, false, 0, key, value);
	}
#endif

	return result;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {

	string value = ht->read(key);

#ifdef DEBUGLOGMP2
	if(value.empty()) {
		log->logReadFail(&memberNode->addr, false, 0, key);
	}
	else {
		log->logReadSuccess(&memberNode->addr, false, 0, key, value);
	}
#endif

	return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	//TODO: Find use of replicaType
	bool result = ht->update(key, value);

#ifdef DEBUGLOGMP2
	if(result) {
		log->logUpdateSuccess(&memberNode->addr, false, 0, key, value);
	}
	else {
		log->logUpdateFail(&memberNode->addr, false, 0, key, value);
	}
#endif

	return result;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	
	bool result = ht->deleteKey(key);

#ifdef DEBUGLOGMP2
	if(result) {
		log->logDeleteSuccess(&memberNode->addr, false, 0, key);
	}
	else {
		log->logDeleteFail(&memberNode->addr, false, 0, key);
	}
#endif

	return result;
}

//TODO: Who calls this?
/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	//Go through all requests and log/delete expired requests
	for(auto state : requestResponseStateMap) {
		int transID = state.first;
		int requestTime = state.second.requestTime;
		string key = state.second.key;
		string value = state.second.value;

		if(par->getcurrtime() - requestTime > RESPONSE_EXPIRY_TIME) {
#ifdef DEBUGLOGMP2
			log->logCreateFail(&memberNode->addr, true, transID, key, value);
#endif
			requestResponseStateMap.erase(transID);
		}
	}
	
	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
	
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string strMessage(data, data + size);
		Message message(strMessage);

		//TODO: Figure out how to NOT log stabilization create key requests
		//Handle the message types here
		switch(message.type) {
		
		case MessageType::CREATE:
			createKeyValue(message.key, message.value, ReplicaType::SECONDARY);
			//Reply if this client-initiated
			if(message.transID != 0) {
				Message message(message.transID, memberNode->addr, MessageType::REPLY, true);
				sendMessage(&message.fromAddr, message);
			}
		break;
			
		case MessageType::REPLY: 
			map<int, RequestResponseState>::iterator itr = requestResponseStateMap.find(message.transID);
			if(itr != requestResponseStateMap.end()) {
				RequestResponseState state = itr->second;
				int requestTime = state.requestTime;
				int responseCount = state.responseCount + 1;

				//Expired requests already handled above <<TODO: Issue in multi-threaded env>>			
				//If QUORUM, log success message and remove entry from global state
				if(responseCount >= 2) {
#ifdef DEBUGLOGMP2	
					log->logCreateSuccess(&memberNode->addr, true, message.transID, message.key, message.value);
#endif
					requestResponseStateMap.erase(message.transID);
				}
			}
			else {
				//No processing required
			}
		break;
		};

		//If I receive a create key request in the stabilization phase, 
		//Send create key request to all replicas.
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}


/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	Node currentNode(memberNode->addr);
	
	//--- Handle cases with 1,2 nodes in the system ---
	//If ring size is 1, No action
	if(ring.size() == 1) {
		return;
	}

	//If ring size is 2, transfer all data to the peer (shortcut -- hacky)
	if(ring.size() == 2) {
		for(map<string, string>::const_iterator it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it) {
			Message message(0, memberNode->addr, MessageType::CREATE, it->first, it->second);
			Node peer = getNextNode(ring, currentNode);
			sendMessage(&peer.nodeAddress, message);
		}
	}
	
	//Prepare new HRO
	vector<Node> newHRO;
	Node prevNode = getPrevNode(ring, currentNode);
	Node prevPrevNode = getPrevNode(ring, prevNode);
	newHRO.emplace_back();
	newHRO.emplace_back();

	//Prepare new IMR
	vector<Node> newIMR;
	Node nextNode = getNextNode(ring, currentNode);
	Node nextNextNode = getNextNode(ring, nextNode);
	newIMR.emplace_back(nextNode);
	newIMR.emplace_back(nextNextNode);

	//--- Common logic for node addition/deletion in/from the ring ---
	//Iterate all data keys and call findNodes method().
	for(map<string, string>::const_iterator data = ht->hashTable.begin(); data != ht->hashTable.end(); ++data) {
		vector<Node> nodes = findNodes(data->first);
		
		//Find role of current node for the data (replica/owner)
		vector<Node>::iterator itr = std::find_if(nodes.begin(), nodes.end(), [&](Node& node) {
			return node.getHashCode() == hashFunction(memberNode->addr.getAddress());
		});
		int distance = std::distance(nodes.begin(), itr);
		bool isCurrentNodeOwner = distance == 0;
		bool isCurrentNodeReplica = distance == 1 || distance == 2;
		bool isCurrentNodeSecondaryReplica = distance == 1;
		bool isCurrentNodeTertiaryReplica = distance == 2;

		//If current node is part of find Nodes (I am the replica)
		if(isCurrentNodeReplica) {
			//Check if old HRO == new HRO, then no action
			if(!areMemListsEqual(haveReplicasOf, newHRO)) {
				//if I am the secondary replica <<TODO: OR there are 2 new HROs (2 insert case), 
				if(isCurrentNodeSecondaryReplica) {
					//and if that data is applicable for new HRO (i.e. new HRO present in find Nodes), 
					for(Node hroNode : getIntersection(nodes, newHRO)) {
						//then transfer this data to new HRO(s). (These node(s) are new owners.) 
						Message message(0, memberNode->addr, MessageType::CREATE, data->first, data->second);
						sendMessage(hroNode.getAddress(), message);
						//If success, add this node(s) to HRO. <<Not required. Assume success.>>
					}
				}
			}
		}
		
		
		//If current node is part of find Nodes (I am the owner)
		if(isCurrentNodeOwner) {
			//Check if old IMR == new IMR, then no action
			if(!areMemListsEqual(hasMyReplicas, newIMR)) {
				//Transfer this data to new IMR(s). (These node(s) are prospective replica.) If success, add this node(s) to IMR.
				for(Node newlyIntroducedIMRNode : getElementsInVec1NotInVec2(newIMR, haveReplicasOf)) {
					Message message(0, memberNode->addr, MessageType::CREATE, data->first, data->second);
					sendMessage(newlyIntroducedIMRNode.getAddress(), message);
				}
			}
		}
	}

	//--- Delete unwanted data (After successfuly processing all data keys) ---
	//Iterate all data keys and call findNodes method.
	for(map<string, string>::const_iterator data = ht->hashTable.begin(); data != ht->hashTable.end(); ++data) {
		vector<Node> nodes = findNodes(data->first);
		//If current node is not a part of find Nodes, delete that data key. [<<Can be done within +- affected node>>]
		vector<Node>::iterator itr = std::find_if(nodes.begin(), nodes.end(), [&](Node& node) {
			return node.getHashCode() == hashFunction(memberNode->addr.getAddress());
		});
		int distance = std::distance(nodes.begin(), itr);
		if(distance < 0 || distance > 2) {
			//Delete the key
			ht->deleteKey(data->first);
		}
	}


	//--- Update new vectors (HRO, IMR) to the global ones. ---
	hasMyReplicas = newIMR;
	haveReplicasOf = newHRO;
}

bool areMemListsEqual(vector<Node>& vec1, vector<Node>& vec2) {
  if(vec1.size() != vec2.size()) {
    return false;
  }
  
  //Both empty vectors
  if(vec1.size() == 0) {
    return true;
  }
  
  vector<Node>::iterator citr1 = vec1.begin();
  vector<Node>::iterator citr2 = vec2.begin();
  while(citr1 != vec1.end()) {
    if(citr1->getHashCode() != citr2->getHashCode()) {
      return false;
    }
    
    citr1++;
    citr2++;
  }
  
  return true;
}

/**
 * FUNCTION NAME: getNodeIndexInVector
 *
 * DESCRIPTION: Returns the index of input node present in the ring
 */
int MP2Node::getNodeIndexInVector(vector<Node>& ring, Node& currNode) {

	if(ring.size() == 0) {
		return -1;
	}

	int i = 0;
	while(i < ring.size() && ring[i].getHashCode() != currNode.getHashCode()) {
		i++;
	}

	//Current node must be present in the ring
	//assert(i != ring.size());
		
	return i;
}

/**
 * FUNCTION NAME: getNextNode
 *
 * DESCRIPTION: Returns the node present next to the input node in the ring
 */
Node MP2Node::getNextNode(vector<Node>& ring, Node& currNode) {
	if(ring.size() <= 1) {
		assert(false);
		return Node();
	}
	 
	if (ring.size() >= 2) {	
		int i = getNodeIndexInVector(ring, currNode);
		return ring.at((i+1) % ring.size());
	}
}

/**
 * FUNCTION NAME: getPrevNode
 *
 * DESCRIPTION: Returns the node present previous to the input node in the ring
 */
Node MP2Node::getPrevNode(vector<Node>& ring, Node& currNode) {
	if(ring.size() <= 1) {
		assert(false);
		return Node();
	}
	 
	if (ring.size() >= 2) {	
		int i = getNodeIndexInVector(ring, currNode);
		return ring.at((i-1) % ring.size());
	}
}

/**
 * FUNCTION NAME: compareNodeVectors
 *
 * DESCRIPTION: Returns whether two vectors containing Node are equal
 */
bool MP2Node::compareNodeVectors(vector<Node> vec1, vector<Node> vec2) {
	return std::equal(vec1.begin(), vec1.end(), vec2.begin(), [](Node& node1, Node& node2){
		return node1.getHashCode() == node2.getHashCode();
	});
}

/**
 * FUNCTION NAME: getIntersection
 *
 * DESCRIPTION: Returns intersection of two vectors
 */
vector<Node> MP2Node::getIntersection(vector<Node> v1, vector<Node> v2) {

    vector<Node> v3;

	sort(v1.begin(), v1.end(), [](const Node& node1, const Node& node2) {
		return const_cast<Node&>(node1).getHashCode() < const_cast<Node&>(node2).getHashCode();
	});

	sort(v2.begin(), v2.end(), [](const Node& node1, const Node& node2) {
		return const_cast<Node&>(node1).getHashCode() < const_cast<Node&>(node2).getHashCode();
	});
    
    set_intersection(v1.begin(),v1.end(),v2.begin(),v2.end(),back_inserter(v3), [](Node& node1, Node& node2) {
		return node1.getHashCode() == node2.getHashCode();
	});

    return v3;
}

vector<Node> MP2Node::getElementsInVec1NotInVec2(vector<Node> vec1, vector<Node> vec2) {
	
	std::vector<Node> vec3;
	std::remove_copy_if(vec1.begin(), vec1.end(), std::back_inserter(vec3), [&](const Node& arg) {
		return getNodeIndexInVector(vec2, const_cast<Node&>(arg)) == vec2.size();
	});

	return vec3;
}