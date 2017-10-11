/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

//Forward decl
bool areMemListsEqual(vector<Node>& vec1, vector<Node>& vec2); 

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
	sort(curMemList.begin(), curMemList.end());

  //Step 3: Run the stabilization protocol IF REQUIRED
  bool change = areMemListsEqual(curMemList, ring);
  
  //Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
  if(!ht->isEmpty() && change) {
    stabilizationProtocol();
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
	Message message(g_transID++, memberNode->addr, MessageType::CREATE, key, value);
	
	//TODO: Keep state of this transaction ID and create request.
	//Log when QUORUM acknoledgements are recieved for this transaction ID.
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
	Message message(g_transID++, memberNode->addr, MessageType::READ, key);
	
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
	Message message(g_transID++, memberNode->addr, MessageType::UPDATE, key, value);
	
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
	Message message(g_transID++, memberNode->addr, MessageType::DELETE, key);
	
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
		log->logUpdateFail(&memberNode->addr, false, 0, key, value);
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

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */

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
	
	//--- Handle corner cases with 1,2 nodes in the system ---
	//If ring size is 2 (shortcut -- hacky)
		//Transfer all data to the peer
	
	//If ring size is 1
		//No action
		
	//--- Common logic for node addition/deletion in/from the ring ---
	//Iterate all data keys and call findNodes method().
		//If current node is part of find Nodes (I am the replica)
			//Check if old HRO == new HRO - then ignore
			//If not, 
				//and if I am the primary replica (optional?), 
				//and if that data is applicable for new HRO (i.e. new HRO present in find Nodes), 
					//then transfer this data to new HRO(s). (These node(s) are new owners.) 
					//If success, add this node(s) to HRO.
		
		//If current node is part of find Nodes (I am the owner)
			//Check if old IMR == new IMR - then ignore
			//If not, then transfer this data to new IMR(s). (These node(s) are prospective replica.) If success, add this node(s) to IMR.
	
	//--- Delete unwanted data (After successfuly processing all data keys) ---
	//Iterate all data keys and call findNodes method.
		//If current node is not a part of find Nodes, delete that data key. [Can be done within +- affected node]


	//--- Update new vectors (HRO, IMR) to the global ones. ---
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
