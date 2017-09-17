/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 10
#define GOSSIP_FANOUT 3

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    HEARTBEAT,
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];
    bool isIntroducer;
    
public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	void processPeerMemberList(const vector<MemberListEntry>& peerMemberList);
	
	virtual ~MP1Node();
	
private:
    void processJoinReqMsg(void *env, char *data, int size);
    void processJoinRepMsg(void *env, char *data, int size);
    void processHeartbeatMsg(void *env, char *data, int size);
    void removeStaleMembers();
    void updateOwnHeartbeat();
    void* prepareHeartbeatMsg(Address* addrPtr, long* heartBeat, vector<MemberListEntry>& memberList, size_t* msgSize);
    void* prepareJoinRepMsg(Address* addrPtr, long* heartBeat, vector<MemberListEntry>& memberList, size_t* msgSize);
    bool isNodePresentInList(const MemberListEntry& node);
    
    vector<MemberListEntry> selectFanoutRandomPeers();
    void printMemberList(const vector<MemberListEntry>& memberList);
	void printMemberList(vector<MemberListEntry>::const_iterator begin, vector<MemberListEntry>::const_iterator end);
};

#endif /* _MP1NODE_H_ */
