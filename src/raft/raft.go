package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// some global variable
// var broadcastTime = 15 // time to brocast one message
var commitNumber = 1 // the commend number to commit

// Role : used to represent server's current role
type Role int

const (
	Server Role = iota + 1
	Candidate
	Leader
)

// Log : server used in logs
type Log struct {
	Term    int
	Content interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	mu        sync.Mutex
	locks     []sync.Mutex // lock used in heartbeat

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// presistent state
	curTerm int   // currentTerm
	voteFor int   // voteFor
	log     []Log //log

	// volatile state
	commitIndex int // commitIndex
	lastApplied int // lastApplied, may not be used?

	applyChan chan ApplyMsg

	// leader state
	nextIndex  []int //nextIndex
	matchIndex []int //matchIndex
	entries    []Log // used to store entries in server

	// some variable not in alg to ensure model
	electChan   chan bool // channel to inform that election raise
	isCandidate bool      //  whether the server is candidate
	role        Role      // current role

	commandIndex map[interface{}]int // get command Index

	// some lock. The lock and unlock must follow the following sequence
	// roleMu        sync.Mutex // lock role
	// logMu         sync.Mutex // lock log
	// termMu        sync.Mutex // lock curTerm
	// entriesMu     sync.Mutex // lock entries
	// nextIndexMu   sync.Mutex // lock nextIndex
	// matchIndexMu  sync.Mutex // lock matchIndex
	// commitIndexMu sync.Mutex // lock commitIndex
}

// GetState : return currentTerm and whether this server believes it is the leader.
// use role and term
func (rf *Raft) GetState() (int, bool) {
	// var term int
	// var isleader bool
	// Your code here.
	// rf.roleMu.Lock()
	// rf.termMu.Lock()
	// defer rf.roleMu.Lock()
	// defer rf.termMu.Lock()

	return rf.curTerm, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type VoteArg struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastTerm     int
}

//
// example RequestVote RPC reply structure.
//
type VoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// AppendArg :  used in Append
type AppendArg struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

// AppendReply :  used in Append
type AppendReply struct {
	Term    int
	Success bool
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// appenEntries : used to append entries
// process :
//	1. update timer
//	2. delete different entries
//	3. append entries
//	4. update commitIndex
// resource: log, commitIndex
func (rf *Raft) appendEntries(arg AppendArg) {

	if arg.Entries != nil {
		fmt.Printf("APPEND: %v receive entries from %v, with index %v , term %v content %v\n",
			rf.me, arg.LeaderID, arg.PrevLogIndex, arg.Entries[0].Term, arg.Entries[0].Content)
	}
	// rf.logMu.Lock()
	// defer rf.logMu.Unlock()
	// if append log is conflict with new, delete all follow it
	// if len(rf.log) != 0 && len(rf.log) > arg.PrevLogIndex+1 {
	// 	if arg.PrevLogIndex < 0 {
	// 		rf.log = rf.log[:0]
	// 	} else if rf.log[arg.PrevLogIndex].Term != arg.PrevLogTerm {
	// 		rf.log = rf.log[:arg.PrevLogIndex]
	// 	}
	// }

	curIndex := arg.PrevLogIndex + 1
	// if curIndex < arg.PrevLogIndex {
	// 	curIndex = arg.PrevLogIndex
	// }

	for _, entry := range arg.Entries {
		// if append log is same with new, continue
		if len(rf.log) > curIndex {
			if rf.log[curIndex].Term == entry.Term {
				continue
			} else {
				fmt.Printf("APPEND_CUT : %v cut log from %v to %v with %v %v\n", rf.me, len(rf.log), curIndex, rf.log[curIndex].Term, entry.Term)
				rf.log = rf.log[:curIndex]
			}
		}
		rf.log = append(rf.log, entry)
		fmt.Printf("APPEND : %v append, content : %v, len : %v, term : %v\n", rf.me, entry.Content, len(rf.log), arg.Term)
		curIndex++
	}

	// server commit some command, some need to update it
	// rf.commitIndexMu.Lock()
	// fmt.Printf("COMPARE : %v %v\n", arg.LeaderCommit, rf.commitIndex)
	if arg.LeaderCommit > rf.commitIndex {
		index := min(arg.LeaderCommit, len(rf.log)-1)
		if index > rf.commitIndex {
			rf.commit(index)
		}
	}
	// rf.commitIndexMu.Unlock()
}

// Append : function used to append entries
// process :
// 	1. check term
//	2. check log length, if is 0, append if leader is just start, too
// 	3. check prev is same
//  4. invoke appendEntries to append
// resource: log, term
func (rf *Raft) Append(arg AppendArg, reply *AppendReply) {
	// rf.logMu.Lock()
	// rf.termMu.Lock()
	// defer rf.logMu.Unlock()
	// fmt.Printf("LOCK : %v start lock at Append\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer fmt.Printf("LOCK : %v release lock at Append\n", rf.me)

	if arg.Entries != nil {
		fmt.Printf("APPEND_TRY: %v get entry from %v, term: %v %v, length: %v %v\n",
			rf.me, arg.LeaderID, rf.curTerm, arg.Term, len(rf.log), arg.PrevLogIndex)
	}
	reply.Term = rf.curTerm
	// rf.termMu.Unlock()

	if arg.Term < rf.curTerm {
		fmt.Printf("APPEND_FAIL1 : %v get entry from %v\n", rf.me, arg.LeaderID)
		reply.Success = false
		return
	}

	// update timer
	rf.electChan <- true

	if arg.Term > rf.curTerm {
		fmt.Printf("INFO : %v improve term number: %v -> %v\n", rf.me, rf.curTerm, arg.Term)
		rf.beServer()
		rf.curTerm = arg.Term
	}

	// server just start, don't have log
	if len(rf.log) == 0 {
		// leader just start, too
		reply.Success = true
		rf.appendEntries(arg)
		return
	}

	if len(rf.log) <= arg.PrevLogIndex {
		fmt.Printf("APPEND_FAIL2 : %v get entry from %v, %v %v\n", rf.me, arg.LeaderID, len(rf.log), arg.PrevLogIndex)
		reply.Success = false
		return
	}
	if arg.PrevLogTerm == -1 || rf.log[arg.PrevLogIndex].Term == arg.PrevLogTerm {
		reply.Success = true
		rf.appendEntries(arg)
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendVote(server int, args VoteArg, reply *VoteReply) bool {
	ok := rf.peers[server].Call("Raft.Vote", args, reply)
	return ok
}

// CommandArg is used in StartCommand
type CommandArg struct {
	index   int         // command index
	command interface{} // command content
}

// CommandReply is used in StartCommand
type CommandReply struct {
	result bool // whether add success
	term   int  // currentTrem
	index  int  // current index
}

// leader use it to commit
// find the mid number of matchIndex and update it
// resource : matchIndex, commitIndex
// func (rf *Raft) updateCommitIndex() {
// 	number := 0

// 	// rf.nextIndexMu.Lock()
// 	// rf.matchIndexMu.Lock()
// 	for _, v := range rf.matchIndex {
// 		if v == rf.commitIndex {
// 			number++
// 		}
// 		if rf.log[v].Term < rf.curTerm {
// 			return
// 		}
// 	}
// 	// if commitIndex number >= major, don't need to update it
// 	if number > len(rf.peers)/2 {
// 		return
// 	}
// 	index := seekForMid(rf.matchIndex)
// 	if index > rf.commitIndex {
// 		rf.commit(index)
// 	}

// 	// rf.matchIndexMu.Unlock()
// 	// rf.nextIndexMu.Unlock()
// }

// seekForMid : seek mid number of a array, which used to find commit index in matchIndex array
func seekForMid(array []int) int {
	arr := make([]int, len(array))
	copy(arr, array)
	sort.Ints(arr)
	major := len(array) / 2
	return arr[major]
}

// StartCommand : used by client to append command
// resource : log, entries, nextIndex, matchIndex, commitIndex
// func (rf *Raft) StartCommand(index int, term int, command interface{}) {
// 	// rf.logMu.Lock()
// 	// rf.entriesMu.Lock()
// 	// fmt.Printf("LOCK : %v start lock at StartCommand\n", rf.me)
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// defer fmt.Printf("LOCK : %v release lock at StartCommand\n", rf.me)

// 	rf.entries = append(rf.entries, Log{term, command})
// 	// cache until commitNumber
// 	if len(rf.entries) < commitNumber {
// 		return
// 	}

// 	rf.matchIndex[rf.me] = len(rf.log) - 1
// 	rf.nextIndex[rf.me] = len(rf.log)

// 	// rf.commitIndexMu.Lock()
// 	// rf.commitIndexMu.Unlock()
// 	// var wg sync.WaitGroup
// 	// wg.Add(len(rf.peers))
// 	major := make(chan bool)
// 	for i, peer := range rf.peers {
// 		go func(i int, peer *labrpc.ClientEnd) {
// 			// defer wg.Done()
// 			if i != rf.me {
// 				reply := AppendReply{}
// 				var arg AppendArg
// 				if rf.nextIndex[i] > 0 {
// 					arg = AppendArg{term, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, rf.entries, rf.commitIndex}
// 				} else {
// 					arg = AppendArg{term, rf.me, -1, -1, rf.entries, rf.commitIndex}
// 				}

// 				ok := false
// 				for !ok {
// 					ok = peer.Call("Raft.Append", arg, &reply)
// 				}
// 				major <- reply.Success
// 				if reply.Success {
// 					// rf.nextIndexMu.Lock()
// 					// rf.matchIndexMu.Lock()

// 					rf.matchIndex[i] = len(rf.log) - 1
// 					rf.nextIndex[i] = len(rf.log)

// 					// rf.matchIndexMu.Unlock()
// 					// rf.nextIndexMu.Unlock()
// 				} else {
// 					// not leader, return
// 					if reply.Term > rf.curTerm {
// 						fmt.Printf("DOWN : %v go back to server\n", rf.me)
// 						rf.role = Server
// 						rf.curTerm++
// 						return
// 					}

// 					go rf.logCopy(peer, i)
// 				}
// 			}
// 		}(i, peer)
// 	}

// 	// wg.Wait()
// 	agree := 1
// 	for i := 0; i < len(rf.peers)-1; i++ {
// 		if rf.role != Leader {
// 			return
// 		}
// 		if <-major {
// 			agree++
// 		}

// 	}
// 	if agree > len(rf.peers)/2 {
// 		rf.updateCommitIndex()
// 		// break
// 	}
// 	rf.entries = make([]Log, 0)
// 	return

// 	// rf.entriesMu.Unlock()
// 	// rf.logMu.Unlock()
// }

// logCopy : used by leader to get log consistency with one server
// resource: log, term, nextIndex, matchIndex, commitIndex
// func (rf *Raft) logCopy(peer *labrpc.ClientEnd, i int) {
// 	var entries []Log
// 	// rf.logMu.Lock()
// 	// rf.termMu.Lock()
// 	// rf.nextIndexMu.Lock()
// 	// fmt.Printf("LOCK : %v start lock at logCopy\n", rf.me)
// 	rf.mu.Lock()
// 	for {
// 		// we must copy from start, so break
// 		if rf.nextIndex[i] == 0 {
// 			break
// 		}
// 		entries = rf.log[rf.nextIndex[i]-1 : rf.nextIndex[i]]
// 		var arg AppendArg
// 		if rf.nextIndex[i] > 0 {
// 			arg = AppendArg{rf.curTerm, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, entries, rf.commitIndex}
// 		} else {
// 			arg = AppendArg{rf.curTerm, rf.me, -1, -1, entries, rf.commitIndex}
// 		}
// 		reply := AppendReply{}
// 		peer.Call("Raft.Append", arg, &reply)
// 		if !reply.Success {
// 			rf.nextIndex[i]--
// 			if reply.Term > rf.curTerm {
// 				fmt.Printf("DOWN : %v go back to server\n", rf.me)
// 				rf.role = Server
// 				return
// 			}
// 		} else {
// 			break
// 		}
// 	}
// 	rf.mu.Unlock()
// 	// fmt.Printf("LOCK : %v release lock at logCopy\n", rf.me)

// 	entries = rf.log[rf.nextIndex[i] : len(rf.log)-1]
// 	reply := AppendReply{}

// 	for !reply.Success {
// 		var arg AppendArg
// 		if rf.nextIndex[i] > 0 {
// 			arg = AppendArg{rf.curTerm, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, entries, rf.commitIndex}
// 		} else {
// 			arg = AppendArg{rf.curTerm, rf.me, -1, -1, entries, rf.commitIndex}
// 		}
// 		peer.Call("Raft.Append", arg, &reply)
// 		if reply.Term > rf.curTerm {
// 			fmt.Printf("DOWN : %v go back to server\n", rf.me)
// 			rf.role = Server
// 			return
// 		}
// 	}
// 	// rf.nextIndexMu.Unlock()
// 	// rf.termMu.Unlock()
// 	// rf.logMu.Unlock()

// 	// rf.matchIndexMu.Lock()
// 	// rf.nextIndexMu.Lock()
// 	rf.nextIndex[i] = len(rf.log)
// 	rf.matchIndex[i] = len(rf.log) - 1
// 	// rf.nextIndexMu.Unlock()
// 	// rf.matchIndexMu.Unlock()
// }

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// resource : role, log, term
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// rf.roleMu.Lock()
	if rf.role != Leader {
		return -1, -1, false
	}
	// rf.roleMu.Unlock()

	// rf.logMu.Lock()
	// rf.termMu.Lock()
	// fmt.Printf("LOCK : %v start lock at Start\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer fmt.Printf("LOCK : %v release lock at Start\n", rf.me)
	// if index, ok := rf.commandIndex[command]; ok {
	// 	term := rf.curTerm
	// 	isLeader := true
	// 	return index + 1, term, isLeader
	// } else {
	index := len(rf.log)
	term := rf.curTerm
	rf.log = append(rf.log, Log{term, command})
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log) - 1
	isLeader := true
	// rf.commandIndex[command] = index

	fmt.Printf("COMMAND : %v %v %v %v\n", rf.me, index, term, command)

	return index + 1, term, isLeader
	// }

	// rf.logMu.Unlock()
	// rf.termMu.Unlock()

}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.locks = make([]sync.Mutex, len(peers))

	// Your initialization code here.
	rf.curTerm = -1
	rf.voteFor = -1
	rf.log = make([]Log, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.electChan = make(chan bool)
	rf.isCandidate = false
	rf.role = Server

	rf.applyChan = applyCh

	// states uesd for leader, so don't nend to initialization now
	rf.nextIndex = nil
	rf.matchIndex = nil

	rf.commandIndex = make(map[interface{}]int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.timeForElection()

	return rf
}

func (rf *Raft) beServer() {
	rf.role = Server
	rf.voteFor = -1
}

//
// Vote for a candidate
// process :
// 1. check arg.Term < curTerm .
// 	  if true, we shouldn't vote for it
//    if false, we need to ensure that we are Server now
// 2. check log length.
// 	  if length is 0, this server is just start, so it vote for any body
//    else we need to ensure candidate's log is better than us(paper give how to define 'better')
// one point : when we get a vote, we need to update getHeart
// resource : role, log, term
//
func (rf *Raft) Vote(arg VoteArg, reply *VoteReply) {
	// Your code here.
	// rf.roleMu.Lock()
	// rf.logMu.Lock()
	// rf.termMu.Lock()
	// defer rf.termMu.Unlock()
	// defer rf.logMu.Unlock()
	// fmt.Printf("LOCK : %v start lock at Vote\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer fmt.Printf("LOCK : %v release lock at Vote\n", rf.me)

	if arg.Term < rf.curTerm {
		fmt.Printf("ELECTION_FAIL0 : %v get from %v with term %v %v\n", rf.me, arg.CandidateID, rf.curTerm, arg.Term)
		reply.VoteGranted = false
		return
	}

	if arg.Term > rf.curTerm {
		rf.beServer()
	}
	// rf.roleMu.Unlock()

	if rf.voteFor != -1 && rf.voteFor != arg.CandidateID {
		reply.VoteGranted = false
		return
	}

	if len(rf.log) == 0 {
		// defer rf.termMu.Unlock()
		rf.electChan <- true
		fmt.Printf("ELECTION_VOTE1: %v vote for %v with term : %v %v\n", rf.me, arg.CandidateID, rf.curTerm, arg.Term)
		rf.voteFor = arg.CandidateID
		reply.VoteGranted = true
		rf.curTerm = arg.Term
		rf.beServer()
		return
	}

	if rf.log[len(rf.log)-1].Term > arg.LastTerm {
		reply.VoteGranted = false
		fmt.Printf("ELECTION_FAIL1 : %v get from %v with term %v %v\n", rf.me, arg.CandidateID, rf.log[len(rf.log)-1].Term, arg.LastTerm)
		return
	}

	if rf.log[len(rf.log)-1].Term == arg.LastTerm && len(rf.log) > arg.LastLogIndex+1 {
		fmt.Printf("ELECTION_FAIL2 : %v get from %v, %v %v\n", rf.me, arg.CandidateID, len(rf.log), arg.LastLogIndex)
		reply.VoteGranted = false
		return
	}

	rf.electChan <- true
	fmt.Printf("ELECTION_VOTE2 : %v vote for %v with term : %v %v\n", rf.me, arg.CandidateID, rf.curTerm, arg.Term)
	rf.voteFor = arg.CandidateID
	reply.VoteGranted = true
	rf.curTerm = arg.Term
	rf.beServer()
	return

}

// startElection is used when this server is candidate
// resource term, role, commitIndex
func (rf *Raft) startElection() bool {
	// defer rf.roleMu.Unlock()
	rf.role = Candidate

	// always vote for myself
	agree := 1
	// rf.termMu.Lock()
	// rf.commitIndexMu.Lock()
	rf.curTerm++

	var arg VoteArg
	if len(rf.log) != 0 {
		arg = VoteArg{rf.curTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
	} else {
		arg = VoteArg{rf.curTerm, rf.me, -1, -1}
	}

	agreeChan := make(chan bool)
	for i, peer := range rf.peers {
		go func(i int, peer *labrpc.ClientEnd) {
			if i != rf.me {
				// fmt.Printf("ELECTION : %v send to %v\n", rf.me, i)
				reply := VoteReply{}
				peer.Call("Raft.Vote", arg, &reply)
				if reply.Term > rf.curTerm {
					rf.curTerm = reply.Term
					rf.beServer()
				}
				// fmt.Printf("%v get %v\n", rf.me, reply.VoteGranted)
				agreeChan <- reply.VoteGranted
			}
		}(i, peer)

	}
	// rf.commitIndexMu.Unlock()
	// rf.termMu.Unlock()
	for i := 0; i < len(rf.peers)-1; i++ {
		if <-agreeChan {
			agree++
			// fmt.Printf("VOTE_NUM : %v get %v agree\n", rf.me, agree)
		}
		// receive heart inf, so can't be candidate
		if rf.role != Candidate {
			return false
		}

		if agree > len(rf.peers)/2 {
			return true
		}
	}

	return false
	// fmt.Printf("ELECTION: %v get %v argee\n", rf.me, agree)
	// return agree > len(rf.peers)/2
}

func (rf *Raft) commit(index int) {
	fmt.Printf("COMMIT : %v commit from %v to %v, len %v \n", rf.me, rf.commitIndex, index, len(rf.log))
	for i := rf.commitIndex + 1; i < index+1; i++ {
		fmt.Printf("COMMIT : %v commit, index %v, content %v \n", rf.me, i, rf.log[i].Content)
		msg := ApplyMsg{i + 1, rf.log[i].Content, false, nil}
		rf.applyChan <- msg
	}
	rf.commitIndex = index
}

// init server function
func (rf *Raft) initServer() {
	rf.role = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}

	rf.entries = make([]Log, 0)
}

// sendHeart : leader use it to send heart
//
func (rf *Raft) sendHeart() {
	for {
		if rf.role != Leader {
			return
		}

		mid := seekForMid(rf.matchIndex)
		if mid > rf.commitIndex {
			rf.commit(mid)
		}

		// rf.logMu.Lock()
		// rf.termMu.Lock()
		// rf.nextIndexMu.Lock()

		var wg sync.WaitGroup
		wg.Add(len(rf.peers))
		for i, peer := range rf.peers {
			go func(i int, peer *labrpc.ClientEnd) {
				defer wg.Done()
				// fmt.Printf("HEART : %v send heartbeat to %v\n", rf.me, i)
				if i == rf.me {
					rf.electChan <- true
					return
				}
				// fmt.Printf("LOCK : %v start lock at sendHeart\n", rf.me)
				rf.locks[i].Lock()
				defer rf.locks[i].Unlock()
				// defer fmt.Printf("LOCK : %v release lock at sendHeart\n", rf.me)

				var arg AppendArg
				begin := rf.nextIndex[i]
				if begin > len(rf.log) {
					begin = len(rf.log)
				} else if begin < 0 {
					begin = 0
				}
				var entries []Log
				if len(rf.log) == 0 {
					entries = nil
				} else {
					entries = rf.log[begin:len(rf.log)]
				}

				if rf.role != Leader {
					return
				}
				if len(rf.log) != 0 && rf.nextIndex[i] > 0 {
					arg = AppendArg{rf.curTerm, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, entries, rf.commitIndex}
				} else {
					arg = AppendArg{rf.curTerm, rf.me, -1, -1, entries, rf.commitIndex}
				}
				reply := AppendReply{}
				// fmt.Printf("%v send to %v ,len: %v \n", rf.me, i, len(entries))
				ok := peer.Call("Raft.Append", arg, &reply)

				// fmt.Printf("HEART : %v receive from %v, with %v %v, ok: %v\n", rf.me, i, rf.curTerm, reply.Term, ok)

				if ok {

					if reply.Success {
						rf.matchIndex[i] = len(rf.log) - 1
						rf.nextIndex[i] = len(rf.log)
						return
					}

					if reply.Term > rf.curTerm {
						fmt.Printf("DOWN : %v go back to server\n", rf.me)
						rf.curTerm = reply.Term
						rf.beServer()
						return
					}
					rf.nextIndex[i]--
				}
			}(i, peer)
		}
		// wg.Wait()
		time.Sleep(50 * time.Millisecond)

		// rf.nextIndexMu.Unlock()
		// rf.termMu.Unlock()
		// rf.logMu.Unlock()
	}
}

// time function decided whether start election
func (rf *Raft) timeForElection() {
	timer := time.NewTimer(time.Duration(rf.me*500) * time.Microsecond)
	for {
		// when is candidate, not timer for election
		// if rf.isCandidate {
		// 	time.Sleep(time.Duration(broadcastTime*len(rf.peers)) * time.Millisecond)
		// }

		// start random timer for election
		select {
		case <-rf.electChan:
			// get heat, reset timer
			// fmt.Printf("HEARTBEAT: %v get heartbeat, reset timer\n", rf.me)
			rand.Seed(time.Now().UnixNano())
			waitTime := rand.Intn(400) + 300
			timer.Reset(time.Duration(waitTime) * time.Millisecond)
		case <-timer.C:
			// time out, begin election
			if rf.role != Leader {
				if len(rf.log) != 0 {
					fmt.Printf("ELECTION_START : %v start to election with term %v, last log %v %v\n", rf.me, rf.curTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
				} else {
					fmt.Printf("ELECTION_START : %v start to election with term %v, last log %v -1\n", rf.me, rf.curTerm, len(rf.log))
				}
				result := rf.startElection()
				if result {
					fmt.Printf("ELECTION_WIN : %v win with %v\n", rf.me, rf.curTerm)
					rf.initServer()
					go func() {
						rf.sendHeart()
					}()
				}
			}
			rand.Seed(time.Now().UnixNano())
			waitTime := rand.Intn(400) + 300
			timer.Reset(time.Duration(waitTime) * time.Millisecond)
		}
	}
}
