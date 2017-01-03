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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Log : log entries in raft
type Log struct {
	Term    int
	Command interface{}
}

//
// Raft : A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//TODO: may need to store cluster info?
	// persistent statue on all server
	currentTerm int
	voteFor     int
	log         []Log

	// volatile state on all server
	role        Status
	commitIndex int
	lastApplied int

	applyChan chan ApplyMsg

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	// election channel
	electionChan chan bool
	// heartbeat channel
	heartbeatChan chan bool
}

// Status : machine role in raft, leader, follower and candidate
type Status int

const (
	//Leader : leader in raft paper
	Leader = 1 << iota
	//Candidate : candidate in raft paper
	Candidate
	//Follower : follower in raft paper
	Follower
)

// GetState eturn currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	term := rf.currentTerm
	isleader := (rf.role == Leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

// AppendEntriesArgs AppendEntries RPC Request arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []Log

	LeaderCommit int
}

// AppendEntriesReply AppendEntries RPC Relpy arguments structure.
type AppendEntriesReply struct {
	Term    int
	Success bool
	// this is used to speed up append
	LogIndex int
}

// VoteArgs : Vote RPC Request arguments structure.
type VoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogTerm  int
	LastLogIndex int
}

// VoteReply : Vote RPC Request reply structure.
type VoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// Vote : Vote RPC handler.
func (rf *Raft) Vote(args VoteArgs, reply *VoteReply) {
	// Your code here.
	fmt.Printf("VOTE : %v get from %v\n", rf.me, args.CandidateID)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//Receiver implementation 1
	if args.Term < rf.currentTerm {
		fmt.Printf("VOTE_DENY1 : %v get from %v with %v %v\n", rf.me, args.CandidateID, rf.currentTerm, args.Term)
		return
	}
	// args.Term > currentTerm, so update it
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
		rf.persist()
	}
	//Receiver implementation 2
	if !(rf.voteFor == -1 || rf.voteFor == args.CandidateID) {
		fmt.Printf("VOTE_DENY3 : %v get from %v, voteFor %v %v\n", rf.me, args.CandidateID, rf.voteFor, args.CandidateID)
		return
	}
	lastLog := rf.log[len(rf.log)-1]
	if args.LastLogTerm < lastLog.Term {
		fmt.Printf("VOTE_DENY2 : %v get from %v with term %v < %v %v\n", rf.me, args.CandidateID, lastLog.Term, args.LastLogTerm, args.LastLogTerm < lastLog.Term)
		return
	}

	if args.LastLogTerm == lastLog.Term && len(rf.log) > args.LastLogIndex {
		fmt.Printf("VOTE_DENY2 : %v get from %v with index %v <= %v %v\n", rf.me, args.CandidateID, len(rf.log), args.LastLogIndex, args.LastLogIndex < len(rf.log))
		return
	}

	// if rf.voteFor == -1 {
	// 	lastLog = rf.log[len(rf.log)-1]
	// 	if args.LastLogTerm >= lastLog.Term && args.LastLogIndex >= len(rf.log) {
	rf.toFollower()
	reply.VoteGranted = true
	rf.voteFor = args.CandidateID
	rf.heartbeatChan <- true
	rf.persist()
	// }
	// }

}

func (rf *Raft) handle() {
	rand.Seed(time.Now().UnixNano())
	maxTime := 500
	base := 200
	timer := time.NewTimer(time.Duration(rand.Intn(maxTime)+base) * time.Millisecond)
	for {
		select {
		// heartbeat timeout, start to election
		case <-timer.C:
			rf.election()
			timer.Reset(time.Duration(rand.Intn(maxTime)+base) * time.Millisecond)
		// we get heartbeat
		case <-rf.heartbeatChan:
			// fmt.Printf("HEARTBEAT : %v get heartbeat\n", rf.me)
			timer.Reset(time.Duration(rand.Intn(maxTime)+base) * time.Millisecond)
		// we win election, init leader
		case <-rf.electionChan:
			fmt.Printf("ELECTION_WIN : %d\n", rf.me)
			rf.mu.Lock()
			rf.role = Leader
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
				rf.nextIndex[i] = len(rf.log)
			}
			rf.mu.Unlock()
			go rf.sendHeartbeat()
			timer.Reset(time.Duration(rand.Intn(maxTime)+base) * time.Millisecond)
		}
	}
}

func median(array []int) int {
	arr := make([]int, len(array))
	copy(arr, array)
	sort.Ints(arr)
	return arr[len(array)/2]
}

func (rf *Raft) commit(index int) {
	fmt.Printf("COMMIT : %v commit from %v to %v %v\n", rf.me, rf.commitIndex, index, rf.log[rf.commitIndex+1:index+1])
	for i := rf.commitIndex + 1; i <= index; i++ {
		msg := ApplyMsg{i, rf.log[i].Command, false, nil}
		rf.applyChan <- msg
	}
	rf.commitIndex = index
}

func (rf *Raft) sendHeartbeat() {
	for i, peer := range rf.peers {
		go func(i int, peer *labrpc.ClientEnd) {
			for rf.role == Leader {
				if i == rf.me {
					rf.heartbeatChan <- true
					rf.matchIndex[i] = len(rf.log)
					rf.nextIndex[i] = len(rf.log) + 1
					mid := median(rf.matchIndex)
					if rf.log[mid].Term == rf.currentTerm && mid > rf.commitIndex {
						rf.commit(mid)
					}
				} else {
					begin := rf.nextIndex[i]
					end := len(rf.log)
					if begin > end {
						begin = end
					}
					arg := AppendEntriesArgs{rf.currentTerm, rf.me, begin - 1, rf.log[begin-1].Term, rf.log[begin:end], rf.commitIndex}
					reply := AppendEntriesReply{}
					ok := peer.Call("Raft.AppendEntries", arg, &reply)
					if !ok {
						// time.Sleep(25 * time.Millisecond)
						continue
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.toFollower()
						rf.persist()
						fmt.Printf("TO_FOLLOWER : %v down from Leader to Follower\n", rf.me)
					} else {
						rf.matchIndex[i] = reply.LogIndex - 1
						rf.nextIndex[i] = reply.LogIndex
					}
					rf.mu.Unlock()
				}
				time.Sleep(25 * time.Millisecond)
			}
		}(i, peer)
	}

	// for rf.role == Leader {
	// 	for i, peer := range rf.peers {
	// 		go func(i int, peer *labrpc.ClientEnd) {
	// 			if i == rf.me {
	// 				rf.heartbeatChan <- true
	// 				rf.matchIndex[i] = len(rf.log)
	// 				rf.nextIndex[i] = len(rf.log) + 1
	// 			} else {
	// 				rf.mu.Lock()
	// 				begin := rf.nextIndex[i]
	// 				end := len(rf.log)
	// 				if begin > end {
	// 					begin = end
	// 				}
	// 				arg := AppendEntriesArgs{rf.currentTerm, rf.me, begin - 1, rf.log[begin-1].Term, rf.log[begin:end], rf.commitIndex}
	// 				rf.mu.Unlock()
	// 				reply := AppendEntriesReply{}
	// 				ok := peer.Call("Raft.AppendEntries", arg, &reply)
	// 				if !ok {
	// 					return
	// 				}
	// 				rf.mu.Lock()
	// 				if reply.Term > rf.currentTerm {
	// 					rf.currentTerm = reply.Term
	// 					rf.toFollower()
	// 					fmt.Printf("TO_FOLLOWER : %v down from Leader to Follower\n", rf.me)
	// 				} else {
	// 					rf.matchIndex[i] = reply.LogIndex - 1
	// 					rf.nextIndex[i] = reply.LogIndex
	// 				}
	// 				rf.mu.Unlock()
	// 			}
	// 		}(i, peer)
	// 	}
	// 	mid := median(rf.matchIndex)
	// 	if rf.log[mid].Term == rf.currentTerm && mid > rf.commitIndex {
	// 		rf.commit(mid)
	// 	}
	// 	time.Sleep(50 * time.Millisecond)
	// }
}

func (rf *Raft) toFollower() {
	rf.role = Follower
	rf.voteFor = -1
}

// AppendEntries : append entries to log
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		fmt.Printf("APPEND_FAIL0 : %v append with %v, return %v\n", rf.me, args.Term, reply.Term)
		return
	}

	rf.heartbeatChan <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
		rf.persist()
	}

	// fmt.Printf("APPEND_TRY : %v append with %v/ %v, %v/ %v\n", rf.me, len(rf.log), args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)

	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		reply.LogIndex = len(rf.log)
		fmt.Printf("APPEND_FAIL1 : %v append with %v, return %v\n", rf.me, args.PrevLogIndex, reply.LogIndex)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		// find first one that have same term with entries
		for i := args.PrevLogIndex - 1; i > 0; i-- {
			if rf.log[i].Term == args.PrevLogTerm {
				reply.LogIndex = i
			} else {
				break
			}
		}
		if reply.LogIndex < 1 {
			reply.LogIndex = 1
		}
		fmt.Printf("APPEND_FAIL2 : %v append with %v, %v, return %v\n", rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, reply.LogIndex)
		return
	}

	if len(args.Entries) > 0 {
		fmt.Printf("APPEND : %v append with %v, %v\n", rf.me, args.Entries[0].Term, args.Entries)
	}

	rf.log = rf.log[:args.PrevLogIndex+1]
	for _, log := range args.Entries {
		rf.log = append(rf.log, log)
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commit(args.LeaderCommit)
	}
	reply.LogIndex = len(rf.log)
}

func (rf *Raft) election() {
	vote := 0
	rf.role = Candidate
	rf.currentTerm++
	fmt.Printf("ELECTION_START : %d timeout, start election with term %v\n", rf.me, rf.currentTerm)
	for i, peer := range rf.peers {
		go func(i int, peer *labrpc.ClientEnd) {
			if i == rf.me {
				vote++
			} else {
				rf.mu.Lock()
				arg := VoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].Term, len(rf.log)}

				rf.mu.Unlock()
				var reply VoteReply
				// ok := false
				// we invoke this inside go, must ensure its role doesn't change
				// for !ok && rf.role == Candidate {
				ok := peer.Call("Raft.Vote", arg, &reply)
				if !ok {
					return
				}
				// }
				fmt.Printf("ELECTION : %v send to %v with last term %v, %v\n", rf.me, i, rf.log[len(rf.log)-1].Term, reply.VoteGranted)
				if reply.VoteGranted {
					rf.mu.Lock()
					vote++
					if vote > len(rf.peers)/2 && rf.role == Candidate {
						rf.electionChan <- true
					}
					rf.mu.Unlock()
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term + 1
				}
			}
		}(i, peer)
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
func (rf *Raft) sendVote(server int, args VoteArgs, reply *VoteReply) bool {
	ok := rf.peers[server].Call("Raft.Vote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := len(rf.log)
	term := rf.currentTerm

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := (rf.role == Leader)
	if isLeader {
		fmt.Printf("APPEND : Leader %v append %v with %v\n", rf.me, command, term)
		log := Log{term, command}
		rf.log = append(rf.log, log)
		rf.persist()
	}

	return index, term, isLeader
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

	// Your initialization code here.
	rf.currentTerm = 0
	// -1 is nil for voteFor
	rf.voteFor = -1
	log := Log{0, nil}
	// log has at least one element, convenient for programming
	rf.log = []Log{log}

	// volatile state on all server
	rf.role = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyChan = applyCh

	// volatile state on leader
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionChan = make(chan bool)
	rf.heartbeatChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.handle()

	return rf
}
