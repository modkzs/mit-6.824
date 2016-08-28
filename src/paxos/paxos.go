package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me int)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string

	me         int                 // index into peers[]
	acceptSeq  string              // current accept seq
	v          interface{}         // current accept value
	prepareSeq string              // current prepare seq
	values     map[int]interface{} // all history value
	doneSeq    int                 // used in Done
	minSeq     int                 // seq already forget
	tsToSeq    map[int]string      // a map from seq to ts, which is unique globally
	status     map[int]Fate        // a map from seq to status
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// PrepareArg is used in Paxos.Prepare function
type PrepareArg struct {
	Seq string
}

// PrepareReply is used in Paxos.Prepare function
type PrepareReply struct {
	Seq    string
	V      interface{}
	Result bool
}

// AcceptArg is used in Paxos.Accept function
type AcceptArg struct {
	Seq string
	V   interface{}
}

// AcceptReply is used in Paxos.Accept function
type AcceptReply struct {
	Seq    string
	Result bool
}

// DecideArg is used in Paxos.Decide function
type DecideArg struct {
	V   interface{}
	Seq int
}

// ForgetArg used in Paxos.Forget function
type ForgetArg struct {
	Seq int
}

// DoneReply used in Paxos.GetDone function
type DoneReply ForgetArg

// UpdateReply used in Paxos.GetDone function
type UpdateArg ForgetArg

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(s int, v interface{}) {
	// Your code here.
	go func() {
		for {
			length := len(px.peers)
			argee := 0
			noReply := 0
			// prepare
			//fmt.Printf("%v start prepare, arg: %v\n", px.me, s)

			// replace seq to a globally unique epoch
			t := time.Now().UnixNano()
			seq := strconv.Itoa(s) + "-" + strconv.FormatInt(t, 10)

			for i, peer := range px.peers {
				arg := PrepareArg{seq}
				reply := PrepareReply{}
				var ok bool
				if i == px.me {
					px.Prepare(arg, &reply)
					//fmt.Printf("%v prepare, arg: seq %v,  result %v\n", i, arg.Seq, reply.Result)
					ok = true
				} else {
					ok = call(peer, "Paxos.Prepare", arg, &reply)
					//fmt.Printf("%v prepare, arg: seq %v,  result %v\n", i, arg.Seq, reply.Result)
				}
				if !ok {
					noReply++
					//fmt.Printf("%v fail to get one reply from %v\n", px.me, i)
				}

				if reply.Result {
					argee++
					if reply.Seq > seq {
						v = reply.V
					}
				}

			}
			if noReply > length/2 {
				//fmt.Printf("%v fail to get %v reply\n", px.me, noReply)
				time.Sleep(2 * time.Second)
				continue
			}
			if argee <= length/2 {
				//fmt.Printf("%v fail to get major agree, seq %v with %v agree\n", px.me, seq, argee)
				arg := UpdateArg{s}
				for i, peer := range px.peers {
					if i == px.me {
						px.Update(arg, new(struct{}))
					} else {
						call(peer, "Paxos.Update", arg, new(struct{}))
					}
				}
				return
			}

			argee = 0
			noReply = 0
			// accept
			//fmt.Printf("%v start accept, arg: seq %v v %v\n", px.me, seq, v)
			for i, peer := range px.peers {
				arg := AcceptArg{seq, v}
				arg.Seq = seq
				reply := AcceptReply{}
				var ok bool

				if i == px.me {
					px.Accept(arg, &reply)
					//fmt.Printf("%v accept, arg: seq %v v %v, result %v\n", i, arg.Seq, arg.V, reply.Result)
					ok = true
				} else {
					ok = call(peer, "Paxos.Accept", arg, &reply)
					//fmt.Printf("%v accept, arg: seq %v v %v, result %v\n", i, arg.Seq, arg.V, reply.Result)
				}
				if !ok {
					noReply++
				}
				if reply.Result {
					argee++
				}
			}
			if noReply > length/2 {
				time.Sleep(2 * time.Second)
				continue
			}
			if argee <= length/2 {
				arg := UpdateArg{s}
				for i, peer := range px.peers {
					if i == px.me {
						px.Update(arg, new(struct{}))
					} else {
						call(peer, "Paxos.Update", arg, new(struct{}))
					}
				}
				return
			}

			//decide
			//fmt.Printf("%v start decide\n", px.me)
			for i, peer := range px.peers {
				arg := DecideArg{v, s}
				if i == px.me {
					px.Decide(arg, new(struct{}))
				} else {
					call(peer, "Paxos.Decide", arg, new(struct{}))
				}
			}
			return
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.doneSeq = seq
}

func (px *Paxos) getSeq(seq string) int {
	s, _ := strconv.Atoi(strings.Split(seq, "-")[0])
	return s
}

// Prepare function, used by acceptor
func (px *Paxos) Prepare(arg PrepareArg, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	//fmt.Printf("paxos %v prepare, seq: %v with prepare seq %v\n", px.me, arg.Seq, px.prepareSeq)

	if arg.Seq < px.prepareSeq && len(arg.Seq) <= len(px.prepareSeq) {
		//fmt.Printf("paxos %v prepare, seq: %v, fail with %v\n", px.me, arg.Seq, px.prepareSeq)
		reply.Result = false
		// px.status[px.getSeq(arg.Seq)] = Decided
	} else {
		//fmt.Printf("paxos %v prepare, seq: %v, success\n", px.me, arg.Seq)
		px.prepareSeq = arg.Seq
		reply.Result = true
		reply.Seq = px.acceptSeq
		reply.V = px.v
	}

	return nil
}

// Accept function, used by acceptor
func (px *Paxos) Accept(arg AcceptArg, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	//fmt.Printf("paxos %v accept, seq: %v, value: %v\n with prepare seq %v", px.me, arg.Seq, arg.V, px.prepareSeq)

	if arg.Seq >= px.prepareSeq || len(arg.Seq) > len(px.prepareSeq) {
		px.acceptSeq = arg.Seq
		px.v = arg.V
		px.prepareSeq = arg.Seq

		reply.Result = true
		reply.Seq = arg.Seq
	} else {
		reply.Result = false
		px.status[px.getSeq(arg.Seq)] = Decided
	}
	return nil
}

// Decide function is used by proposer
func (px *Paxos) Decide(arg DecideArg, _ *struct{}) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	//fmt.Printf("paxos %v decide, seq: %v, value: %v\n", px.me, arg.Seq, arg.V)

	px.values[arg.Seq] = arg.V
	px.status[arg.Seq] = Decided
	return nil
}

func (px *Paxos) Update(arg UpdateArg, _ *struct{}) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.status[arg.Seq] = Decided
	return nil
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.getSeq(px.prepareSeq)
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	//reply := DoneReply{}
	//px.GetDone(*new(struct{}), &reply)
	min := px.doneSeq
	for i, peer := range px.peers {
		if i != px.me {
			reply := DoneReply{}
			result := call(peer, "Paxos.GetDone", *new(struct{}), &reply)
			if result {
				if min > reply.Seq {
					min = reply.Seq
				}
			} else {
				return px.minSeq + 1
			}
		}
	}

	for i, peer := range px.peers {
		arg := ForgetArg{min}
		if i != px.me {
			call(peer, "Paxos.Forget", arg, new(struct{}))
		} else {
			px.Forget(arg, new(struct{}))
		}
	}

	return min + 1
}

// GetDone : used to get min done seq
func (px *Paxos) GetDone(_ struct{}, reply *DoneReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// min, _ := strconv.Atoi(strings.Split(px.minSeq, "-")[0])
	reply.Seq = px.doneSeq
	return nil
}

// Forget : release all record whose seq < arg.seq
func (px *Paxos) Forget(arg ForgetArg, _ *struct{}) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	for k := range px.values {
		if k <= arg.Seq {
			delete(px.values, k)
		}
	}
	px.minSeq = arg.Seq
	return nil
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.minSeq {
		return Forgotten, nil
	} else if v, ok := px.status[seq]; ok {
		return Decided, v
	} else {
		return Pending, nil
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	//fmt.Printf("init paxos %v\n", me)
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.prepareSeq = ""
	px.acceptSeq = ""
	px.v = nil
	px.values = make(map[int]interface{})
	px.tsToSeq = make(map[int]string)
	px.minSeq = -1
	px.doneSeq = -1
	px.status = make(map[int]Fate)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
