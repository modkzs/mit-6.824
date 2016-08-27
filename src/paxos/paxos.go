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

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

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
	dead       int32               // for testing
	unreliable int32               // for testing
	rpcCount   int32               // for testing
	peers      []string

	me         int                 // index into peers[]
	acceptSeq  int                 // current accept seq
	v          interface{}         // current accept value
	prepareSeq int                 // current prepare seq
	values     map[int]interface{} // all history value
	doneSeq    int                 // used in Done
	minSeq     int                 // seq already forget
	tsToSeq    map[int]int         // a map from seq to ts, which is unique globally
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
	Seq int
}

// PrepareReply is used in Paxos.Prepare function
type PrepareReply struct {
	Seq    int
	V      interface{}
	Result bool
}

// AcceptArg is used in Paxos.Accept function
type AcceptArg struct {
	Seq int
	V   interface{}
}

// AcceptReply is used in Paxos.Accept function
type AcceptReply struct {
	Seq    int
	Result bool
}

// DecideArg is used in Paxos.Decide function
type DecideArg struct {
	V   interface{}
	Seq int
}

// ForgetArg used in Paxos.Forget function
type ForgetArg PrepareArg

// DoneReply used in Paxos.GetDone function
type DoneReply PrepareArg

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		length := len(px.peers)
		argee := 0
		// prepare
		fmt.Printf("%v start prepare\n", px.me)

		// replace seq to a globally unique epoch

		for i, peer := range px.peers {
			arg := PrepareArg{seq}
			reply := PrepareReply{}
			if i == px.me {
				px.Prepare(arg, &reply)
				continue
			} else {
				call(peer, "Paxos.Prepare", arg, &reply)
				fmt.Printf("%v prepare, arg: seq %v,  result %v\n", i, arg.Seq, reply.Result)
			}
			if reply.Result {
				argee++
			}
			if reply.Seq > seq {
				v = reply.V
			}
		}
		if argee <= length/2 {
			fmt.Printf("paxos %v fail to get major agree, seq %v\n", px.me, seq)
			return
		}

		argee = 0
		// accept
		fmt.Printf("%v start accept\n", px.me)
		for i, peer := range px.peers {
			arg := AcceptArg{seq, v}
			arg.Seq = seq
			reply := AcceptReply{}

			if i == px.me {
				px.Accept(arg, &reply)
			} else {
				call(peer, "Paxos.Accept", arg, &reply)
				fmt.Printf("%v accept, arg: seq %v v %v, result %v\n", i, arg.Seq, arg.V, reply.Result)
			}
			if reply.Result {
				argee++
			}
		}
		if argee <= length/2 {
			return
		}

		//decide
		fmt.Printf("%v start decide\n", px.me)
		for i, peer := range px.peers {
			arg := DecideArg{v, seq}
			if i == px.me {
				px.Decide(arg, new(struct{}))
			} else {
				call(peer, "Paxos.Decide", arg, new(struct{}))
			}
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

// Prepare function, used by acceptor
func (px *Paxos) Prepare(arg PrepareArg, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	fmt.Printf("paxos %v prepare, seq: %v with prepare seq %v\n", px.me, arg.Seq, px.prepareSeq)

	if arg.Seq < px.prepareSeq {
		fmt.Printf("paxos %v prepare, seq: %v, fail with %v\n", px.me, arg.Seq, px.prepareSeq)
		reply.Result = false
	} else {
		fmt.Printf("paxos %v prepare, seq: %v, success\n", px.me, arg.Seq)
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

	fmt.Printf("paxos %v accept, seq: %v, value: %v\n", px.me, arg.Seq, arg.V)

	if arg.Seq >= px.prepareSeq {
		px.acceptSeq = arg.Seq
		px.v = arg.V
		px.prepareSeq = arg.Seq

		reply.Result = true
		reply.Seq = arg.Seq
	} else {
		reply.Result = false
	}
	return nil
}

// Decide function is used by proposer
func (px *Paxos) Decide(arg DecideArg, _ *struct{}) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	fmt.Printf("paxos %d decide, seq: %d, value: %s\n", px.me, arg.Seq, arg.V)

	px.values[arg.Seq] = arg.V
	return nil
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.prepareSeq
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
	min := px.doneSeq
	for i, peer := range px.peers {
		if i != px.me {
			reply := DoneReply{}
			result := call(peer, "Paxos.GetDone", new(struct{}), &reply)
			if result {
				if min > reply.Seq {
					min = reply.Seq
				}
			} else {
				return px.minSeq
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

	return min
}

// GetDone : used to get min done seq
func (px *Paxos) GetDone(_ struct{}, reply *DoneReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

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
	} else if v, ok := px.values[seq]; ok {
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
	fmt.Printf("init paxos %v\n", me)
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.prepareSeq = -1
	px.acceptSeq = -1
	px.v = nil
	px.values = make(map[int]interface{})
	px.tsToSeq = make(map[int]int)
	px.minSeq = -1
	px.doneSeq = -1

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
