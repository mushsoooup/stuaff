package raft

import (
	"cs/logger"
	"hash/fnv"
	"math/rand"
	"net/rpc"
	"slices"
	"time"
)

const (
	cmdTicker = iota
	cmdRequestVote
	cmdAppendEntries
	cmdGatherVote
	cmdSendAppendEntries
	cmdSendAppendEntriesReply
	cmdSubmit
	cmdStatus
	cmdShutdown
)

const (
	stateCandidate = iota
	stateFollower
	stateLeader
)

const (
	tickerPeriod    = 50 * time.Millisecond
	electionTimeout = 1000 * time.Millisecond
)

type Entry struct {
	Term    int
	Command any
}

type cmd struct {
	method int
	args   any
	reply  any
	done   chan struct{}
}

// Packet sent to upper service
type Packet struct {
	Term    int
	Index   int
	Command any
}

type Raft struct {
	id       int
	leaderId int
	state    int
	peers    []peer
	server   *rpc.Server
	cmdCh    chan cmd
	dead     bool
	applyCh  chan Packet
	// state
	currentTerm int
	votedFor    int
	votes       int
	log         []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	heartbeat   time.Time
}

func (r *Raft) Debug(format string, args ...any) {
	args = append([]any{r.id}, args...)
	logger.Debug("[%v] "+format+"\n", args...)
}

func (r *Raft) Make(addrs, endpoints []string, applyCh chan Packet) {
	if endpoints == nil {
		endpoints = make([]string, len(addrs))
	}
	// Generate id
	r.id = r.generateId(addrs[0])
	// Initialize self
	r.Debug("starting server @%v endpoint @%v", addrs[0], endpoints[0])
	r.serveRPC(addrs[0])
	for idx, peer := range addrs[1:] {
		r.makePeer(peer, endpoints[1:][idx])
	}
	// Initialize state
	r.dead = false
	r.state = stateFollower
	r.currentTerm = 0
	r.votedFor = -1
	r.commitIndex = 0
	r.lastApplied = 0
	r.nextIndex = make([]int, len(r.peers))
	r.matchIndex = make([]int, len(r.peers))
	r.heartbeat = time.Now()
	r.cmdCh = make(chan cmd)
	r.applyCh = applyCh

	go r.ticker()
	r.run()
}

func (r *Raft) run() {
loop:
	for {
		cmd := <-r.cmdCh
		switch cmd.method {
		case cmdTicker:
			r.checkTimeout()
		case cmdRequestVote:
			r.RequestVote(cmd.args.(*RequestVoteArgs), cmd.reply.(*RequestVoteReply))
		case cmdAppendEntries:
			r.AppendEntries(cmd.args.(*AppendEntriesArgs), cmd.reply.(*AppendEntriesReply))
		case cmdGatherVote:
			r.gatherVote(cmd.args.(*RequestVoteReply))
		case cmdSendAppendEntries:
			r.sendAppendEntries(cmd.args.(*sendAppendEntriesArgs))
		case cmdSendAppendEntriesReply:
			r.sendAappendEntriesReply(cmd.args.(*sendAappendEntriesReply))
		case cmdSubmit:
			r.submit(cmd.args.(*submission), cmd.reply.(*submissionReply))
		case cmdStatus:
			r.status(cmd.reply.(*statusReply))
		case cmdShutdown:
			r.dead = true
			cmd.done <- struct{}{}
			break loop
		}
		cmd.done <- struct{}{}
	}
}

type statusReply struct {
	currentTerm int
	leaderId    int
	length      int
	applied     int
}

func (r *Raft) status(reply *statusReply) {
	reply.currentTerm = r.currentTerm
	reply.leaderId = r.leaderId
	reply.length = len(r.log)
	reply.applied = r.lastApplied
}

func (r *Raft) Status() (currentTerm, leaderId, length, applied int) {
	done := make(chan struct{}, 1)
	reply := &statusReply{}
	r.cmdCh <- cmd{
		method: cmdStatus,
		args:   nil,
		reply:  reply,
		done:   done,
	}
	<-done
	return reply.currentTerm, reply.leaderId, reply.length, reply.applied
}

// CAUTION!!!
// This version of generateId is NOT safe as it might collide with other peer
// Remove mod operation to get safer result
func (r *Raft) generateId(addr string) int {
	f := fnv.New32a()
	f.Write([]byte(addr))
	id := int(f.Sum32()) % 1000
	if id < 0 {
		return -id
	}
	return id
}

// Check for election timeout periodically
func (r *Raft) ticker() {
	done := make(chan struct{}, 1)
	for {
		// Add some randomness
		time.Sleep(tickerPeriod + time.Duration(rand.Int()%45)*time.Millisecond)
		r.cmdCh <- cmd{
			method: cmdTicker,
			args:   nil,
			reply:  nil,
			done:   done,
		}
		<-done
	}
}

func (r *Raft) checkTimeout() {
	if time.Now().After(r.heartbeat.Add(electionTimeout+time.Duration(rand.Int()%1000)*time.Millisecond)) && r.state != stateLeader && !r.dead {
		r.election()
	}
}

type sendAppendEntriesArgs struct {
	idx    int
	notify chan bool
}

func (r *Raft) sendAppendEntries(args *sendAppendEntriesArgs) {
	// Prepare args
	prevLogIndex := r.nextIndex[args.idx] - 1
	prevLogTerm := 0
	if prevLogIndex < 0 {
		prevLogIndex = 0
	}
	if prevLogIndex != 0 {
		prevLogTerm = r.log[prevLogIndex-1].Term
	}
	entries := append([]Entry{}, r.log[prevLogIndex:]...)
	appendEntriesArgs := &AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.lastApplied,
	}
	go func() {
		reply := &AppendEntriesReply{}
		r.peers[args.idx].appendEntries(appendEntriesArgs, reply)
		// Notify syncPeer
		r.cmdCh <- cmd{
			method: cmdSendAppendEntriesReply,
			args: &sendAappendEntriesReply{
				reply:  reply,
				idx:    args.idx,
				target: prevLogIndex + len(entries),
				notify: args.notify,
			},
			reply: nil,
			done:  make(chan struct{}, 1),
		}
	}()
}

type sendAappendEntriesReply struct {
	reply  *AppendEntriesReply
	idx    int
	target int
	notify chan bool
}

func (r *Raft) sendAappendEntriesReply(args *sendAappendEntriesReply) {
	r.checkTerm(args.reply.Term)
	if r.state != stateLeader {
		close(args.notify)
		return
	}
	// Sync nextIndex and matchIndex
	if args.reply.Success {
		r.nextIndex[args.idx] = args.target + 1
		r.matchIndex[args.idx] = args.target
		// Try to increase commitIndex
		matches := append([]int{}, r.matchIndex...)
		slices.Sort(matches)
		r.commitIndex = max(r.commitIndex, matches[(len(r.peers)+1)/2])
		r.syncLog()
	} else {
		r.nextIndex[args.idx] = min(1, r.nextIndex[args.idx]-1)
	}
	args.notify <- args.reply.Success
}

// syncPeer also responsible for heartbeat
func (r *Raft) syncPeer(idx int) {
	done := make(chan struct{}, 1)
	notify := make(chan bool, 1)
	for {
		// Send AppendEntries
		r.cmdCh <- cmd{
			method: cmdSendAppendEntries,
			args: &sendAppendEntriesArgs{
				idx:    idx,
				notify: notify,
			},
			reply: nil,
			done:  done,
		}
		<-done
		// AppendEntries done, sleep for a while before next try if succeed
		succeed, ok := <-notify
		// Not leader anymore, quit sync
		if !ok {
			break
		}
		if succeed {
			time.Sleep(tickerPeriod)
		}
	}
}

func (r *Raft) shiftCandidate() {
	r.state = stateCandidate
	r.currentTerm++
	r.votedFor = r.id
	r.votes = 1
	r.heartbeat = time.Now()
	r.Debug("timeout, start election at term %v", r.currentTerm)
}

func (r *Raft) shiftFollower(term int) {
	r.state = stateFollower
	r.Debug("decay to follower at term %v -> %v", r.currentTerm, term)
	r.currentTerm = term
	r.votedFor = -1
}

func (r *Raft) shiftLeader() {
	r.Debug("is now the leader")
	r.state = stateLeader
	r.leaderId = r.id
	for idx := range r.peers {
		r.nextIndex[idx] = len(r.log) + 1
		r.matchIndex[idx] = 0
		go r.syncPeer(idx)
	}
}

func (r *Raft) gatherVote(reply *RequestVoteReply) {
	if reply.Term == r.currentTerm && reply.VoteGranted {
		r.votes++
	}
	if r.state == stateCandidate && r.votes > len(r.peers)/2 {
		r.shiftLeader()
	}
}

func (r *Raft) election() {
	// Shift to candidate
	r.shiftCandidate()
	// Prepare args
	// Get lastest log index and term
	lastLogIndex := len(r.log)
	lastLogTerm := 0
	if lastLogIndex != 0 {
		lastLogTerm = r.log[lastLogIndex-1].Term
	}
	args := &RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	for idx := range r.peers {
		go func(idx int) {
			reply := &RequestVoteReply{}
			// Ask for vote
			r.peers[idx].requestVote(args, reply)
			// Gather vote
			r.cmdCh <- cmd{
				method: cmdGatherVote,
				args:   reply,
				done:   make(chan struct{}, 1),
			}
		}(idx)
	}
}

func (r *Raft) checkTerm(term int) {
	if term > r.currentTerm {
		r.shiftFollower(term)
	}
}

func (r *Raft) Shutdown() {
	done := make(chan struct{}, 1)
	r.cmdCh <- cmd{
		method: cmdShutdown,
		args:   nil,
		reply:  nil,
		done:   done,
	}
	<-done
}

type submission struct {
	command any
}

type submissionReply struct {
	leaderAddr string
	term       int
	index      int
}

func (r *Raft) submit(args *submission, reply *submissionReply) {
	// If not leader, return leader address
	for _, peer := range r.peers {
		if peer.id == r.leaderId {
			reply.leaderAddr = peer.endpoint
			break
		}
	}
	if r.state != stateLeader {
		r.Debug("get submit but not the leader")
		return
	}
	// Append log
	r.log = append(r.log, Entry{
		Term:    r.currentTerm,
		Command: args.command,
	})
	reply.term = r.currentTerm
	reply.index = len(r.log)
}

// Submit tries to submit a command and hopefully apply it to the fsm
func (r *Raft) Submit(command any) (leaderAddr string, term, index int) {
	done := make(chan struct{}, 1)
	reply := &submissionReply{}
	r.cmdCh <- cmd{
		method: cmdSubmit,
		args: &submission{
			command: command,
		},
		reply: reply,
		done:  done,
	}
	<-done
	return reply.leaderAddr, reply.term, reply.index
}

func (r *Raft) syncLog() {
	for r.lastApplied < r.commitIndex {
		log := r.log[r.lastApplied]
		r.applyCh <- Packet{
			Term:    log.Term,
			Index:   r.lastApplied + 1,
			Command: log.Command,
		}
		r.lastApplied++
		r.Debug("applied log %v", r.lastApplied)
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Check for new term
	r.checkTerm(args.Term)
	// Reply false if term < currentTerm (§5.1)
	reply.Term = r.currentTerm
	reply.VoteGranted = false
	if args.Term < r.currentTerm {
		r.Debug("refuse to grant vote: old term")
		return
	}

	// If votedFor is null or candidateId
	// and candidate's log is at least as up-to-date as receiver's log, grant vote(§5.2, §5.4)
	// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date.

	// Supress election
	r.heartbeat = time.Now()

	// If vote already granted
	if r.votedFor != -1 && r.votedFor != args.CandidateId {
		r.Debug("refuse to grant vote: already granted to %v", r.votedFor)
		return
	}
	// Get latest log term
	lastLogTerm := 0
	if len(r.log) != 0 {
		lastLogTerm = r.log[len(r.log)-1].Term
	}

	// Candidate with older term
	if args.LastLogTerm < lastLogTerm {
		r.Debug("refuse to grant vote: old log term %v < %v", args.LastLogTerm, lastLogTerm)
		return
	} else if args.LastLogTerm > lastLogTerm ||
		// Candidate with newer term
		args.LastLogIndex >= len(r.log) {
		// Or at least same length
		r.Debug("grant vote to %v", args.CandidateId)
		r.votedFor = args.CandidateId
		reply.VoteGranted = true
		return
	}
	r.Debug("refuse to grant vote: old log index %v < %v", args.LastLogIndex, len(r.log))
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.checkTerm(args.Term)
	reply.Term = r.currentTerm
	reply.Success = false
	// Reply false if term < currentTerm (§5.1)
	if args.Term < r.currentTerm {
		r.Debug("refuse to append entries : old term %v < %v", args.Term, r.currentTerm)
		return
	}
	r.leaderId = args.LeaderId
	r.state = stateFollower
	r.heartbeat = time.Now()
	//  Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > len(r.log) ||
		(args.PrevLogIndex != 0 && r.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		r.Debug("refuse to append entries : mismatch entry at %v", args.PrevLogIndex)
		return
	}
	reply.Success = true
	// If an existing entry conflicts with a new one (same index but different terms)
	// delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	// If leaderCommit > commitIndex
	// set commitIndex = min(leaderCommit, index of last new entry)
	r.log = append(r.log[:args.PrevLogIndex], args.Entries...)
	if len(args.Entries) != 0 {
		r.Debug("sync log to %v", len(r.log))
	}
	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = min(args.LeaderCommit, len(r.log))
	}
	// Sync log to fsm
	r.syncLog()
}
