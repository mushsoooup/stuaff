package raft

import (
	"log"
	"net/rpc"
)

type peer struct {
	id       int
	addr     string
	endpoint string
	conn     *rpc.Client
}

func (r *Raft) makePeer(addr, endpoint string) {
	r.peers = append(r.peers, peer{id: r.generateId(addr), addr: addr, endpoint: endpoint})
}

func (p *peer) call(f string, args, reply any) bool {
	if p.conn == nil {
		var err error
		p.conn, err = rpc.Dial("tcp", p.addr)
		if err != nil {
			log.Printf("error dialing rpc %v\n", err)
			return false
		}
	}
	err := p.conn.Call(f, args, reply)
	if err != nil {
		log.Printf("error calling rpc %v\n", err)
		p.conn.Close()
		p.conn = nil
		return false
	}
	return true
}

func (p *peer) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return p.call("Raft.AppendEntriesRPC", args, reply)
}

func (p *peer) requestVote(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return p.call("Raft.RequestVoteRPC", args, reply)
}
