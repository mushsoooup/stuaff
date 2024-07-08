package raft

import (
	"log"
	"net"
	"net/rpc"
)

func (r *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return r.processRPC(cmdAppendEntries, args, reply)
}

func (r *Raft) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return r.processRPC(cmdRequestVote, args, reply)
}

func (r *Raft) processRPC(method int, args, reply any) error {
	done := make(chan struct{}, 1)
	r.cmdCh <- cmd{
		method: method,
		args:   args,
		reply:  reply,
		done:   done,
	}
	<-done
	return nil
}

func (r *Raft) serveRPC(addr string) {
	r.server = rpc.NewServer()
	err := r.server.Register(r)
	if err != nil {
		log.Fatalf("error registering rpc server %v\n", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("error listening %v %v\n", addr, err)
	}
	go r.rpcHandler(ln)
}

func (r *Raft) rpcHandler(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatalf("error accepting connection %v\n", err)
		}
		go r.server.ServeConn(c)
	}
}
