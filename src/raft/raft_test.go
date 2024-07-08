package raft

import (
	"cs/config"
	"os"
	"testing"
	"time"
)

func TestElection(t *testing.T) {
	confs, err := makeConf()
	if err != nil {
		t.Fatalf("error making config %v", err)
	}
	peers := make([]Raft, len(confs))
	for idx := range peers {
		addrs := config.ReadConfig(confs[idx])
		go peers[idx].Make(addrs, nil, nil)
	}
	leader := checkOneLeader(peers)
	if leader == -1 {
		t.Fatalf("error no leader elected")
	} else if leader == -2 {
		t.Fatalf("error multiple leader")
	}
	time.Sleep(2 * electionTimeout)
	leader2 := checkOneLeader(peers)
	if leader2 != leader {
		t.Fatalf("leadership change when no error occurs")
	}
}

func TestRelection(t *testing.T) {
	confs, err := makeConf()
	if err != nil {
		t.Fatalf("error making config %v", err)
	}
	peers := make([]Raft, len(confs))
	for idx := range peers {
		addrs := config.ReadConfig(confs[idx])
		go peers[idx].Make(addrs, nil, nil)
	}
	leader := checkOneLeader(peers)
	if leader == -1 {
		t.Fatalf("error no leader elected")
	} else if leader == -2 {
		t.Fatalf("error multiple leader")
	}
	// Take down leader
	peers[leader].Shutdown()
	leader = checkOneLeader(peers)
	if leader == -1 {
		t.Fatalf("error no leader elected")
	} else if leader == -2 {
		t.Fatalf("error multiple leader")
	}
}

func TestSubmit(t *testing.T) {
	confs, err := makeConf()
	if err != nil {
		t.Fatalf("error making config %v", err)
	}
	peers := make([]Raft, len(confs))
	for idx := range peers {
		addrs := config.ReadConfig(confs[idx])
		applyChan := make(chan Packet)
		go peers[idx].Make(addrs, nil, applyChan)
		go func() {
			for {
				<-applyChan
			}
		}()
	}
	leader := checkOneLeader(peers)
	if leader == -1 {
		t.Fatalf("error no leader elected")
	} else if leader == -2 {
		t.Fatalf("error multiple leader")
	}
	// Partial failure
	takeDown := (leader + 1) % len(confs)
	peers[takeDown].Shutdown()
	peers[leader].Submit("100")
	time.Sleep(electionTimeout)
	for idx := range peers {
		if idx == takeDown {
			continue
		}
		if _, _, _, applied := peers[idx].Status(); applied != 1 {
			t.Fatalf("error apply command")
		}
	}
}

func makeConf() ([]string, error) {
	confs := []string{"./.follower1.conf", "./.follower2.conf", "./.follower3.conf"}
	addrs := []string{"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"}
	for idx, each := range confs {
		os.Remove(each)
		file, err := os.Create(each)
		if err != nil {
			return nil, err
		}
		file.WriteString("follower_info " + addrs[idx] + "\n")
		for i := range addrs {
			if idx == i {
				continue
			}
			file.WriteString("follower_info " + addrs[i] + "\n")
		}
	}
	return confs, nil
}

func checkOneLeader(peers []Raft) int {
	leader := -1
	for range 10 {
		time.Sleep(electionTimeout)
		for idx, each := range peers {
			if each.state == stateLeader && !each.dead {
				if leader != -1 {
					leader = -2
					break
				} else {
					leader = idx
				}
			}
		}
		if leader != -1 {
			break
		}
	}
	return leader
}
