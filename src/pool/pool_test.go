package pool

import (
	"net"
	"testing"
	"time"
)

func TestPoolExaust(t *testing.T) {
	p := Pool{}
	p.Start(func(c net.Conn) error {
		time.Sleep(2 * time.Second)
		return nil
	}, 1024)
	for i := 0; i < 1024; i++ {
		if !p.Serve(&net.TCPConn{}) {
			t.Fatalf("error calling Serve when limit not exceeded.")
		}
	}
	p.lock.Lock()
	if p.workersCnt != 1024 {
		t.Fatalf("no enough worker.")
	}
	p.lock.Unlock()
	if p.Serve(&net.TCPConn{}) {
		t.Fatalf("error calling Serve when limit exceeded.")
	}
	time.Sleep(6 * time.Second)
	p.lock.Lock()
	if p.workersCnt != 0 {
		t.Fatalf("worker not cleaned after idle timeout.")
	}
	p.lock.Unlock()
}
