package pool

import (
	"cs/logger"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const maxIdle = time.Second * 2

type ConnHandler func(net.Conn) error

// Basic goroutine pool struct
type Pool struct {
	workerFunc ConnHandler
	lock       sync.Mutex

	concurrency    int
	ready          []*workerChan
	workersCnt     int
	workerChanPool sync.Pool
}

type workerChan struct {
	lastUsed time.Time
	ch       chan net.Conn
}

// get tries to abtain a workerChan to throw job to
// fails when cocurrently running goroutines reach maximum
func (p *Pool) get() *workerChan {
	var ch *workerChan

	new := false

	// First try to use existing idle worker
	p.lock.Lock()
	ready := p.ready
	n := len(ready) - 1
	if n < 0 {
		if p.workersCnt < p.concurrency {
			new = true
			p.workersCnt++
		}
	} else {
		ch = ready[n]
		ready[n] = nil
		p.ready = ready[:n]
	}
	p.lock.Unlock()

	// If no idle worker and we can create new worker
	if ch == nil && new {
		vch := p.workerChanPool.Get()
		ch = vch.(*workerChan)
		go func() {
			p.worker(ch)
			p.workerChanPool.Put(vch)
		}()
	}

	return ch
}

func (p *Pool) worker(ch *workerChan) {
	var c net.Conn
	var err error
	for c = range ch.ch {
		if c == nil {
			break
		}
		// connection reset by peer is normal
		if err = p.workerFunc(c); err != nil && err != io.EOF &&
			!strings.Contains(err.Error(), "reset by peer") &&
			!strings.Contains(err.Error(), "i/o timeout") {
			logger.Debug("error serving %q<->%q: %v", c.LocalAddr(), c.RemoteAddr(), err)
		}
		_ = c.Close()
		ch.lastUsed = time.Now()
		p.lock.Lock()
		p.ready = append(p.ready, ch)
		p.lock.Unlock()
	}
	p.lock.Lock()
	p.workersCnt--
	p.lock.Unlock()
}

// Start starts a Pool using handler and sets concurrency
func (p *Pool) Start(handler ConnHandler, concurrency int) {
	p.workerFunc = handler
	p.concurrency = concurrency
	p.workerChanPool.New = func() any {
		return &workerChan{
			ch: make(chan net.Conn, 1),
		}
	}
	// Periodically clean unused worker
	go func() {
		var scratch []*workerChan
		for {
			p.clean(&scratch)
			time.Sleep(maxIdle)
		}
	}()
}

// Serve serves a connection
func (p *Pool) Serve(c net.Conn) bool {
	ch := p.get()
	if ch == nil {
		return false
	}
	ch.ch <- c
	return true
}

func (p *Pool) clean(scratch *[]*workerChan) {
	criticalTime := time.Now().Add(maxIdle)

	p.lock.Lock()
	ready := p.ready
	n := len(ready)

	// Binary search
	l, r := 0, n-1
	for l <= r {
		mid := (l + r) / 2
		if criticalTime.After(p.ready[mid].lastUsed) {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	i := r
	if i == -1 {
		p.lock.Unlock()
		return
	}
	*scratch = append((*scratch)[:0], ready[:i+1]...)
	m := copy(ready, ready[i+1:])
	for i = m; i < n; i++ {
		ready[i] = nil
	}
	p.ready = ready[:m]
	p.lock.Unlock()

	tmp := *scratch
	for i := range tmp {
		tmp[i].ch <- nil
		tmp[i] = nil
	}
}
