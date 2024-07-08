package lb

import (
	"bufio"
	"cs/logger"
	"cs/pool"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type HandlerFunc func(*RequestCtx) error

type web struct {
	addr  string
	valid bool
}

type Server struct {
	p          pool.Pool
	ctxPool    sync.Pool
	readerPool sync.Pool
	writerPool sync.Pool
	handler    HandlerFunc
	proxy      []web
	lastPorxy  int
	proxyLock  sync.Mutex
}

// Helper function to send back response
func FormatResponse(data []byte, status int64, headers ...string) []byte {
	result := make([]byte, 0, len(data))
	result = append(result, []byte("HTTP/1.1 ")...)
	result = strconv.AppendInt(result, status, 10)
	result = append(result, []byte(" Service Unavailable")...)
	result = append(result, []byte("\r\nServer: GO SERVER\r\n")...)
	for _, header := range headers {
		result = append(result, []byte(header)...)
		result = append(result, []byte("\r\n")...)
	}
	result = append(result, []byte("Content-Length: ")...)
	result = strconv.AppendInt(result, int64(len(data)), 10)
	result = append(result, []byte("\r\n\r\n")...)
	result = append(result, data...)
	return result
}

var unavailable = FormatResponse([]byte("503 Service Unavailable"), 503, "Content-type: text/plain")

var idleTimeout = 1 * time.Second

func (s *Server) checkProxy() {
	client := http.Client{}
	for {
		time.Sleep(1 * time.Second)
		s.proxyLock.Lock()
		for i := range s.proxy {
			addr := s.proxy[i].addr
			s.proxyLock.Unlock()
			client.Timeout = 50 * time.Millisecond
			_, err := client.Get("http://" + addr + "/ping")
			s.proxyLock.Lock()
			s.proxy[i].valid = true
			if err != nil {
				s.proxy[i].valid = false
			}
		}
		s.proxyLock.Unlock()
	}
}

// Serve starts the server
func (s *Server) Serve(ln net.Listener, proxy ...string) {
	s.ctxPool.New = func() any {
		return &RequestCtx{}
	}
	s.readerPool.New = func() any {
		return &bufio.Reader{}
	}
	s.writerPool.New = func() any {
		return &bufio.Writer{}
	}

	if len(proxy) != 0 {
		// Cut http://
		p := proxy[0]
		p = strings.TrimPrefix(p, "http://")
		if !strings.Contains(p, ":") {
			p = p + ":80"
		}
		logger.Debug("final proxy %v", p)
		for _, p := range proxy {
			s.proxy = append(s.proxy, web{
				addr:  p,
				valid: true,
			})
		}
	}

	go s.checkProxy()

	s.p.Start(s.serveConn, 256*1024)
	for {
		c, err := ln.Accept()
		if err != nil {
			logger.Debug("error establishing connection %v", err)
			continue
		}
		if !s.p.Serve(c) {
			c.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			c.Write(unavailable)
			c.Close()
		}
	}
}

func (s *Server) pickProxy() string {
	s.proxyLock.Lock()
	for i := range s.proxy {
		sel := (i + s.lastPorxy + 1) % len(s.proxy)
		sProxy := s.proxy[sel]
		if sProxy.valid {
			s.proxyLock.Unlock()
			logger.Debug("choose proxy %s", sProxy.addr)
			s.lastPorxy = sel
			return sProxy.addr
		}
	}
	s.proxyLock.Unlock()
	return ""
}

func (s *Server) serveConn(c net.Conn) (err error) {
	ctx := s.acquireCtx(c)
	if s.proxy != nil {
		sProxy := s.pickProxy()
		if sProxy == "" {
			return errors.New("no valid proxy")
		}
		ctx.proxy, err = net.Dial("tcp", sProxy)
		if err != nil {
			logger.Debug("failed to connect to proxy %v", s.proxy)
			s.releaseCtx(ctx)
			return err
		}
		go redirectProxy(ctx.c, ctx.proxy)
	}
	reader := s.acquireReader(ctx.c)
	writer := s.acquireWriter(ctx.c)
	// Supports http pipelining
	for {
		if err = c.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
			break
		}
		_, err = reader.Peek(1)
		if err != nil {
			break
		}
		err = ctx.parseData(reader)
		if err != nil {
			break
		}
		err = s.handler(ctx)
		if err != nil {
			c.Write(unavailable)
		} else if s.proxy == nil {
			c.SetWriteDeadline(time.Now().Add(idleTimeout))
			err = ctx.Res.Write(writer)
		}
		if err != nil {
			break
		}
		ctx.Req.Reset()
		ctx.Res.Reset()
	}
	s.releaseCtx(ctx)
	s.releaseReader(reader)
	s.releaseWriter(writer)
	return err
}

func (s *Server) acquireCtx(c net.Conn) *RequestCtx {
	ctx := s.ctxPool.Get().(*RequestCtx)
	ctx.s = s
	ctx.c = c
	return ctx
}
func (s *Server) releaseCtx(ctx *RequestCtx) {
	ctx.Reset()
	s.ctxPool.Put(ctx)
}

func (s *Server) acquireReader(reader io.Reader) *bufio.Reader {
	r := s.readerPool.Get().(*bufio.Reader)
	r.Reset(reader)
	return r
}

func (s *Server) acquireWriter(writer io.Writer) *bufio.Writer {
	w := s.writerPool.Get().(*bufio.Writer)
	w.Reset(writer)
	return w
}

func (s *Server) releaseReader(r *bufio.Reader) {
	s.readerPool.Put(r)
}

func (s *Server) releaseWriter(w *bufio.Writer) {
	s.writerPool.Put(w)
}

func (s *Server) RegisterHandler(handler HandlerFunc) {
	s.handler = handler
}
