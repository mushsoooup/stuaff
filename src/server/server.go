package server

import (
	"bufio"
	"cs/http"
	"cs/logger"
	"cs/pool"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type HandlerFunc func(*RequestCtx) error

type Server struct {
	p          pool.Pool
	ctxPool    sync.Pool
	readerPool sync.Pool
	writerPool sync.Pool
	handler    HandlerFunc
	proxy      string
}

var unavailable = http.FormatResponse([]byte("503 Service Unavailable"), 503, "Content-type: text/plain")

var idleTimeout = 1 * time.Second

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
		s.proxy = p
	}

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

func (s *Server) serveConn(c net.Conn) (err error) {
	ctx := s.acquireCtx(c)
	if s.proxy != "" {
		ctx.proxy, err = net.Dial("tcp", s.proxy)
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
		} else if s.proxy == "" {
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
