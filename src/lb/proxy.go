package lb

import (
	"bufio"
	"net"
	"time"
)

type Proxy struct {
	s *Server
}

func (p *Proxy) Serve(addr string, proxy ...string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}
	server := Server{}
	p.s = &server
	server.RegisterHandler(p.handler)
	server.Serve(ln, proxy...)
}

func (p *Proxy) handler(ctx *RequestCtx) error {
	// Modify host
	ctx.Req.AddHeader("Host", ctx.proxy.RemoteAddr().String())
	// Send to proxy
	err := ctx.proxy.SetWriteDeadline(time.Now().Add(idleTimeout))
	if err != nil {
		return err
	}
	_, err = ctx.proxy.Write(ctx.Req.Prepare(ctx.Req.GetMethod()))
	if err != nil {
		return err
	}
	return nil
}

func redirectProxy(c, proxy net.Conn) {
	// Redirect all responses to client
	r := bufio.NewReader(proxy)
	for {
		_, err := r.Peek(1)
		if err != nil {
			break
		}
		data, err := r.Peek(r.Buffered())
		if err != nil {
			break
		}
		_, err = c.Write(data)
		if err != nil {
			break
		}
	}
	// Close ctx.c so the other handler exits too
	c.Close()
}
