package lb

import (
	"bufio"
	"bytes"
	"cs/http"
	"errors"
	"net"
	"strconv"
)

var errNeedMore = errors.New("error need more")

type RequestCtx struct {
	c     net.Conn
	proxy net.Conn
	s     *Server
	Req   http.Request
	Res   http.Response
}

func (ctx *RequestCtx) parseData(r *bufio.Reader) error {
	n := 1
	for {
		err := ctx.tryRead(r, n)
		if err == nil {
			break
		}
		if err != errNeedMore {
			return err
		}
		n = r.Buffered() + 1
	}
	// If there is body
	length, err := strconv.Atoi(ctx.Req.GetHeader("Content-Length"))
	if err == nil {
		b, err := r.Peek(length)
		if err != nil {
			return err
		}
		ctx.Req.SetData(b)
	}
	return nil
}

func (ctx *RequestCtx) tryRead(r *bufio.Reader, n int) error {
	// Wait for next package
	_, err := r.Peek(n)
	if err != nil {
		return err
	}
	// Read full buffer
	b, err := r.Peek(r.Buffered())
	if err != nil {
		return err
	}
	firstAdvance := ctx.parseFirstLine(b)
	if firstAdvance == -1 {
		return errNeedMore
	} else if firstAdvance == -2 {
		return errors.New("error reading first line")
	}
	headerAdvance := ctx.parseHeader(b[firstAdvance:])
	if firstAdvance == -1 {
		return errNeedMore
	} else if firstAdvance == -2 {
		return errors.New("error reading header")
	}
	r.Discard(headerAdvance + firstAdvance)
	return nil
}

var strHTTP11 = []byte("HTTP/1.1")
var strCLRF = []byte("\r\n")

func (r *RequestCtx) parseFirstLine(data []byte) int {
	// method
	idx := bytes.IndexByte(data, ' ')
	if idx == -1 {
		return -1
	}
	r.Req.SetMethod(data[:idx])
	d := data[idx+1:]
	// uri
	idx = bytes.IndexByte(d, ' ')
	if idx == -1 {
		return -1
	}
	r.Req.SetPath(d[:idx])
	d = d[idx+1:]
	// protocol
	idx = bytes.Index(d, strCLRF)
	if idx == -1 {
		return -1
	}
	proto := d[:idx]
	if !bytes.Equal(strHTTP11, proto) {
		return -2
	}
	d = d[idx+2:]
	return len(data) - len(d)
}

func (ctx *RequestCtx) parseHeader(data []byte) int {
	var idx int
	var idx2 int
	d := data
	for {
		idx = bytes.IndexByte(d, ':')
		if idx == -1 {
			return -1
		}
		idx2 = bytes.Index(d[idx+1:], strCLRF)
		if idx == -1 {
			return -1
		}
		ctx.Req.AddHeader(string(d[:idx]), string(d[idx+2:idx+1+idx2]))
		d = d[idx+idx2+3:]
		if bytes.Equal(d[:2], strCLRF) {
			break
		}
	}
	return len(data) - len(d) + 2
}

func (ctx *RequestCtx) Reset() {
	// Close() of ctx.c is handled by pool
	ctx.c = nil
	ctx.s = nil
	if ctx.proxy != nil {
		ctx.proxy.Close()
		ctx.proxy = nil
	}
	ctx.Res.Reset()
}
