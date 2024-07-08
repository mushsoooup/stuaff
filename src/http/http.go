package http

import (
	"bufio"
	"bytes"
	"strconv"
	"strings"
)

func StatusMessage(status int64) string {
	switch status {
	case 200:
		return " OK"
	case 404:
		return " Not Found"
	case 501:
		return " Not Implemented"
	case 503:
		return " Service Unavailable"
	default:
		return " Unknown"
	}
}

// Helper function to send back response
func FormatResponse(data []byte, status int64, headers ...string) []byte {
	result := make([]byte, 0, len(data))
	result = append(result, []byte("HTTP/1.1 ")...)
	result = strconv.AppendInt(result, status, 10)
	result = append(result, StatusMessage(status)...)
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

type Request struct {
	method  string
	path    string
	params  map[string]string
	headers map[string]string
	data    []byte
}

func (r *Request) Reset() {
	r.method = ""
	r.path = ""
	r.params = make(map[string]string)
	r.headers = make(map[string]string)
	r.data = r.data[:0]
}

func (r *Request) SetMethod(method []byte) {
	r.method = string(method)
}
func (r *Request) SetPath(path []byte) {
	idx := bytes.IndexByte(path, '?')
	if idx == -1 {
		r.path = string(path)
		return
	}
	r.path = string(path[:idx])
	params := bytes.Split(path[idx+1:], []byte{'&'})
	if r.params == nil {
		r.params = make(map[string]string)
	}
	for _, param := range params {
		idx := bytes.IndexByte(param, '=')
		if idx == -1 {
			continue
		}
		r.params[string(param[:idx])] = string(param[idx+1:])
	}
}
func (r *Request) SetData(data []byte) {
	r.data = append(r.data[:0], data...)
}
func (r *Request) AddHeader(key, val string) {
	if r.headers == nil {
		r.headers = make(map[string]string)
	}
	r.headers[key] = val
}
func (r *Request) GetMethod() string {
	return string(r.method)
}
func (r *Request) GetPath() string {
	return string(r.path)
}
func (r *Request) GetParam(key string) string {
	return r.params[key]
}
func (r *Request) GetData() []byte {
	return r.data
}
func (r *Request) GetHeader(key string) string {
	return r.headers[key]
}

type Response struct {
	headers     map[string]string
	data        []byte
	status      int64
	contentType string
}

func (r *Response) Reset() {
	r.headers = make(map[string]string)
	r.data = r.data[:0]
	r.status = 0
	r.contentType = ""
}
func (r *Response) SetContentType(contentType string) {
	r.contentType = contentType
}

func (r *Response) AddHeader(key, val string) {
	if r.headers == nil {
		r.headers = make(map[string]string)
	}
	r.headers[key] = val
}

func (r *Response) SetData(data []byte, status int) {
	r.status = int64(status)
	r.data = data
}

func (r *Response) Write(writer *bufio.Writer) error {
	if r.status == 0 {
		r.status = 200
	}
	if r.contentType == "" {
		r.contentType = "text/plain"
	}
	r.AddHeader("Content-Length", strconv.FormatInt(int64(len(r.data)), 10))
	r.AddHeader("Content-Type", r.contentType)
	writer.Write([]byte("HTTP/1.1 "))
	writer.Write([]byte(strconv.Itoa(int(r.status))))
	writer.Write([]byte(StatusMessage(r.status)))
	writer.Write([]byte("\r\nServer: GO SERVER\r\n"))
	for key, val := range r.headers {
		writer.Write([]byte(key + ": " + val + "\r\n"))
	}
	writer.Write([]byte("\r\n"))
	writer.Write(r.data)
	return writer.Flush()
}

func (r *Request) Prepare(method string) []byte {
	b := strings.Builder{}
	b.WriteString(method + " " + r.path)
	first := true
	// Build param list
	for key, val := range r.params {
		if first {
			b.WriteByte('?')
		} else {
			b.WriteByte('&')
		}
		b.WriteString(key + "=" + val)
	}
	b.WriteString(" HTTP/1.1\r\n")
	// Build headers
	for key, val := range r.headers {
		b.WriteString(key + ": " + val + "\r\n")
	}
	b.WriteString("\r\n")
	b.Write(r.data)
	return []byte(b.String())
}
