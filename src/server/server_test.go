package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestRequestParse(t *testing.T) {
	s := Server{}
	errChan := make(chan string)
	s.RegisterHandler(func(rc *RequestCtx) error {
		method := rc.Req.GetMethod()
		path := rc.Req.GetPath()
		host := rc.Req.GetHeader("Host")
		hello := rc.Req.GetHeader("Hello")
		if method != "GET" {
			errChan <- fmt.Sprintf("wrong method %v", method)
		} else if path != "/index.html" {
			errChan <- fmt.Sprintf("wrong path %v", path)
		} else if host != "127.0.0.1:65501" {
			errChan <- fmt.Sprintf("wrong host %v", host)
		} else if hello != "World!" {
			errChan <- fmt.Sprintf("wrong hello %v", hello)
		}
		rc.Res.SetContentType("text/plain")
		rc.Res.SetData([]byte("this is a test"), 200)
		return nil
	})
	ln, err := net.Listen("tcp", ":65501")
	if err != nil {
		t.Fatalf("error listening port 65501 %v", err)
	}
	go s.Serve(ln)
	time.Sleep(1 * time.Second)
	client := http.Client{}
	req, err := http.NewRequest("GET", "http://127.0.0.1:65501/index.html", nil)
	if err != nil {
		t.Fatalf("error creating request %v", err)
	}
	req.Header.Add("Hello", "World!")
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("error sending request %v", err)
	}
	select {
	case errStr := <-errChan:
		t.Fatal(errStr)
	default:
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("error reading response body %v", err)
	}
	if string(data) != "this is a test" {
		t.Fatalf("error incorrect response")
	}
}
