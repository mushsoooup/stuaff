package router

import (
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestStatic(t *testing.T) {
	r := Router{}
	err := r.LoadStatic("../../static")
	if err != nil {
		t.Fatalf("error loading static %v", err)
	}
	go r.Serve(":65530")
	c := &http.Client{}
	time.Sleep(1 * time.Second)
	// Test random html
	testIndex(c, t)
	// Test 404
	test404(c, t)
	// Test wrong method
	testWrongMethod(c, t)
}

func testIndex(c *http.Client, t *testing.T) {
	res, err := c.Get("http://127.0.0.1:65530/index.html")
	if err != nil {
		t.Fatalf("error requesting static file %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("error requesting static file with code %v", res.StatusCode)
	}
	bytes, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatalf("error reading response %v", err)
	}
	file, err := os.Open("../../static/index.html")
	if err != nil {
		t.Fatalf("error opening file index.html %v", err)
	}
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("error reading file index.html %v", err)
	}
	if len(bytes) != len(fileBytes) {
		t.Fatalf("response not correct")
	}
	for idx := range bytes {
		if bytes[idx] != fileBytes[idx] {
			t.Fatalf("response not correct")
			break
		}
	}
}

func test404(c *http.Client, t *testing.T) {
	res, err := c.Get("http://127.0.0.1:65530/false.html")
	if err != nil {
		t.Fatalf("error requesting static file %v", err)
	}
	if res.StatusCode != 404 {
		t.Fatalf("error requesting static file with code %v", res.StatusCode)
	}
	bytes, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatalf("error reading response %v", err)
	}
	file, err := os.Open("../../static/404.html")
	if err != nil {
		t.Fatalf("error opening file 404.html %v", err)
	}
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("error reading file 404.html %v", err)
	}
	if len(bytes) != len(fileBytes) {
		t.Fatalf("response not correct")
	}
	for idx := range bytes {
		if bytes[idx] != fileBytes[idx] {
			t.Fatalf("response not correct")
			break
		}
	}
}

func testWrongMethod(c *http.Client, t *testing.T) {
	req, _ := http.NewRequest("Post", "http://127.0.0.1:65530/index.html", strings.NewReader("test message"))
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("error requesting static file %v", err)
	}
	if res.StatusCode != 501 {
		t.Fatalf("error requesting static file with code %v", res.StatusCode)
	}
	bytes, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatalf("error reading response %v", err)
	}
	file, err := os.Open("../../static/501.html")
	if err != nil {
		t.Fatalf("error opening file 501.html %v", err)
	}
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("error reading file 501.html %v", err)
	}
	if len(bytes) != len(fileBytes) {
		t.Fatalf("response not correct")
	}
	for idx := range bytes {
		if bytes[idx] != fileBytes[idx] {
			t.Fatalf("response not correct")
			break
		}
	}
}
