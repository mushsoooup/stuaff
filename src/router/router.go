package router

import (
	"cs/server"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
)

type routeEntry struct {
	handler     server.HandlerFunc
	allowMethod string
}

type Router struct {
	routeMap map[string]*routeEntry
}

func (r *Router) Serve(addr string) {
	r.logo()
	r.displayRouteMap()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}
	server := server.Server{}
	server.RegisterHandler(r.handler)
	server.Serve(ln)
}

// Please load static so error pages function
func (r *Router) handler(ctx *server.RequestCtx) error {
	entry, ok := r.routeMap[ctx.Req.GetPath()]
	if !ok {
		if ctx.Req.GetPath() == "/" {
			entry = r.routeMap["/index.html"]
		} else {
			entry = r.routeMap["/404.html"]
		}
	} else if entry.allowMethod != ctx.Req.GetMethod() {
		entry = r.routeMap["/501.html"]
	}
	return entry.handler(ctx)
}

// Register handler
func (r *Router) Register(path string, method string, handler server.HandlerFunc) {
	if r.routeMap == nil {
		r.routeMap = make(map[string]*routeEntry)
	}
	r.routeMap[path] = &routeEntry{
		handler:     handler,
		allowMethod: method,
	}
}

// Load static folder and resgister files to specific path
func (r *Router) LoadStatic(static string) error {
	err := filepath.Walk(static, func(file string, info fs.FileInfo, err error) error {
		if info == nil || info.IsDir() || err != nil {
			return nil
		}
		idx := strings.LastIndex(info.Name(), ".")
		if idx == -1 {
			return nil
		}
		extension := info.Name()[idx+1:]
		var contentType string
		switch extension {
		case "html":
			fallthrough
		case "css":
			contentType = "text/" + extension
		case "js":
			contentType = "text/javascript"
		case "json":
			contentType = "application/json"
		default:
			return nil
		}
		routePath, err := filepath.Rel(static, file)
		routePath = strings.ReplaceAll(routePath, "\\", "/")
		if err != nil {
			log.Printf("error registering route %v\n", file)
		}
		r.Register("/"+routePath, "GET", r.serveFile(file, contentType, info.Name()))
		return nil
	})
	return err
}

// Serve static file
func (r *Router) serveFile(path string, contentType string, name string) server.HandlerFunc {
	return func(ctx *server.RequestCtx) error {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		ctx.Res.SetContentType(contentType)
		content, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		var status int
		switch name {
		case "404.html":
			status = 404
		case "501.html":
			status = 501
		case "503.html":
			status = 503
		default:
			status = 200
		}
		ctx.Res.SetData(content, status)
		return nil
	}
}

func (r *Router) logo() {
	log.Println("────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────")
	log.Println("─██████████████─██████████─██████──────────██████─████████──████████─██████──────────██████─██████████████─██████████████───")
	log.Println("─██░░░░░░░░░░██─██░░░░░░██─██░░██████████──██░░██─██░░░░██──██░░░░██─██░░██──────────██░░██─██░░░░░░░░░░██─██░░░░░░░░░░██───")
	log.Println("─██████░░██████─████░░████─██░░░░░░░░░░██──██░░██─████░░██──██░░████─██░░██──────────██░░██─██░░██████████─██░░██████░░██───")
	log.Println("─────██░░██───────██░░██───██░░██████░░██──██░░██───██░░░░██░░░░██───██░░██──────────██░░██─██░░██─────────██░░██──██░░██───")
	log.Println("─────██░░██───────██░░██───██░░██──██░░██──██░░██───████░░░░░░████───██░░██──██████──██░░██─██░░██████████─██░░██████░░████─")
	log.Println("─────██░░██───────██░░██───██░░██──██░░██──██░░██─────████░░████─────██░░██──██░░██──██░░██─██░░░░░░░░░░██─██░░░░░░░░░░░░██─")
	log.Println("─────██░░██───────██░░██───██░░██──██░░██──██░░██───────██░░██───────██░░██──██░░██──██░░██─██░░██████████─██░░████████░░██─")
	log.Println("─────██░░██───────██░░██───██░░██──██░░██████░░██───────██░░██───────██░░██████░░██████░░██─██░░██─────────██░░██────██░░██─")
	log.Println("─────██░░██─────████░░████─██░░██──██░░░░░░░░░░██───────██░░██───────██░░░░░░░░░░░░░░░░░░██─██░░██████████─██░░████████░░██─")
	log.Println("─────██░░██─────██░░░░░░██─██░░██──██████████░░██───────██░░██───────██░░██████░░██████░░██─██░░░░░░░░░░██─██░░░░░░░░░░░░██─")
	log.Println("─────██████─────██████████─██████──────────██████───────██████───────██████──██████──██████─██████████████─████████████████─")
	log.Println("──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── by 😘🥰")
}

func (r *Router) displayRouteMap() {
	for path, entry := range r.routeMap {
		name := runtime.FuncForPC(reflect.ValueOf(entry.handler).Pointer()).Name()
		log.Printf("%v\tmethod: %v\thandler %v\n", path, entry.allowMethod, name)
	}
}
