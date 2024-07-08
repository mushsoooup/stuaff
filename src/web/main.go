package main

import (
	"cs/database"
	"cs/router"
	"encoding/json"
	"flag"
	"os"
	"runtime"
)

type Config struct {
	Ip     string   `json:"server_ip"`
	Port   string   `json:"server_port"`
	Thread int      `json:"threads"`
	Addr   []string `json:"kv_address"`
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config_path", "", "")
	flag.Parse()
	file, err := os.ReadFile(configPath)
	if err != nil {
		panic(err)
	}
	var config Config
	json.Unmarshal(file, &config)
	database.Init(config.Addr)
	runtime.GOMAXPROCS(config.Thread)
	r := router.Router{}
	r.LoadStatic("./static")
	r.Register("/api/search/course", "GET", GetCourse)
	r.Register("/api/search/all", "GET", GetAllCourse)
	r.Register("/api/search/student", "GET", GetStudent)
	r.Register("/api/choose", "POST", Choose)
	r.Register("/api/drop", "POST", Drop)
	r.Register("/ping", "GET", Ping)
	r.Serve(config.Ip + ":" + config.Port)
}
