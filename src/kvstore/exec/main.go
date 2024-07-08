package main

import (
	"cs/kvstore"
	"flag"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config_path", "", "")
	flag.Parse()
	if configPath == "" {
		panic("invalid config path")
	}
	var kv kvstore.KVStore
	kv.Serve(configPath)
}
