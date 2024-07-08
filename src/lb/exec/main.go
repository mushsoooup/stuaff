package main

import (
	"cs/lb"
	"flag"
)

func main() {
	var ip, port, web_config_path string
	flag.StringVar(&ip, "ip", "", "")
	flag.StringVar(&port, "port", "", "")
	flag.StringVar(&web_config_path, "web_config_path", "", "")
	flag.Parse()
	l := lb.LoadBalancer{}
	l.Serve(ip+":"+port, web_config_path)
}
