package lb

import (
	"encoding/json"
	"os"
)

type LoadBalancer struct {
}

type Config struct {
	Servers []string `json:"servers"`
}

func (l *LoadBalancer) Serve(addr string, config string) {
	content, err := os.ReadFile(config)
	if err != nil {
		panic(err)
	}
	var c Config
	err = json.Unmarshal(content, &c)
	if err != nil {
		panic(err)
	}
	s := Proxy{}
	s.Serve(addr, c.Servers...)
}
