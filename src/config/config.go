package config

import (
	"io"
	"log"
	"os"
	"strings"
)

func ReadConfig(config string) (addrs []string) {
	file, err := os.Open(config)
	if err != nil {
		log.Fatalf("error opening config file %v\n", config)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("error reading config %v\n", err)
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		// Comment line
		if len(line) != 0 && line[0] == '!' {
			continue
		}
		addr, found := strings.CutPrefix(line, "follower_info")
		if found {
			addrs = append(addrs, strings.TrimSpace(addr))
		}
	}
	return addrs
}
