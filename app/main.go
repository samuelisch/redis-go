package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	port := flag.Int("port", 6379, "Port to listen on")
	replicaOf := flag.String("replicaof", "", "Define which host and port to replicate")
	flag.Parse()

	repl = &ReplicationManager{
		replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
	}

	ownPort := fmt.Sprintf("%d", *port)
	addr := ":" + ownPort
	replicaVal := fmt.Sprintf("%s", *replicaOf)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind on %s: %v", ownPort, err)
		os.Exit(1)
	}
	fmt.Println("Listening on port", ownPort)
	if replicaVal != "" {
		fmt.Println("Replicating from", replicaVal)
		parts := strings.Split(replicaVal, " ")
		if len(parts) != 2 {
			fmt.Println("replicaof must be: host port")
			os.Exit(1)
		}
		masterHost := parts[0]
		masterPort := parts[1]

		go func() {
			err := startReplicationClient(masterHost, masterPort, ownPort)
			if err != nil {
				fmt.Println("Replication client error:", err)
			}
		}()
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn, replicaVal)
	}
}
