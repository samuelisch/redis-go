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
	dir := flag.String("dir", "", "Directory for RDB persistence")
	dbfilename := flag.String("dbfilename", "", "RDB filename")
	flag.Parse()

	role := "master"
	if *replicaOf != "" {
		role = "slave"
	}

	s := newServer(role, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", *dir, *dbfilename)

	ownPort := fmt.Sprintf("%d", *port)
	addr := ":" + ownPort
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to bind on %s: %v\n", ownPort, err)
		os.Exit(1)
	}
	fmt.Println("Listening on port", ownPort)

	if *replicaOf != "" {
		fmt.Println("Replicating from", *replicaOf)
		parts := strings.Split(*replicaOf, " ")
		if len(parts) != 2 {
			fmt.Println("replicaof must be: host port")
			os.Exit(1)
		}
		go func() {
			if err := startReplicationClient(s, parts[0], parts[1], ownPort); err != nil {
				fmt.Println("Replication client error:", err)
			}
		}()
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			os.Exit(1)
		}
		go handleConn(s, conn)
	}
}
