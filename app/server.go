package main

import (
	"bufio"
	"net"
	"strings"
)

type Client struct {
	conn          net.Conn
	multiCommands []func() string
	watched       map[string]uint64
	role          string
	replOffset    int
}

func handleConn(conn net.Conn, replicaVal string) {
	clientRole := "master"
	if replicaVal != "" {
		clientRole = "slave"
	}

	client := &Client{
		conn:          conn,
		multiCommands: nil,
		watched:       map[string]uint64{},
		role:          clientRole,
	}

	transferred := false
	defer func() {
		if !transferred {
			conn.Close()
		}
	}()

	reader := bufio.NewReader(conn)
	for {
		args, err := parseCommand(reader)
		if err != nil {
			return
		}

		cmd := strings.ToUpper(args[0])
		handler, ok := commandHandlers[cmd]
		if !ok {
			conn.Write([]byte(encodeError("ERR unknown command '" + cmd + "'")))
			continue
		}
		if cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD" && cmd != "WATCH" && cmd != "UNWATCH" && client.multiCommands != nil {
			client.multiCommands = append(client.multiCommands, func() string {
				return handler(client, args)
			})
			conn.Write([]byte(encodeSimpleString("QUEUED")))
		} else {
			conn.Write([]byte(handler(client, args)))
		}
		if cmd == "PSYNC" {
			transferred = true
			return
		}
	}
}
