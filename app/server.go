package main

import (
	"bufio"
	"net"
	"strings"
)

type Server struct {
	store      *Store
	repl       *ReplicationManager
	role       string
	dir        string
	dbfilename string
}

func newServer(role string, replid string, dir string, dbfilename string) *Server {
	return &Server{
		store:      newStore(),
		repl:       &ReplicationManager{replid: replid},
		role:       role,
		dir:        dir,
		dbfilename: dbfilename,
	}
}

type Client struct {
	conn          net.Conn
	multiCommands []func() string
	watched       map[string]uint64
	replOffset    int
}

func handleConn(server *Server, conn net.Conn) {
	client := &Client{
		conn:    conn,
		watched: map[string]uint64{},
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
				return handler(server, client, args)
			})
			conn.Write([]byte(encodeSimpleString("QUEUED")))
		} else {
			conn.Write([]byte(handler(server, client, args)))
		}
		if cmd == "PSYNC" {
			transferred = true
			return
		}
	}
}
