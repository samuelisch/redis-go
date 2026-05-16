package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

const emptyRDBHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

type ReplicaConn struct {
	writer      *bufio.Writer
	reader      *bufio.Reader
	knownOffset int
}

type ReplicationManager struct {
	mu       sync.Mutex
	replid   string
	offset   int
	replicas []*ReplicaConn
}

func (r *ReplicationManager) addReplica(conn net.Conn) *ReplicaConn {
	rc := &ReplicaConn{
		writer: bufio.NewWriter(conn),
		reader: bufio.NewReader(conn),
	}
	r.mu.Lock()
	r.replicas = append(r.replicas, rc)
	r.mu.Unlock()

	go rc.readLoop(r)
	return rc
}

func (r *ReplicationManager) propagate(args []string) {
	encoded := encodeArray(args)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offset += len(encoded)
	alive := r.replicas[:0]
	for _, replica := range r.replicas {
		_, err1 := replica.writer.WriteString(encoded)
		err2 := replica.writer.Flush()
		if err1 == nil && err2 == nil {
			alive = append(alive, replica)
		}
	}
	r.replicas = alive
}

func (r *ReplicationManager) sendGetAck() {
	r.mu.Lock()
	defer r.mu.Unlock()
	getack := encodeArray([]string{"REPLCONF", "GETACK", "*"})
	for _, rc := range r.replicas {
		rc.writer.WriteString(getack)
		rc.writer.Flush()
	}
}

func (rc *ReplicaConn) readLoop(r *ReplicationManager) {
	for {
		args, err := parseCommand(rc.reader)
		if err != nil {
			return
		}
		if len(args) == 3 && strings.ToUpper(args[0]) == "REPLCONF" && strings.ToUpper(args[1]) == "ACK" {
			offset, err := strconv.Atoi(args[2])
			if err == nil {
				r.mu.Lock()
				rc.knownOffset = offset
				r.mu.Unlock()
			}
		}
	}
}

func sendPing(conn net.Conn, reader *bufio.Reader) error {
	_, err := conn.Write([]byte(encodeArray([]string{"PING"})))

	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to write PING: %w", err)
	}
	if !strings.HasPrefix(line, "+PONG") {
		return fmt.Errorf("unexpected PING response: %q", line)
	}
	fmt.Println("Master:", strings.TrimSpace(line))
	return nil
}

func sendReplconf(conn net.Conn, reader *bufio.Reader, args ...string) error {
	cmd := append([]string{"REPLCONF"}, args...)
	_, err := conn.Write([]byte(encodeArray(cmd)))
	if err != nil {
		return fmt.Errorf("write REPLCONF: %w", err)
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read REPLCONF reply: %w", err)
	}
	if !strings.HasPrefix(line, "+OK") {
		return fmt.Errorf("unexpected REPLCONF reply: %q", line)
	}
	return nil
}

func sendPsync(conn net.Conn, reader *bufio.Reader) error {
	cmd := append([]string{"PSYNC", "?", "-1"})
	_, err := conn.Write([]byte(encodeArray(cmd)))
	if err != nil {
		return fmt.Errorf("write PSYNC: %w", err)
	}
	return nil
}

func startReplicationClient(server *Server, masterHost string, masterPort string, ownPort string) error {
	addr := net.JoinHostPort(masterHost, masterPort)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to master %s: %w", addr, err)
	}
	fmt.Println("Connected to master instance", addr)

	reader := bufio.NewReader(conn)

	if err = sendPing(conn, reader); err != nil {
		conn.Close()
		return err
	}
	if err = sendReplconf(conn, reader, "listening-port", ownPort); err != nil {
		conn.Close()
		return err
	}
	if err = sendReplconf(conn, reader, "capa", "psync2"); err != nil {
		conn.Close()
		return err
	}
	if err = sendPsync(conn, reader); err != nil {
		conn.Close()
		return err
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return fmt.Errorf("read FULLRESYNC: %w", err)
	}
	if !strings.HasPrefix(line, "+FULLRESYNC") {
		conn.Close()
		return fmt.Errorf("unexpected PSYNC response: %q", line)
	}
	fmt.Println("Received:", strings.TrimSpace(line))

	line, err = reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return fmt.Errorf("read RDB header: %w", err)
	}
	rdbLen, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		conn.Close()
		return fmt.Errorf("parse RDB length: %w", err)
	}
	if _, err = io.ReadFull(reader, make([]byte, rdbLen)); err != nil {
		conn.Close()
		return fmt.Errorf("read RDB body: %w", err)
	}
	fmt.Printf("Loaded RDB snapshot (%d bytes)\n", rdbLen)

	replicaClient := &Client{conn: conn, watched: map[string]uint64{}}
	for {
		args, err := parseCommand(reader)
		if err != nil {
			conn.Close()
			return fmt.Errorf("replication stream ended: %w", err)
		}
		cmd := strings.ToUpper(args[0])
		encoded := encodeArray(args)
		if handler, ok := commandHandlers[cmd]; ok {
			if cmd == "REPLCONF" {
				if resp := handler(server, replicaClient, args); resp != "" {
					conn.Write([]byte(resp))
				}
				replicaClient.replOffset += len(encoded)
			} else {
				replicaClient.replOffset += len(encoded)
				handler(server, replicaClient, args)
			}
		}
	}
}
