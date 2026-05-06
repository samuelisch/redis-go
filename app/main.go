package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"strconv"
	"strings"
	"time"
)

type ExpiryType string

const (
	ExpiryPX ExpiryType = "PX"
	ExpiryEX ExpiryType = "EX"
)

var store = map[string]string{}

func setExpiry(expiryType ExpiryType, ttl int64, storedKey string) {
	if expiryType != ExpiryPX && expiryType != ExpiryEX {
		fmt.Println("EXPIRY TYPE NOT VALID, SKIPPING")
		return
	}
	var expiryTime time.Duration
	if expiryType == "EX" {
		expiryTime = time.Duration(ttl)*time.Second
	} else {
		expiryTime = time.Duration(ttl)*time.Millisecond
	}
	time.AfterFunc(expiryTime, func() {
		delete(store, storedKey)
		fmt.Println("Expired key: ", storedKey)
	})
}

func parseCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	argCount, _ := strconv.Atoi(strings.TrimSpace(line[1:]))

	args := make([]string, 0, argCount)
	for i := 0; i < argCount; i++ {
		reader.ReadString('\n')
		value, _ := reader.ReadString('\n')
		args = append(args, strings.TrimSpace(value))
	}
	return args, nil
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		args, err := parseCommand(reader)
		if err != nil {
			return
		}

		switch strings.ToUpper(args[0]) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])))
		case "GET":
			fmt.Println(store)
			v, found := store[args[1]]
			if found {
				conn.Write([]byte("$-1\r\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
			}
		case "SET":
			store[args[1]] = args[2]
			if len(args) >= 5 {
				timeoutType := strings.ToUpper(args[3])
				if timeoutType == "PX" || timeoutType == "EX" {
					expiryValue, _ := strconv.ParseInt(args[4], 10, 64)
					setExpiry(ExpiryType(timeoutType), expiryValue, args[1])
				}
			}
			conn.Write([]byte("+OK\r\n"))
		default:
			conn.Write([]byte("+NO IDEA WHAT THIS IS MATE\r\n"))
		}
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}
}
