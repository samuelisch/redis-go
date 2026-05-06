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

type ValueKind int
const (
	KindString ValueKind = iota
	KindStringList
)
type StoreValue struct {
	Kind ValueKind
	S string
	Slice []string
}

var store = map[string]StoreValue{}

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
			v, found := store[args[1]]
			if found {
				if v.Kind != KindString {
					fmt.Errorf("WRONGTYPE trying to access something other than a string")
					return
				}
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v.S), v.S)))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		case "SET":
			store[args[1]] = StoreValue{
				Kind: KindString,
				S: args[2],
			}
			if len(args) >= 5 {
				timeoutType := strings.ToUpper(args[3])
				if timeoutType == "PX" || timeoutType == "EX" {
					expiryValue, _ := strconv.ParseInt(args[4], 10, 64)
					setExpiry(ExpiryType(timeoutType), expiryValue, args[1])
				}
			}
			conn.Write([]byte("+OK\r\n"))
		case "RPUSH":
			v, found := store[args[1]]
			list := []string{}
			if found {
				if v.Kind != KindStringList {
					fmt.Errorf("WRONGTYPE trying to access something other than a list")
					return
				}
				list = v.Slice
			}
			for _, item := range args[2:] {
				list = append(list, item)
			}
			store[args[1]] = StoreValue{
				Kind: KindStringList, 
				Slice: list,
			}
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", len(list))))
		case "LRANGE":
			v, found := store[args[1]]
			if found && len(args) == 4 {
				start, err := strconv.Atoi(args[2])
				if err != nil {
					conn.Write([]byte("*0\r\n"))
				}
				end, err := strconv.Atoi(args[3])
				if err != nil {
					conn.Write([]byte("*0\r\n"))
				}
				if end < len(v.Slice) {
					end += 1
				} else {
					end = len(v.Slice)
				}
				list := v.Slice[start:end]
				resp := "*" + strconv.Itoa(len(list)) + "\r\n"
				for _, item := range list {
					s := fmt.Sprintf("$%d\r\n%s\r\n", len(item), item)
					resp += s
				}
				conn.Write([]byte(resp));
			} else {
				conn.Write([]byte("*0\r\n"))
			}
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
