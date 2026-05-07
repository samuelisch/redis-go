package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"strconv"
	"strings"
	"time"
	"sync"
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
var waiters = map[string][]chan string{}
var waitersMu sync.Mutex

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

func encodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func encodeError(msg string) string {
	return fmt.Sprintf("-%s\r\n", msg)
}

func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func encodeNullBulk() string {
	return "$-1\r\n"
}

func encodeInteger(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

func encodeArray(items []string) string {
	resp := fmt.Sprintf("*%d\r\n", len(items))
	for _, item := range items {
		resp += encodeBulkString(item)
	}
	return resp
}

func handlePing(args []string) string {
	return encodeSimpleString("PONG")
}

func handleEcho(args []string) string {
	if len(args) < 2 {
		return encodeError("ERR wrong number of arguments for 'echo' command")
	}
	return encodeBulkString(args[1])
}

func handleGet(args []string) string {
	if len(args) < 2 {
		return encodeError("ERR wrong number of arguments for 'get' command")
	}
	v, found := store[args[1]]
	if !found {
		return encodeNullBulk()
	}
	if v.Kind != KindString {
		return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return encodeBulkString(v.S)
}

func handleSet(args []string) string {
	if len(args) < 3 {
		return encodeError("ERR wrong number of arguments for 'set' command")
	}
	store[args[1]] = StoreValue{Kind: KindString, S: args[2]}
	if len(args) >= 5 {
		timeoutType := strings.ToUpper(args[3])
		if timeoutType == "PX" || timeoutType == "EX" {
			expiryValue, _ := strconv.ParseInt(args[4], 10, 64)
			setExpiry(ExpiryType(timeoutType), expiryValue, args[1])
		}
	}
	return encodeSimpleString("OK")
}

func checkWaiters(key string, list []string) []string {
	waitersMu.Lock()
	for len(list) > 0 {
		chans, ok := waiters[key]
		if !ok || len(chans) == 0 {
			break
		}
		ch := chans[0]
		waiters[key] = chans[1:]
		ch <- list[0]
		list = list[1:]
	}
	waitersMu.Unlock()
	return list
}

func handleRpush(args []string) string {
	if len(args) < 3 {
		return encodeError("ERR wrong number of arguments for 'rpush' command")
	}
	v, found := store[args[1]]
	list := []string{}
	if found {
		if v.Kind != KindStringList {
			return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		list = v.Slice
	}
	list = append(list, args[2:]...)
	count := len(list)
	list = checkWaiters(args[1], list)
	store[args[1]] = StoreValue{Kind: KindStringList, Slice: list}

	return encodeInteger(count)
}

func handleLpush(args []string) string {
	if len(args) < 3 {
		return encodeError("ERR wrong number of arguments for 'rpush' command")
	}
	v, found := store[args[1]]
	list := []string{}
	if found {
		if v.Kind != KindStringList {
			return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		list = v.Slice
	}
	for i := 2; i < len(args); i++ {
		list = append([]string{args[i]}, list...)
	}
	count := len(list)
	list = checkWaiters(args[1], list)
	store[args[1]] = StoreValue{Kind: KindStringList, Slice: list}

	return encodeInteger(count)
}

func handleLlen(args []string) string {
	if len(args) != 2 {
		return encodeError("ERR wrong number of arguments for 'llen' command")
	}
	v, found := store[args[1]]
	if !found {
		return encodeInteger(0)
	}
	return encodeInteger(len(v.Slice))
}

func handleLrange(args []string) string {
	if len(args) != 4 {
		return encodeError("ERR wrong number of arguments for 'lrange' command")
	}
	v, found := store[args[1]]
	if !found {
		return encodeArray([]string{})
	}
	if v.Kind != KindStringList {
		return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	start, err := strconv.Atoi(args[2])
	if err != nil {
		return encodeError("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[3])
	if err != nil {
		return encodeError("ERR value is not an integer or out of range")
	}
	list := v.Slice
	if start < 0 {
		start = len(list) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(list) + end
	}
	if end >= len(list) {
		end = len(list) - 1
	}
	if start > end {
		return encodeArray([]string{})
	}
	return encodeArray(list[start : end+1])
}

func handleLpop(args []string) string {
	if len(args) < 2 || len(args) > 3 {
		return encodeError("ERR wrong number of arguments for 'lpop' command")
	}
	v, found := store[args[1]]
	if !found {
		return encodeNullBulk()
	}
	if v.Kind != KindStringList {
		return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := v.Slice
	if (len(list) == 0) {
		return encodeNullBulk()
	}

	if (len(args) == 3) {
		numToRemove, err := strconv.Atoi(args[2])
		if err != nil {
			return encodeError("ERR value is not an integer or out of range")
		}
		if (numToRemove >= len(list)) {
			store[args[1]] = StoreValue{Kind: KindStringList, Slice: []string{}}
			return encodeArray(list)
		}
		removed := []string{}
		for i := 0; i < numToRemove; i++ {
			element := list[0]
			removed = append(removed, element)
			list = list[1:]
		}
		store[args[1]] = StoreValue{Kind: KindStringList, Slice: list}
		return encodeArray(removed)
	}

	element := list[0]
	store[args[1]] = StoreValue{Kind: KindStringList, Slice: list[1:]}
	return encodeBulkString(element);
}

func handleBlpop(args []string) string {
	if len(args) != 3 {
		return encodeError("ERR wrong number of arguments for 'lpop' command")
	}
	key := args[1]
	_, err := strconv.Atoi(args[2])
	if err != nil {
		return encodeError("ERR value is not an integer or out of range")
	}

	v, found := store[key]
	if found && v.Kind == KindStringList && len(v.Slice) > 0 {
		element := v.Slice[0]
		store[key] = StoreValue{Kind: KindStringList, Slice: v.Slice[1:]}
		return encodeArray([]string{key, element})
	}

	ch := make(chan string, 1)
	waitersMu.Lock()
	waiters[key] = append(waiters[key], ch)
	waitersMu.Unlock()

	if timeout > 0 {
		expiryTime := time.Duration(timeout)*time.Second
		time.AfterFunc(expiryTime, func() {
			waitersMu.Lock()
			chans := waiters[key]
			for i, c := range changs {
				if c == ch {
					waiters[key] = append(chans[:i], chans[i + 1 ]...)
					close(ch)
					break
				}
			}
			waitersMu.Unlock()
		})
	}

	element, ok := <-ch
	if (!ok) {
		return encodeNullBulk()
	}

	return encodeArray([]string{key, element})
}

var commandHandlers = map[string]func([]string) string{
	"PING": handlePing,
	"ECHO": handleEcho,
	"GET": handleGet,
	"SET": handleSet,
	"RPUSH": handleRpush,
	"LPUSH": handleLpush,
	"LLEN": handleLlen,
	"LRANGE": handleLrange,
	"LPOP": handleLpop,
	"BLPOP": handleBlpop,
}

func handleConn(conn net.Conn) {
	defer conn.Close()

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
		conn.Write([]byte(handler(args)))
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
