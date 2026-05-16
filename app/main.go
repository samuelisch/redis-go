package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	conn          net.Conn
	multiCommands []func() string
	watched       map[string]uint64
	role          string
	replOffset    int
}

type ExpiryType string

const (
	ExpiryPX ExpiryType = "PX"
	ExpiryEX ExpiryType = "EX"
)

type ValueKind int

const (
	KindString ValueKind = iota
	KindStringList
	KindSet
	KindZSet
	KindHash
	KindStream
	KindVectorSet
)

type StoreValue struct {
	Kind   ValueKind
	S      string
	Slice  []string
	Stream []map[string]string
}

var kindNames = map[ValueKind]string{
	KindString:     "string",
	KindStringList: "list",
	KindSet:        "set",
	KindZSet:       "zset",
	KindHash:       "hash",
	KindStream:     "stream",
	KindVectorSet:  "vectorset",
}

type StreamEntry struct {
	ID     string
	Fields string
}

type StreamWaiterEntry struct {
	ID      string
	Channel chan string
}

var store = map[string]StoreValue{}
var versions = map[string]uint64{}
var waiters = map[string][]chan string{}
var streamWaiters = map[string][]StreamWaiterEntry{}
var storeMu sync.RWMutex
var versionsMu sync.RWMutex
var waitersMu sync.RWMutex
var streamWaitersMu sync.RWMutex

const emptyRDBHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

type ReplicaConn struct {
	writer *bufio.Writer
}

type ReplicationManager struct {
	mu       sync.Mutex
	replid   string
	offset   int
	replicas []*ReplicaConn
}

func (r *ReplicationManager) addReplica(conn net.Conn) *ReplicaConn {
	rc := &ReplicaConn{writer: bufio.NewWriter(conn)}
	r.mu.Lock()
	r.replicas = append(r.replicas, rc)
	r.mu.Unlock()
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

var repl *ReplicationManager

func setExpiry(expiryType ExpiryType, ttl int64, storedKey string) {
	if expiryType != ExpiryPX && expiryType != ExpiryEX {
		fmt.Println("EXPIRY TYPE NOT VALID, SKIPPING")
		return
	}
	var expiryTime time.Duration
	if expiryType == "EX" {
		expiryTime = time.Duration(ttl) * time.Second
	} else {
		expiryTime = time.Duration(ttl) * time.Millisecond
	}
	time.AfterFunc(expiryTime, func() {
		delete(store, storedKey)
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

func encodeNullArray() string {
	return "*-1\r\n"
}

func setKey(key string, value StoreValue) {
	storeMu.Lock()
	versionsMu.Lock()
	store[key] = value
	versions[key]++
	storeMu.Unlock()
	versionsMu.Unlock()
}

func handlePing(_ *Client, args []string) string {
	return encodeSimpleString("PONG")
}

func handleEcho(_ *Client, args []string) string {
	if len(args) < 2 {
		return encodeError("ERR wrong number of arguments for 'echo' command")
	}
	return encodeBulkString(args[1])
}

func handleGet(_ *Client, args []string) string {
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

func handleSet(_ *Client, args []string) string {
	if len(args) < 3 {
		return encodeError("ERR wrong number of arguments for 'set' command")
	}
	setKey(args[1], StoreValue{Kind: KindString, S: args[2]})
	if len(args) >= 5 {
		timeoutType := strings.ToUpper(args[3])
		if timeoutType == "PX" || timeoutType == "EX" {
			expiryValue, _ := strconv.ParseInt(args[4], 10, 64)
			setExpiry(ExpiryType(timeoutType), expiryValue, args[1])
		}
	}
	repl.propagate(args)
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

func handleRpush(_ *Client, args []string) string {
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
	setKey(args[1], StoreValue{Kind: KindStringList, Slice: list})
	repl.propagate(args)
	return encodeInteger(count)
}

func handleLpush(_ *Client, args []string) string {
	if len(args) < 3 {
		return encodeError("ERR wrong number of arguments for 'lpush' command")
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
	setKey(args[1], StoreValue{Kind: KindStringList, Slice: list})
	repl.propagate(args)
	return encodeInteger(count)
}

func handleLlen(_ *Client, args []string) string {
	if len(args) != 2 {
		return encodeError("ERR wrong number of arguments for 'llen' command")
	}
	v, found := store[args[1]]
	if !found {
		return encodeInteger(0)
	}
	return encodeInteger(len(v.Slice))
}

func handleLrange(_ *Client, args []string) string {
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

func handleLpop(_ *Client, args []string) string {
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
	if len(list) == 0 {
		return encodeNullBulk()
	}

	if len(args) == 3 {
		numToRemove, err := strconv.Atoi(args[2])
		if err != nil {
			return encodeError("ERR value is not an integer or out of range")
		}
		if numToRemove >= len(list) {
			setKey(args[1], StoreValue{Kind: KindStringList, Slice: []string{}})
			repl.propagate(args)
			return encodeArray(list)
		}
		var removed []string
		for i := 0; i < numToRemove; i++ {
			element := list[0]
			removed = append(removed, element)
			list = list[1:]
		}
		setKey(args[1], StoreValue{Kind: KindStringList, Slice: list})
		repl.propagate(args)
		return encodeArray(removed)
	}

	element := list[0]
	setKey(args[1], StoreValue{Kind: KindStringList, Slice: list[1:]})
	repl.propagate(args)
	return encodeBulkString(element)
}

func handleBlpop(_ *Client, args []string) string {
	if len(args) != 3 {
		return encodeError("ERR wrong number of arguments for 'lpop' command")
	}
	key := args[1]
	timeout, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return encodeError("ERR value is not an integer or out of range")
	}

	v, found := store[key]
	if found && v.Kind == KindStringList && len(v.Slice) > 0 {
		element := v.Slice[0]
		setKey(key, StoreValue{Kind: KindStringList, Slice: v.Slice[1:]})
		return encodeArray([]string{key, element})
	}

	ch := make(chan string, 1)
	waitersMu.Lock()
	waiters[key] = append(waiters[key], ch)
	waitersMu.Unlock()

	if timeout > 0 {
		expiryTime := time.Duration(timeout * float64(time.Second))
		time.AfterFunc(expiryTime, func() {
			waitersMu.Lock()
			chans := waiters[key]
			for i, c := range chans {
				if c == ch {
					waiters[key] = append(chans[:i], chans[i+1:]...)
					close(ch)
					break
				}
			}
			waitersMu.Unlock()
		})
	}

	element, ok := <-ch
	if !ok {
		return encodeNullArray()
	}

	return encodeArray([]string{key, element})
}

func handleType(_ *Client, args []string) string {
	if len(args) != 2 {
		return encodeError("ERR wrong number of arguments for 'type' command")
	}
	v, found := store[args[1]]
	if !found {
		return encodeSimpleString("none")
	}
	return encodeSimpleString(kindNames[v.Kind])
}

func parseEntryId(id string) (ms int64, seq *int, err error) {
	parts := strings.Split(id, "-")
	ms, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, nil, err
	}
	if len(parts) == 2 {
		s, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, nil, err
		}
		seq = &s
	}
	return ms, seq, nil
}

func nextSequence(ms, lastMs int64, lastSeq int) int {
	if ms == lastMs {
		return lastSeq + 1
	}
	return 0
}

func validateEntryId(stream []map[string]string, id string) (string, string) {
	splitId := strings.Split(id, "-")
	if id != "*" && len(splitId) != 2 {
		return id, encodeError("ERR The ID specific in XADD must follow the convention num-num, num-* or *")
	}

	lastEntrySplitId := []string{"0", "0"}
	if len(stream) > 0 {
		lastEntrySplitId = strings.Split(stream[len(stream)-1]["id"], "-")
	}
	lastMs, _ := strconv.ParseInt(lastEntrySplitId[0], 10, 64)
	lastSeq, _ := strconv.Atoi(lastEntrySplitId[1])

	switch {
	case id == "*":
		currentMs := time.Now().UnixMilli()
		if currentMs < lastMs {
			return id, encodeError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		id = strconv.FormatInt(currentMs, 10) + "-" + strconv.Itoa(nextSequence(currentMs, lastMs, lastSeq))

	case splitId[1] == "*":
		ms, err := strconv.ParseInt(splitId[0], 10, 64)
		if err != nil {
			return id, encodeError("ERR value is not an integer or out of range")
		}
		if ms < lastMs {
			return id, encodeError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		id = strconv.FormatInt(ms, 10) + "-" + strconv.Itoa(nextSequence(ms, lastMs, lastSeq))

	default:
		ms, err := strconv.ParseInt(splitId[0], 10, 64)
		if err != nil {
			return id, encodeError("ERR value is not an integer or out of range")
		}
		seq, err := strconv.Atoi(splitId[1])
		if err != nil {
			return id, encodeError("ERR value is not an integer or out of range")
		}
		if ms == 0 && seq <= 0 {
			return id, encodeError("ERR The ID specified in XADD must be greater than 0-0")
		}
		if ms < lastMs || (ms == lastMs && seq <= lastSeq) {
			return id, encodeError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	return id, ""
}

func checkStreamWaiters(key string, entryId string, stream []map[string]string) []map[string]string {
	streamWaitersMu.Lock()
	channelEntries, ok := streamWaiters[key]
	if !ok || len(channelEntries) == 0 {
		streamWaitersMu.Unlock()
		return stream
	}
	currMs, currSeqPtr, _ := parseEntryId(entryId)
	currSeq := *currSeqPtr
	for _, v := range channelEntries {
		waiterMs, waiterSeqPtr, _ := parseEntryId(v.ID)
		waiterSeq := *waiterSeqPtr
		if currMs > waiterMs || (currMs == waiterMs && currSeq >= waiterSeq) {
			ch := v.Channel
			streamWaiters[key] = channelEntries[1:]
			lastEntry := stream[len(stream)-1]
			fields := []string{}
			for k, val := range lastEntry {
				if k != "id" {
					fields = append(fields, k, val)
				}
			}
			ch <- encodeStreamEntries([]StreamEntry{{ID: lastEntry["id"], Fields: encodeArray(fields)}})
			break
		}
	}
	streamWaitersMu.Unlock()
	return stream
}

func handleXadd(_ *Client, args []string) string {
	// XADD streamKey entryId key value ...(key value)
	if len(args) < 5 {
		return encodeError("ERR wrong number of arguments for 'xadd' command")
	}

	streamKey := args[1]
	v, found := store[streamKey]
	stream := []map[string]string{}
	if found {
		if v.Kind != KindStream {
			return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		stream = v.Stream
	}
	entryId := args[2]
	entryId, errResp := validateEntryId(stream, entryId)
	if errResp != "" {
		return errResp
	}
	keyValuePairs := args[3:]
	entry := map[string]string{"id": entryId}
	for i := 0; i < len(keyValuePairs); i += 2 {
		entry[keyValuePairs[i]] = keyValuePairs[i+1]
	}
	stream = append(stream, entry)
	stream = checkStreamWaiters(streamKey, entryId, stream)
	setKey(streamKey, StoreValue{Kind: KindStream, Stream: stream})
	repl.propagate(args)
	return encodeBulkString(entryId)
}

func filterStream(stream []map[string]string, startMs int64, startSeq int, endMs int64, endSeq int) []StreamEntry {
	var entries []StreamEntry
	for _, entry := range stream {
		currentMs, currentSeqPtr, err := parseEntryId(entry["id"])
		if err != nil {
			break
		}
		currentSeq := *currentSeqPtr
		if currentMs > endMs || (currentMs == endMs && currentSeq > endSeq) {
			break
		}
		if currentMs > startMs || (currentMs == startMs && currentSeq >= startSeq) {
			fields := []string{}
			for k, v := range entry {
				if k != "id" {
					fields = append(fields, k, v)
				}
			}
			entries = append(entries, StreamEntry{ID: entry["id"], Fields: encodeArray(fields)})
		}
	}
	return entries
}

func encodeStreamEntries(entries []StreamEntry) string {
	resp := fmt.Sprintf("*%d\r\n", len(entries))
	for _, e := range entries {
		resp += "*2\r\n"
		resp += encodeBulkString(e.ID)
		resp += e.Fields
	}
	return resp
}

func handleXrange(_ *Client, args []string) string {
	if len(args) != 4 {
		return encodeError("ERR wrong number of arguments for 'xrange' command")
	}
	v, found := store[args[1]]
	if !found {
		return encodeArray([]string{})
	}

	startMs := int64(0)
	startSeq := 0
	if args[2] != "-" {
		parsedStartMs, startSeqPtr, err := parseEntryId(args[2])
		if err != nil {
			return encodeError("ERR value is not an integer or out of range")
		}
		startMs = parsedStartMs
		if startSeqPtr != nil {
			startSeq = *startSeqPtr
		}
	}
	endMs := int64(math.MaxInt64)
	endSeq := math.MaxInt64
	if args[3] != "+" {
		parsedEndMs, endSeqPtr, err := parseEntryId(args[3])
		if err != nil {
			return encodeError("ERR value is not an integer or out of range")
		}
		endMs = parsedEndMs
		if endSeqPtr != nil {
			endSeq = *endSeqPtr
		}
	}

	return encodeStreamEntries(filterStream(v.Stream, startMs, startSeq, endMs, endSeq))
}

func handleXread(_ *Client, args []string) string {
	if len(args) < 4 {
		return encodeError("ERR syntax error")
	}
	switch strings.ToUpper(args[1]) {
	case "BLOCK":
		if len(args) != 6 {
			return encodeError("ERR syntax error")
		}
		timeout, _ := strconv.Atoi(args[2])
		if strings.ToUpper(args[3]) != "STREAMS" {
			return encodeError("ERR syntax error")
		}
		streamKey := args[4]
		entryId := args[5]
		if entryId == "$" {
			v, found := store[streamKey]
			if found && v.Kind == KindStream {
				entries := v.Stream
				if len(entries) > 0 {
					entryId = entries[len(entries)-1]["id"]
				}
			} else {
				entryId = "0-0"
			}
		}
		ms, seqPtr, err := parseEntryId(entryId)
		if err != nil {
			return encodeError("ERR value is not an integer or out of range")
		}
		seq := 0
		if seqPtr != nil {
			seq = *seqPtr
		}
		seq++
		v, found := store[streamKey]
		if found && v.Kind == KindStream {
			entries := filterStream(v.Stream, ms, seq, math.MaxInt64, math.MaxInt64)
			if len(entries) > 0 {
				return "*1\r\n*2\r\n" + encodeBulkString(streamKey) + encodeStreamEntries(entries)
			}
		}

		ch := make(chan string, 1)
		streamWaitersMu.Lock()
		streamWaiters[streamKey] = append(streamWaiters[streamKey], StreamWaiterEntry{ID: entryId, Channel: ch})
		streamWaitersMu.Unlock()

		if timeout > 0 {
			expiryTime := time.Duration(timeout * int(time.Millisecond))
			time.AfterFunc(expiryTime, func() {
				streamWaitersMu.Lock()
				chans := streamWaiters[streamKey]
				for i, c := range chans {
					if c.Channel == ch {
						streamWaiters[streamKey] = append(chans[:i], chans[i+1:]...)
						close(ch)
						break
					}
				}
				streamWaitersMu.Unlock()
			})
		}

		encodedEntry, ok := <-ch
		if !ok {
			return encodeNullArray()
		}
		return "*1\r\n*2\r\n" + encodeBulkString(streamKey) + encodedEntry
	case "STREAMS":
		streamQueries := args[2:]
		half := len(streamQueries) / 2
		streamKeys := streamQueries[:half]
		streamEntryIds := streamQueries[half:]
		if len(streamKeys) != len(streamEntryIds) {
			return encodeError("ERR must have the same number of keys and entry id values")
		}

		resp := fmt.Sprintf("*%d\r\n", len(streamKeys))
		for i, streamKey := range streamKeys {
			entryId := streamEntryIds[i]
			ms, seqPtr, err := parseEntryId(entryId)
			if err != nil {
				return encodeError("ERR value is not an integer or out of range")
			}
			seq := 0
			if seqPtr != nil {
				seq = *seqPtr
			}
			// XREAD is exclusive of the given ID, so increment seq by 1
			seq++
			v, found := store[streamKey]
			entries := []StreamEntry{}
			if found && v.Kind == KindStream {
				entries = filterStream(v.Stream, ms, seq, math.MaxInt64, math.MaxInt64)
			}
			resp += "*2\r\n" + encodeBulkString(streamKey) + encodeStreamEntries(entries)
		}
		return resp
	default:
		return encodeError("ERR syntax error")
	}
}

func handleIncr(_ *Client, args []string) string {
	if len(args) != 2 {
		return encodeError("ERR syntax error")
	}
	key := args[1]
	v, found := store[key]
	num := 0
	if found {
		numValue, err := strconv.Atoi(v.S)
		if err != nil {
			return encodeError("ERR value is not an integer or out of range")
		}
		num = numValue
	}
	resNum := num + 1
	setKey(key, StoreValue{Kind: KindString, S: strconv.Itoa(resNum)})
	repl.propagate(args)
	return encodeInteger(resNum)
}

func handleMulti(c *Client, args []string) string {
	if len(args) != 1 {
		return encodeError("ERR syntax error")
	}
	if c.multiCommands != nil {
		return encodeError("ERR MULTI calls can not be nested")
	}
	c.multiCommands = []func() string{}
	return encodeSimpleString("OK")
}

func handleExec(c *Client, args []string) string {
	if len(args) != 1 {
		return encodeError("ERR syntax error")
	}
	if c.multiCommands == nil {
		return encodeError("ERR EXEC without MULTI")
	}
	isWatchMutated := false
	versionsMu.Lock()
	for key, version := range c.watched {
		storeVersion := versions[key]
		if storeVersion != version {
			isWatchMutated = true
			break
		}
	}
	versionsMu.Unlock()
	resp := encodeNullArray()
	if !isWatchMutated {
		resp = fmt.Sprintf("*%d\r\n", len(c.multiCommands))
		for _, queuedCall := range c.multiCommands {
			resp += queuedCall()
		}
	}
	c.multiCommands = nil
	c.watched = nil
	return resp
}

func handleDiscard(c *Client, args []string) string {
	if len(args) != 1 {
		return encodeError("ERR syntax error")
	}
	if c.multiCommands == nil {
		return encodeError("ERR DISCARD without MULTI")
	}
	c.multiCommands = nil
	c.watched = nil
	return encodeSimpleString("OK")
}

func handleWatch(c *Client, args []string) string {
	if len(args) < 2 {
		return encodeError("ERR syntax error")
	}
	if c.multiCommands != nil {
		return encodeError("ERR WATCH inside MULTI is not allowed")
	}
	keys := args[1:]
	versionsMu.Lock()
	for _, k := range keys {
		v := versions[k]
		c.watched[k] = v
	}
	versionsMu.Unlock()
	return encodeSimpleString("OK")
}

func handleUnwatch(c *Client, args []string) string {
	if len(args) != 1 {
		return encodeError("ERR syntax error")
	}
	c.watched = nil
	return encodeSimpleString("OK")
}

func handleInfo(c *Client, args []string) string {
	if len(args) != 2 {
		return encodeError("ERR syntax error")
	}
	output := fmt.Sprintf("role:%s\r\n", c.role)
	if c.role == "master" {
		output += fmt.Sprintf("master_replid:%s\r\n", repl.replid)
		output += fmt.Sprintf("master_repl_offset:%d\r\n", repl.offset)
	}
	return encodeBulkString(output)
}

func handleReplconf(c *Client, args []string) string {
	if len(args) < 2 {
		return encodeError("ERR syntax error")
	}

	replType := strings.ToLower(args[1])

	switch replType {
	case "listening-port":
		if len(args) != 3 {
			return encodeError("ERR syntax error")
		}
		return encodeSimpleString("OK")
	case "capa":
		if len(args) != 3 {
			return encodeError("ERR syntax error")
		}
		return encodeSimpleString("OK")
	case "getack":
		return encodeArray([]string{"REPLCONF", "ACK", strconv.Itoa(c.replOffset)})
	default:
		return encodeError("ERR unknown REPLCONF option")
	}
}

func handlePsync(c *Client, args []string) string {
	if len(args) != 3 {
		return encodeError("ERR syntax error")
	}

	resString := "FULLRESYNC " + repl.replid + " " + strconv.Itoa(repl.offset)
	c.conn.Write([]byte(encodeSimpleString(resString)))

	rdbBytes, _ := hex.DecodeString(emptyRDBHex)
	c.conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(rdbBytes))))
	c.conn.Write(rdbBytes)

	repl.addReplica(c.conn)
	return ""
}

var commandHandlers = map[string]func(*Client, []string) string{
	"PING":    handlePing,
	"ECHO":    handleEcho,
	"GET":     handleGet,
	"SET":     handleSet,
	"RPUSH":   handleRpush,
	"LPUSH":   handleLpush,
	"LLEN":    handleLlen,
	"LRANGE":  handleLrange,
	"LPOP":    handleLpop,
	"BLPOP":   handleBlpop,
	"TYPE":    handleType,
	"XADD":    handleXadd,
	"XRANGE":  handleXrange,
	"XREAD":   handleXread,
	"INCR":    handleIncr,
	"MULTI":   handleMulti,
	"EXEC":    handleExec,
	"DISCARD": handleDiscard,
	"WATCH":   handleWatch,
	"UNWATCH": handleUnwatch,
	"INFO": handleInfo,
	"REPLCONF": handleReplconf,
	"PSYNC": handlePsync,
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

func startReplicationClient(masterHost string, masterPort string, ownPort string) error {
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

	// Read FULLRESYNC <replid> <offset>
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

	// Read RDB: $<len>\r\n<bytes>  (no trailing \r\n after bytes)
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

	// Receive and apply write commands from master indefinitely
	replicaClient := &Client{conn: conn, watched: map[string]uint64{}, role: "slave"}
	for {
		args, err := parseCommand(reader)
		if err != nil {
			conn.Close()
			return fmt.Errorf("replication stream ended: %w", err)
		}
		cmd := strings.ToUpper(args[0])
		if handler, ok := commandHandlers[cmd]; ok {
			if resp := handler(replicaClient, args); resp != "" {
				conn.Write([]byte(resp))
			}
		}
		if cmd != "REPLCONF" {
			replicaClient.replOffset += len(encodeArray(args))
		}
	}
}

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
		// pass in whether replica or master
		go handleConn(conn, replicaVal)
	}
}
