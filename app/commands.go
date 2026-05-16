package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

func handlePing(server *Server, client *Client, args []string) string {
	return encodeSimpleString("PONG")
}

func handleEcho(server *Server, client *Client, args []string) string {
	if len(args) < 2 {
		return encodeError("ERR wrong number of arguments for 'echo' command")
	}
	return encodeBulkString(args[1])
}

func handleGet(server *Server, client *Client, args []string) string {
	if len(args) < 2 {
		return encodeError("ERR wrong number of arguments for 'get' command")
	}
	v, found := server.store.get(args[1])
	if !found {
		return encodeNullBulk()
	}
	if v.Kind != KindString {
		return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return encodeBulkString(v.S)
}

func handleSet(server *Server, client *Client, args []string) string {
	if len(args) < 3 {
		return encodeError("ERR wrong number of arguments for 'set' command")
	}
	server.store.set(args[1], StoreValue{Kind: KindString, S: args[2]})
	if len(args) >= 5 {
		timeoutType := strings.ToUpper(args[3])
		if timeoutType == "PX" || timeoutType == "EX" {
			expiryValue, _ := strconv.ParseInt(args[4], 10, 64)
			server.store.setExpiry(ExpiryType(timeoutType), expiryValue, args[1])
		}
	}
	server.repl.propagate(args)
	return encodeSimpleString("OK")
}

func handleRpush(server *Server, client *Client, args []string) string {
	if len(args) < 3 {
		return encodeError("ERR wrong number of arguments for 'rpush' command")
	}
	v, found := server.store.get(args[1])
	list := []string{}
	if found {
		if v.Kind != KindStringList {
			return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		list = v.Slice
	}
	list = append(list, args[2:]...)
	count := len(list)
	list = server.store.checkWaiters(args[1], list)
	server.store.set(args[1], StoreValue{Kind: KindStringList, Slice: list})
	server.repl.propagate(args)
	return encodeInteger(count)
}

func handleLpush(server *Server, client *Client, args []string) string {
	if len(args) < 3 {
		return encodeError("ERR wrong number of arguments for 'lpush' command")
	}
	v, found := server.store.get(args[1])
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
	list = server.store.checkWaiters(args[1], list)
	server.store.set(args[1], StoreValue{Kind: KindStringList, Slice: list})
	server.repl.propagate(args)
	return encodeInteger(count)
}

func handleLlen(server *Server, client *Client, args []string) string {
	if len(args) != 2 {
		return encodeError("ERR wrong number of arguments for 'llen' command")
	}
	v, found := server.store.get(args[1])
	if !found {
		return encodeInteger(0)
	}
	return encodeInteger(len(v.Slice))
}

func handleLrange(server *Server, client *Client, args []string) string {
	if len(args) != 4 {
		return encodeError("ERR wrong number of arguments for 'lrange' command")
	}
	v, found := server.store.get(args[1])
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

func handleLpop(server *Server, client *Client, args []string) string {
	if len(args) < 2 || len(args) > 3 {
		return encodeError("ERR wrong number of arguments for 'lpop' command")
	}
	v, found := server.store.get(args[1])
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
			server.store.set(args[1], StoreValue{Kind: KindStringList, Slice: []string{}})
			server.repl.propagate(args)
			return encodeArray(list)
		}
		var removed []string
		for i := 0; i < numToRemove; i++ {
			element := list[0]
			removed = append(removed, element)
			list = list[1:]
		}
		server.store.set(args[1], StoreValue{Kind: KindStringList, Slice: list})
		server.repl.propagate(args)
		return encodeArray(removed)
	}

	element := list[0]
	server.store.set(args[1], StoreValue{Kind: KindStringList, Slice: list[1:]})
	server.repl.propagate(args)
	return encodeBulkString(element)
}

func handleBlpop(server *Server, client *Client, args []string) string {
	if len(args) != 3 {
		return encodeError("ERR wrong number of arguments for 'lpop' command")
	}
	key := args[1]
	timeout, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return encodeError("ERR value is not an integer or out of range")
	}

	v, found := server.store.get(key)
	if found && v.Kind == KindStringList && len(v.Slice) > 0 {
		element := v.Slice[0]
		server.store.set(key, StoreValue{Kind: KindStringList, Slice: v.Slice[1:]})
		return encodeArray([]string{key, element})
	}

	ch := make(chan string, 1)
	server.store.waitersMu.Lock()
	server.store.waiters[key] = append(server.store.waiters[key], ch)
	server.store.waitersMu.Unlock()

	if timeout > 0 {
		expiryTime := time.Duration(timeout * float64(time.Second))
		time.AfterFunc(expiryTime, func() {
			server.store.waitersMu.Lock()
			chans := server.store.waiters[key]
			for i, c := range chans {
				if c == ch {
					server.store.waiters[key] = append(chans[:i], chans[i+1:]...)
					close(ch)
					break
				}
			}
			server.store.waitersMu.Unlock()
		})
	}

	element, ok := <-ch
	if !ok {
		return encodeNullArray()
	}

	return encodeArray([]string{key, element})
}

func handleType(server *Server, client *Client, args []string) string {
	if len(args) != 2 {
		return encodeError("ERR wrong number of arguments for 'type' command")
	}
	v, found := server.store.get(args[1])
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

func handleXadd(server *Server, client *Client, args []string) string {
	if len(args) < 5 {
		return encodeError("ERR wrong number of arguments for 'xadd' command")
	}

	streamKey := args[1]
	v, found := server.store.get(streamKey)
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
	stream = server.store.checkStreamWaiters(streamKey, entryId, stream)
	server.store.set(streamKey, StoreValue{Kind: KindStream, Stream: stream})
	server.repl.propagate(args)
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

func handleXrange(server *Server, client *Client, args []string) string {
	if len(args) != 4 {
		return encodeError("ERR wrong number of arguments for 'xrange' command")
	}
	v, found := server.store.get(args[1])
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

func handleXread(server *Server, client *Client, args []string) string {
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
			v, found := server.store.get(streamKey)
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
		v, found := server.store.get(streamKey)
		if found && v.Kind == KindStream {
			entries := filterStream(v.Stream, ms, seq, math.MaxInt64, math.MaxInt64)
			if len(entries) > 0 {
				return "*1\r\n*2\r\n" + encodeBulkString(streamKey) + encodeStreamEntries(entries)
			}
		}

		ch := make(chan string, 1)
		server.store.streamWaitersMu.Lock()
		server.store.streamWaiters[streamKey] = append(server.store.streamWaiters[streamKey], StreamWaiterEntry{ID: entryId, Channel: ch})
		server.store.streamWaitersMu.Unlock()

		if timeout > 0 {
			expiryTime := time.Duration(timeout * int(time.Millisecond))
			time.AfterFunc(expiryTime, func() {
				server.store.streamWaitersMu.Lock()
				chans := server.store.streamWaiters[streamKey]
				for i, c := range chans {
					if c.Channel == ch {
						server.store.streamWaiters[streamKey] = append(chans[:i], chans[i+1:]...)
						close(ch)
						break
					}
				}
				server.store.streamWaitersMu.Unlock()
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
			seq++
			v, found := server.store.get(streamKey)
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

func handleIncr(server *Server, client *Client, args []string) string {
	if len(args) != 2 {
		return encodeError("ERR syntax error")
	}
	key := args[1]
	v, found := server.store.get(key)
	num := 0
	if found {
		numValue, err := strconv.Atoi(v.S)
		if err != nil {
			return encodeError("ERR value is not an integer or out of range")
		}
		num = numValue
	}
	resNum := num + 1
	server.store.set(key, StoreValue{Kind: KindString, S: strconv.Itoa(resNum)})
	server.repl.propagate(args)
	return encodeInteger(resNum)
}

func handleMulti(server *Server, client *Client, args []string) string {
	if len(args) != 1 {
		return encodeError("ERR syntax error")
	}
	if client.multiCommands != nil {
		return encodeError("ERR MULTI calls can not be nested")
	}
	client.multiCommands = []func() string{}
	return encodeSimpleString("OK")
}

func handleExec(server *Server, client *Client, args []string) string {
	if len(args) != 1 {
		return encodeError("ERR syntax error")
	}
	if client.multiCommands == nil {
		return encodeError("ERR EXEC without MULTI")
	}
	isWatchMutated := false
	server.store.versionsMu.Lock()
	for key, version := range client.watched {
		storeVersion := server.store.versions[key]
		if storeVersion != version {
			isWatchMutated = true
			break
		}
	}
	server.store.versionsMu.Unlock()
	resp := encodeNullArray()
	if !isWatchMutated {
		resp = fmt.Sprintf("*%d\r\n", len(client.multiCommands))
		for _, queuedCall := range client.multiCommands {
			resp += queuedCall()
		}
	}
	client.multiCommands = nil
	client.watched = nil
	return resp
}

func handleDiscard(server *Server, client *Client, args []string) string {
	if len(args) != 1 {
		return encodeError("ERR syntax error")
	}
	if client.multiCommands == nil {
		return encodeError("ERR DISCARD without MULTI")
	}
	client.multiCommands = nil
	client.watched = nil
	return encodeSimpleString("OK")
}

func handleWatch(server *Server, client *Client, args []string) string {
	if len(args) < 2 {
		return encodeError("ERR syntax error")
	}
	if client.multiCommands != nil {
		return encodeError("ERR WATCH inside MULTI is not allowed")
	}
	keys := args[1:]
	server.store.versionsMu.Lock()
	for _, k := range keys {
		v := server.store.versions[k]
		client.watched[k] = v
	}
	server.store.versionsMu.Unlock()
	return encodeSimpleString("OK")
}

func handleUnwatch(server *Server, client *Client, args []string) string {
	if len(args) != 1 {
		return encodeError("ERR syntax error")
	}
	client.watched = nil
	return encodeSimpleString("OK")
}

func handleInfo(server *Server, client *Client, args []string) string {
	if len(args) != 2 {
		return encodeError("ERR syntax error")
	}
	output := fmt.Sprintf("role:%s\r\n", server.role)
	if server.role == "master" {
		output += fmt.Sprintf("master_replid:%s\r\n", server.repl.replid)
		output += fmt.Sprintf("master_repl_offset:%d\r\n", server.repl.offset)
	}
	return encodeBulkString(output)
}

func handleReplconf(server *Server, client *Client, args []string) string {
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
		return encodeArray([]string{"REPLCONF", "ACK", strconv.Itoa(client.replOffset)})
	default:
		return encodeError("ERR unknown REPLCONF option")
	}
}

func handlePsync(server *Server, client *Client, args []string) string {
	if len(args) != 3 {
		return encodeError("ERR syntax error")
	}

	resString := "FULLRESYNC " + server.repl.replid + " " + strconv.Itoa(server.repl.offset)
	client.conn.Write([]byte(encodeSimpleString(resString)))

	rdbBytes, _ := hex.DecodeString(emptyRDBHex)
	client.conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(rdbBytes))))
	client.conn.Write(rdbBytes)

	server.repl.addReplica(client.conn)
	return ""
}

func handleWait(server *Server, client *Client, args []string) string {
	if len(args) != 3 {
		return encodeError("ERR syntax error")
	}
	numReplicas, err := strconv.Atoi(args[1])
	if err != nil {
		return encodeError("ERR value is not an integer or out of range")
	}
	timeoutMs, err := strconv.Atoi(args[2])
	if err != nil {
		return encodeError("ERR value is not an integer or out of range")
	}

	server.repl.mu.Lock()
	masterOffset := server.repl.offset
	server.repl.mu.Unlock()

	countCaughtUp := func() int {
		server.repl.mu.Lock()
		defer server.repl.mu.Unlock()
		count := 0
		for _, rc := range server.repl.replicas {
			if rc.knownOffset >= masterOffset {
				count++
			}
		}
		return count
	}

	if masterOffset == 0 {
		server.repl.mu.Lock()
		n := len(server.repl.replicas)
		server.repl.mu.Unlock()
		return encodeInteger(n)
	}

	if countCaughtUp() >= numReplicas {
		return encodeInteger(countCaughtUp())
	}

	server.repl.sendGetAck()

	if timeoutMs == 0 {
		return encodeInteger(countCaughtUp())
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := countCaughtUp(); n >= numReplicas {
			return encodeInteger(n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return encodeInteger(countCaughtUp())
}

func handleConfig(server *Server, client *Client, args []string) string {
	if len(args) < 3 {
		return encodeError("ERR syntax error")
	}
	subCmd := strings.ToUpper(args[1])
	if subCmd != "GET" {
		return encodeError("ERR unsupported CONFIG subcommand: " + args[1])
	}
	param := strings.ToLower(args[2])
	configMap := map[string]string{
		"dir":        server.dir,
		"dbfilename": server.dbfilename,
	}
	val, ok := configMap[param]
	if !ok {
		return encodeArray([]string{})
	}
	return encodeArray([]string{param, val})
}

var commandHandlers = map[string]func(*Server, *Client, []string) string{
	"PING":     handlePing,
	"ECHO":     handleEcho,
	"GET":      handleGet,
	"SET":      handleSet,
	"RPUSH":    handleRpush,
	"LPUSH":    handleLpush,
	"LLEN":     handleLlen,
	"LRANGE":   handleLrange,
	"LPOP":     handleLpop,
	"BLPOP":    handleBlpop,
	"TYPE":     handleType,
	"XADD":     handleXadd,
	"XRANGE":   handleXrange,
	"XREAD":    handleXread,
	"INCR":     handleIncr,
	"MULTI":    handleMulti,
	"EXEC":     handleExec,
	"DISCARD":  handleDiscard,
	"WATCH":    handleWatch,
	"UNWATCH":  handleUnwatch,
	"INFO":     handleInfo,
	"REPLCONF": handleReplconf,
	"PSYNC":    handlePsync,
	"WAIT":     handleWait,
	"CONFIG": handleConfig,
}
