package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

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

func handleWait(c *Client, args []string) string {
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

	repl.mu.Lock()
	masterOffset := repl.offset
	repl.mu.Unlock()

	countCaughtUp := func() int {
		repl.mu.Lock()
		defer repl.mu.Unlock()
		count := 0
		for _, rc := range repl.replicas {
			if rc.knownOffset >= masterOffset {
				count++
			}
		}
		return count
	}

	if masterOffset == 0 {
		repl.mu.Lock()
		n := len(repl.replicas)
		repl.mu.Unlock()
		return encodeInteger(n)
	}

	if countCaughtUp() >= numReplicas {
		return encodeInteger(countCaughtUp())
	}

	repl.sendGetAck()

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

var commandHandlers = map[string]func(*Client, []string) string{
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
}
