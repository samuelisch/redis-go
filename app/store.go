package main

import (
	"fmt"
	"sync"
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

type Store struct {
	mu              sync.RWMutex
	versionsMu      sync.RWMutex
	waitersMu       sync.RWMutex
	streamWaitersMu sync.RWMutex
	data            map[string]StoreValue
	versions        map[string]uint64
	waiters         map[string][]chan string
	streamWaiters   map[string][]StreamWaiterEntry
}

func newStore() *Store {
	return &Store{
		data:          map[string]StoreValue{},
		versions:      map[string]uint64{},
		waiters:       map[string][]chan string{},
		streamWaiters: map[string][]StreamWaiterEntry{},
	}
}

func (st *Store) set(key string, value StoreValue) {
	st.mu.Lock()
	st.versionsMu.Lock()
	st.data[key] = value
	st.versions[key]++
	st.mu.Unlock()
	st.versionsMu.Unlock()
}

func (st *Store) get(key string) (StoreValue, bool) {
	v, ok := st.data[key]
	return v, ok
}

func (st *Store) setExpiry(expiryType ExpiryType, ttl int64, storedKey string) {
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
		st.mu.Lock()
		delete(st.data, storedKey)
		st.mu.Unlock()
	})
}

func (st *Store) checkWaiters(key string, list []string) []string {
	st.waitersMu.Lock()
	for len(list) > 0 {
		chans, ok := st.waiters[key]
		if !ok || len(chans) == 0 {
			break
		}
		ch := chans[0]
		st.waiters[key] = chans[1:]
		ch <- list[0]
		list = list[1:]
	}
	st.waitersMu.Unlock()
	return list
}

func (st *Store) checkStreamWaiters(key string, entryId string, stream []map[string]string) []map[string]string {
	st.streamWaitersMu.Lock()
	channelEntries, ok := st.streamWaiters[key]
	if !ok || len(channelEntries) == 0 {
		st.streamWaitersMu.Unlock()
		return stream
	}
	currMs, currSeqPtr, _ := parseEntryId(entryId)
	currSeq := *currSeqPtr
	for _, v := range channelEntries {
		waiterMs, waiterSeqPtr, _ := parseEntryId(v.ID)
		waiterSeq := *waiterSeqPtr
		if currMs > waiterMs || (currMs == waiterMs && currSeq >= waiterSeq) {
			ch := v.Channel
			st.streamWaiters[key] = channelEntries[1:]
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
	st.streamWaitersMu.Unlock()
	return stream
}
