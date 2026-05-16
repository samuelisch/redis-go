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

var store = map[string]StoreValue{}
var versions = map[string]uint64{}
var waiters = map[string][]chan string{}
var streamWaiters = map[string][]StreamWaiterEntry{}
var storeMu sync.RWMutex
var versionsMu sync.RWMutex
var waitersMu sync.RWMutex
var streamWaitersMu sync.RWMutex

func setKey(key string, value StoreValue) {
	storeMu.Lock()
	versionsMu.Lock()
	store[key] = value
	versions[key]++
	storeMu.Unlock()
	versionsMu.Unlock()
}

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
