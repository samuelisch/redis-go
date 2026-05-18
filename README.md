# redis-go

A Redis server built from scratch, as part of the [CodeCrafters "Build Your Own Redis"](https://codecrafters.io/challenges/redis) challenge. I decided to build this in Go to get hands-on experience with its built-in concurrency model (goroutines and channels) while implementing a Redis server from scratch.

---

## What this is

Progressively less-broken Redis. Every commit gets asserted against unit tests to check functionality, so what I've done here is solving each functionality of Redis, honing in on correctness of outputs, before optimising for edge cases.

The final server handles:

- **Core Commands**: `PING`, `ECHO`, `INCR`
- **Strings**: `GET`, `SET` (with `EX`/`PX`)
- **Lists**: `LPUSH`, `RPUSH`, `LPOP`, `LRANGE`, `LLEN`
- **Blocking lists**: `BLPOP`
- **Streams**: `XADD`, `XRANGE`, `XREAD BLOCK`
- **Transactions**: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`
- **Replication**: `INFO`, `REPLCONF`, `PSYNC`, `WAIT`

Still to come: RDB persistence, AOF persistence, Pub/Sub, Sorted Sets, Geospatial, and Authentication.

I'm sure that my implementation here isn't exactly the most accurate to what the Redis team has built, there are so many edge cases that they handle gracefully. This exercise is mainly an exploratory one, that drills an understanding of how Redis works below the hood.

---

## How it was built

### 1. TCP listener → PING

The most basic, initial working version that accepts a connection, read into a raw byte buffer, write `+PONG\r\n` back, close.

```
conn.Read(buf)
conn.Write([]byte("+PONG\r\n"))
conn.Close()
```

Handles a single connection to start off. Had to read up on goroutines, fun stuff that node handles out of the box.

---

### 2. Concurrent connections

The loop came next: `for { conn := listener.Accept(); go handleConn(conn) }`. One goroutine per client, nothing shared yet between them. I get a false sense of "Hey this isn't that bad."

---

### 3. RESP parsing

Realised raw `conn.Read` doesn't give you clean message boundaries. Switched to `bufio.NewReader` + `ReadString('\n')` to walk the RESP wire format properly. The RESP array format (`*<n>\r\n$<len>\r\n<value>\r\n` per argument) is why you can't just split on spaces — binary values, newlines in payloads, etc. The parser reads arg count first, then reads each bulk string by length prefix.

---

### 4. Key-value store + typed values

A plain `map[string]string` would have worked for `GET`/`SET` but breaks as soon as you add lists and streams. Added a `StoreValue` struct with a `Kind` discriminant:

```go
type StoreValue struct {
    Kind   ValueKind  // KindString, KindStringList, KindStream, ...
    S      string
    Slice  []string
    Stream []map[string]string
}
```

`GET` and `SET` only touch `KindString`. Everything else checks `Kind` first and returns `WRONGTYPE` if it doesn't match. Tried wrapping my head around structs by comparing them to TypeScript interfaces. 

---

### 5. Key expiry — and a logic bug

Schedules deletetion via `time.AfterFunc` when dealing with `EX` and `PX`.

---

### 6. Lists

`RPUSH` appends, `LPUSH` prepends (reversing order for multiple args), `LPOP` removes from front. `LRANGE` with negative indices needs clamping — `-1` means last element, so `end = len(list) + end` then clamp to `[0, len-1]`.

Got really dumbfounded with `LPOP`. why not `SHIFT`? I guess it has to deal with stacks and queues too.

---

### 7. Blocking lists — channels as waiters

`BLPOP` blocks until an element is pushed or a timeout fires. The implementation uses a Go channel per waiting client:

```go
ch := make(chan string, 1)
store.waiters[key] = append(store.waiters[key], ch)
// ...
element, ok := <-ch  // blocks here
```

Channels are something entirely new to me at this point, since the event loop with js/ts is single threaded, we don't need to worry about data races like in Go's goroutines, as there are no shared memory with multithreading structure.

---

### 8. Streams

`XADD` takes a stream key and an entry ID in `<milliseconds>-<sequence>` format. The `*` wildcard fills in the current Unix millisecond; `1234-*` fills in only the sequence. Validation enforces that each new ID is strictly greater than the last — both the ms and sequence parts.

`XRANGE` filters entries between two IDs (supporting `-` and `+` as min/max). `XREAD BLOCK` reuses the same channel pattern as `BLPOP` — a goroutine parks on a channel, and `XADD` wakes it when a matching entry arrives.

---

### 9. INCR

A simple increment on a key value. 

---

### 10. Transactions — MULTI/EXEC/DISCARD

`MULTI` opens a transaction by allocating a `[]func() string` queue on the client. While queued, every command is wrapped in a closure and appended instead of executed:

```go
client.multiCommands = append(client.multiCommands, func() string {
    return handler(server, client, args)
})
conn.Write([]byte("+QUEUED\r\n"))
```

---

### 11. WATCH — optimistic concurrency

`WATCH key` records the current version of a key for the client. `EXEC` checks whether any watched key was mutated since `WATCH` was called. If so, it returns a null array instead of running the queue.

Each `SET` increments a version counter:

```go
func (st *Store) set(key string, value StoreValue) {
    st.mu.Lock()
    st.versionsMu.Lock()
    st.data[key] = value
    st.versions[key]++
    // ...
}
```

`WATCH` snapshots the current version; `EXEC` compares it. If they diverge, we abort the execution.

---

### 12. Replication handshake

A replica connects to a master and runs a fixed handshake before receiving any commands:

```
→ PING
← +PONG
→ REPLCONF listening-port <port>
← +OK
→ REPLCONF capa psync2
← +OK
→ PSYNC ? -1
← +FULLRESYNC <replid> <offset>
← $<rdb-len>\r\n<rdb-bytes>   ← no trailing \r\n
```

The `PSYNC ? -1` tells the master "I have no prior state, give me everything." The master replies with `FULLRESYNC`, then immediately sends an RDB snapshot (a hardcoded empty blob here), then starts streaming write commands. I haven't expanded from there, but cool to know what happens right after handshake so that the replica has all the context of the master server.

---

### 13. Command propagation

After every write command on the master, `propagate()` encodes the command as a RESP array and writes it to every connected replica's buffered writer. The master's `offset` tracks total bytes propagated — replicas echo this back via `REPLCONF ACK <offset>`.

Replicas apply commands silently: same handlers, same store mutations, no reply written back. We'd handle them with `REPLCONF GETACK`.

---

### 14. WAIT

`WAIT <numreplicas> <timeout>` blocks until N replicas have acknowledged the master's current offset. Implementation: send `REPLCONF GETACK *` to all replicas, then poll `rc.knownOffset >= masterOffset` in a `time.Sleep(10ms)` loop until the deadline or enough ACKs land.

Not very elegant, but it works and matches the expected behavior. Reading into Redis' actual implementation, seems like the main server executing `WAIT` is put to sleep until the required amount of replicas has caught up to the offset, or the timer runs out. 

---

## File layout

```
app/
  main.go        — entry point, flag parsing, TCP accept loop
  server.go      — Server and Client types, connection handler
  store.go       — Store: get/set/expiry/waiters
  protocol.go    — RESP encoder/decoder
  commands.go    — one handleX function per command
  replication.go — ReplicationManager, replica handshake client
```

Split from a single `main.go` after it became quite unwieldy. It required a change in how we defined the server shape, previously i was storing some variables globally (store, mutex, etc.). This architectural change made declarations more explicit, and easier to know what dependencies are used in each method.
