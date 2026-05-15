# Redis Replication Design

**Date:** 2026-05-15
**Scope:** Full sync replication — RDB transfer + command propagation + offset tracking

---

## Context

This is a from-scratch Redis implementation in Go (single binary, `app/main.go`).
The PSYNC handshake skeleton is already in place: the replica can connect, send
PING / REPLCONF / PSYNC, and the master responds with FULLRESYNC. What's missing
is everything after that response: RDB transfer, command propagation, and offset
tracking.

---

## What the Protocol Requires

```
Replica → Master:  PSYNC ? -1
Master  → Replica: +FULLRESYNC <replid> 0\r\n
Master  → Replica: $<rdb-len>\r\n<rdb-bytes>     ← no trailing \r\n
Master  → Replica: <write commands as RESP, forever>
Replica → Master:  REPLCONF ACK <offset>           ← periodically
```

The RDB will be a hardcoded empty blob (valid Redis RDB format, zero keys).
This is standard practice for protocol learning and matches what codecrafters
expects.

---

## Architecture: ReplicationManager (Approach B)

A single `ReplicationManager` is created at startup and passed into the
connection handler. It is the only place replication state lives.

```
┌─────────────────────────────────────────────┐
│              ReplicationManager              │
│  replid   string                             │
│  offset   int                                │
│  replicas []*ReplicaConn  ← one per replica  │
└─────────────────────────────────────────────┘
         │ propagate(cmd []string)
         ▼
  ┌──────────────┐   ┌──────────────┐
  │ ReplicaConn  │   │ ReplicaConn  │   ...
  │ writer       │   │ writer       │
  └──────────────┘   └──────────────┘
```

### Why not inline (Approach A)?

Putting replica tracking in globals and making each write handler call
`propagate()` directly means every handler becomes replication-aware. The
`ReplicationManager` boundary keeps that concern in one place — mirroring how
Redis's own `server.h` separates replication state from command dispatch.

---

## Section 1: ReplicationManager

```go
type ReplicationManager struct {
    mu       sync.Mutex
    replid   string
    offset   int
    replicas []*ReplicaConn
}

type ReplicaConn struct {
    writer *bufio.Writer
}
```

- **`replid`** — the 40-char alphanumeric ID advertised in INFO and FULLRESYNC.
  Currently hardcoded in `handleConn`; moves here.
- **`offset`** — total bytes of write commands propagated so far. Starts at 0
  after full sync.
- **`replicas`** — slice of active replica writers. Protected by `mu`. When a
  replica disconnects, it is removed from this slice.

---

## Section 2: Master Side

### PSYNC handler

When the master receives `PSYNC ? -1` it must, on that same connection:

1. Write `+FULLRESYNC <replid> 0\r\n`
2. Decode the hardcoded empty RDB hex → raw bytes
3. Write `$<len>\r\n<rdb-bytes>` — **no trailing `\r\n`** after the binary
4. Call `repl.addReplica(conn)` — register the connection's buffered writer
5. Return without closing the connection (the conn is now a replication stream)

The `handlePsync` handler returns a sentinel (empty string or a dedicated
signal) so that `handleConn` breaks out of its read loop after PSYNC. A
separate goroutine is NOT needed on the master side — the replica conn is
registered into `ReplicationManager` and written to by `propagate()`, which
runs in the goroutine of whichever client triggered the write.

### propagate()

```go
func (r *ReplicationManager) propagate(args []string) {
    encoded := encodeArray(args)
    r.mu.Lock()
    defer r.mu.Unlock()
    r.offset += len(encoded)
    for _, replica := range r.replicas {
        replica.writer.WriteString(encoded)
        replica.writer.Flush()
    }
}
```

Called after every write command succeeds on the master. Write commands:
SET, RPUSH, LPUSH, LPOP, INCR, XADD (and any others that mutate state).

---

## Section 3: Replica Side

`startReplicationClient` currently exits after sending PSYNC. Instead it must:

1. Read and discard the `+FULLRESYNC ...` line
2. Parse `$<len>` and read exactly `<len>` bytes to discard the RDB blob
3. Enter a receive loop:
   - `parseCommand(reader)` — reuse the existing parser
   - Look up the command in `commandHandlers`
   - Apply it (no response written back to master)
   - Add `len(raw_command_bytes)` to a local `offset` counter
4. Send `REPLCONF ACK <offset>` periodically (or after each command for simplicity)

The replica applies commands silently. It does not respond to propagated writes
the same way it would respond to client writes — the master is not waiting for
a reply per command.

---

## Data Flow Summary

```
Client → Master:  SET foo bar
Master:           applies SET locally
                  calls propagate(["SET", "foo", "bar"])
                  → writes RESP to each replica writer
Replica:          receives RESP array
                  calls handleSet(client, args)  ← same handler, no response
                  increments local offset
                  sends REPLCONF ACK <offset>
```

---

## What This Design Does NOT Cover

- **Partial resync**: If a replica reconnects with a known replid+offset, the
  master could replay from a backlog instead of full RDB. Not in scope here.
- **Backlog buffer**: Required for partial resync. Not in scope.
- **WAIT command**: Blocks master until N replicas have acknowledged up to a
  given offset. Not in scope.
- **Real RDB encoding**: The RDB is a hardcoded empty blob. Actual key
  serialization is a separate concern.
- **Replica-of-replica**: Not in scope.

---

## Files Affected

- `app/main.go` — all changes land here (single-file repo)
  - Add `ReplicationManager` and `ReplicaConn` types
  - Move `masterReplid` from `Client` to `ReplicationManager`
  - Update `handlePsync` to send RDB and register replica
  - Add `propagate()` call to write handlers
  - Extend `startReplicationClient` to receive and apply commands
  - Fix existing bug: `storeMu.RLock()` → `storeMu.Lock()` in `setKey`
