# MangoDB Architecture

MangoDB is a distributed key-value store inspired by Riak's BitCask storage model. It's designed for high write throughput using append-only logs, with a cluster coordination layer called MangoTree.

---

## System Overview

```
                      ┌──────────────┐
                      │   MangoTree  │  Cluster coordinator
                      │  (port 9090) │  Registration, routing, health
                      └──────┬───────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │ Primary  │  │Secondary │  │Secondary │
        │ (8082)   │──│ (8083)   │  │ (8084)   │
        └──────────┘  └──────────┘  └──────────┘
              │              ▲              ▲
              │   async      │              │
              └──────────────┴──────────────┘
                    replication
```

A deployment consists of:
- **MangoTree** - A cluster coordinator that tracks node membership and routes requests.
- **Primary node** - Accepts all writes. Asynchronously replicates to secondaries.
- **Secondary nodes** - Receive replicated writes. Can serve reads.
- **Clients** - Connect via TCP using a simple text protocol.

---

## Components

### 1. MangoApp (Entry Point)

`MangoApp.java` bootstraps either a MangoTree instance or a MangoServer based on command-line arguments. A node is started with a port and a role (`PRIMARY` or `SECONDARY`).

### 2. Network Layer - MangoServer

`MangoServer.java` is a TCP server that listens on a configurable port (default 8082).

**How it works:**
1. Accepts incoming TCP connections on a `ServerSocket`.
2. Hands each connection to a thread from a fixed thread pool (default 500 threads).
3. Each handler thread reads newline-delimited commands from a `BufferedReader`.
4. Commands are passed to `CommandProcessor` for parsing and execution.
5. Responses are written back via `PrintWriter`.

The server supports an "ordered response" mode where it processes commands sequentially to preserve ordering guarantees.

### 3. Command Processing

Commands flow through two stages:

**CommandParser** parses raw text into `Command` objects:
```
PUT key value  →  Command(type=PUT, params=["key", "value"])
GET key        →  Command(type=GET, params=["key"])
DELETE key     →  Command(type=DELETE, params=["key"])
```

Supported commands: `PUT`, `GET`, `DELETE`, `EXISTS`, `FLUSH`, `STATUS`, `HELP`, `HEARTBEAT`, `REGISTER`, `SECONDARIES`.

**CommandProcessor** routes commands to the appropriate handler based on the node's role:
- Write commands (`PUT`) go to the storage engine and are queued for replication (on primary).
- Read commands (`GET`) go directly to the storage engine.
- Cluster commands (`REGISTER`, `SECONDARIES`) are handled by MangoTree.

### 4. Storage Engine

The storage engine follows the **BitCask model**: an append-only log on disk with an in-memory index mapping every key to its location on disk.

#### Architecture

```
 Client write
      │
      ▼
┌────────────────────────────────────────────────┐
│         SingleThreadedStorageEngine            │
│                                                │
│  ┌─────────────────────────────────────────┐   │
│  │          Write Queue                    │   │
│  │   ArrayBlockingQueue (1M capacity)      │   │
│  └──────────────┬──────────────────────────┘   │
│                 │                               │
│                 ▼                               │
│  ┌─────────────────────────────────────────┐   │
│  │       Single Write Thread               │   │
│  │  - Polls queue                          │   │
│  │  - Batches up to 1000 writes            │   │
│  │  - Writes batch to disk                 │   │
│  │  - Updates in-memory index              │   │
│  └──────┬──────────────────┬───────────────┘   │
│         │                  │                    │
│         ▼                  ▼                    │
│  ┌─────────────┐  ┌────────────────────┐       │
│  │MemStore     │  │ SerialDiskStore    │       │
│  │(HashMap)    │  │(FileChannel, NIO)  │       │
│  │key→(offset, │  │append-only log     │       │
│  │ file, time) │  │64MB file rotation  │       │
│  └─────────────┘  └────────────────────┘       │
└────────────────────────────────────────────────┘
```

#### Write Path

1. Client sends `PUT key value`.
2. A `WriteRequest` is placed on the `ArrayBlockingQueue`.
3. A `CompletableFuture` is returned to the caller (the future completes when the write is persisted).
4. The single write thread wakes up, drains up to 1000 entries from the queue.
5. The batch is serialized and appended to the current data file via `SerialDiskStore`.
6. For each record written, the `MemStore` is updated with the key's disk offset, filename, and timestamp.
7. All `CompletableFuture`s in the batch are completed.
8. If the current file exceeds 64MB, a new file is created (file rotation).

**Why a single write thread?** Funneling all writes through one thread eliminates lock contention on the data file. Batching amortizes the cost of I/O system calls across many writes, maximizing disk throughput.

#### Read Path

1. Client sends `GET key`.
2. `MemStore` is queried for the key → returns `(offset, filename, timestamp)` or null.
3. If found, `SerialDiskStore` seeks to the offset in the file and reads the `DiskRecord`.
4. The value is extracted and returned to the client.

Reads go directly to the storage engine (no queue) since they are stateless lookups.

#### On-Disk Format (DiskRecord)

Each record on disk is:
```
┌───────────┬────────────┬──────────────┬──────┬───────┬──────────┐
│ timestamp │ key_length │ value_length │ key  │ value │ newline  │
│  8 bytes  │  4 bytes   │   4 bytes    │ var  │  var  │  2 bytes │
└───────────┴────────────┴──────────────┴──────┴───────┴──────────┘
```

Records are never modified or deleted in-place. Deletes write a tombstone (a record with value `__TOMBSTONE__`). Old versions are only removed by compaction (not yet implemented).

#### Crash Recovery

On startup, the storage engine rebuilds the in-memory index by scanning all data files:
1. Files are sorted in reverse chronological order (newest first).
2. Each record is read; if the key hasn't been seen yet, it's added to the MemStore.
3. This ensures the latest version of each key wins.

### 5. Replication

MangoDB uses asynchronous primary-to-secondary replication.

```
Primary                          Secondary
  │                                  │
  │  PUT key value (from client)     │
  │──────────────────────────────►   │
  │  ack to client                   │
  │                                  │
  │  [async] PUT key value           │
  │  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ►   │
  │                                  │
```

**How it works:**
1. When a `PUT` arrives at the primary, it is written to the local storage engine.
2. The write is also placed on the `ReplicationManager`'s queue (`ArrayBlockingQueue`, capacity 1000).
3. A background replication thread polls the queue.
4. For each write, it sends the `PUT` command to all known secondaries via `MangoClient` connections.
5. Secondaries are discovered by querying MangoTree's `SECONDARIES` command.

**Trade-offs:**
- Writes are acknowledged to the client before replication completes (low latency, risk of data loss on primary failure).
- Replication is fire-and-forget (no acknowledgment from secondaries).

### 6. MangoTree (Cluster Coordinator)

MangoTree is a separate process (port 9090) that acts as the cluster's coordination layer.

**Responsibilities:**
- **Node registration** - Nodes send `REGISTER <role> <host> <port>` on startup. MangoTree tracks them in a registry.
- **Request routing** - Clients can connect to MangoTree; it routes write commands to the primary and can distribute reads.
- **Health tracking** - Each registered server has a status: `ALIVE`, `DEAD`, or `UNKNOWN`.

**How registration works:**
1. A MangoServer starts and sends `REGISTER PRIMARY localhost 8082` to MangoTree.
2. MangoTree stores `ServerInfo { role, host, port, status=ALIVE }`.
3. When a secondary starts, it sends `REGISTER SECONDARY localhost 8083`.
4. The primary's `ReplicationManager` queries MangoTree with `SECONDARIES` to discover replicas.

### 7. Client Library

**MangoClient** - A synchronous TCP client for communicating with any MangoDB node.
- `put(key, value)` - Sends `PUT key value\n`, reads response.
- `get(key)` - Sends `GET key\n`, reads response.
- Auto-reconnects if the connection drops.

**MangoTreeClient** - Extends MangoClient with cluster-aware methods.
- `getSecondaries()` - Parses the `SECONDARIES` response into a list of `ServerAddress` objects.

### 8. Configuration

`ConfigManager` loads settings from `config.properties` on the classpath:

| Property | Default | Description |
|---|---|---|
| `port` | 8082 | Server listen port |
| `datapath` | ./data/ | Directory for data files |
| `server.threads` | 500 | Thread pool size |
| `storage.type` | single | Storage engine type |
| `ordered.response` | false | Process commands sequentially |
| `return.key.on.writes` | true | Return key instead of "OK" on PUT |

---

## Data Flow Examples

### Writing a Key

```
Client          MangoServer    CommandProcessor    StorageEngine     ReplicationMgr
  │                 │                │                  │                  │
  │ PUT foo bar     │                │                  │                  │
  │────────────────►│                │                  │                  │
  │                 │ parse+route    │                  │                  │
  │                 │───────────────►│                  │                  │
  │                 │                │  write(foo, bar) │                  │
  │                 │                │─────────────────►│                  │
  │                 │                │                  │ queue write      │
  │                 │                │                  │ batch + flush    │
  │                 │                │  future complete │                  │
  │                 │                │◄─────────────────│                  │
  │                 │                │                  │                  │
  │                 │                │  replicate(PUT)  │                  │
  │                 │                │──────────────────────────────────►  │
  │   "foo"         │                │                  │                  │
  │◄────────────────│                │                  │                  │
```

### Reading a Key

```
Client          MangoServer    CommandProcessor    StorageEngine
  │                 │                │                  │
  │ GET foo         │                │                  │
  │────────────────►│                │                  │
  │                 │ parse+route    │                  │
  │                 │───────────────►│                  │
  │                 │                │  read(foo)       │
  │                 │                │─────────────────►│
  │                 │                │                  │ MemStore lookup
  │                 │                │                  │ → (offset, file)
  │                 │                │                  │ DiskStore read
  │                 │                │  "bar"           │
  │                 │                │◄─────────────────│
  │   "bar"         │                │                  │
  │◄────────────────│                │                  │
```

---

## Project Structure

```
src/
├── main/java/
│   ├── MangoApp.java                    # Entry point, bootstraps server or tree
│   ├── server/
│   │   └── MangoServer.java             # TCP server, connection handling
│   ├── commands/
│   │   ├── Command.java                 # Command model
│   │   ├── CommandParser.java           # Text → Command parsing
│   │   └── CommandProcessor.java        # Command routing and execution
│   ├── storage/
│   │   ├── StorageEngine.java           # Interface
│   │   ├── SingleThreadedStorageEngine  # Primary implementation
│   │   ├── MultiThreadedStorageEngine   # Stub (not yet implemented)
│   │   ├── disk/
│   │   │   ├── DiskStore.java           # Interface
│   │   │   ├── SerialDiskStore.java     # Append-only file I/O with NIO
│   │   │   ├── ConcurrentDiskStore.java # Stub (not yet implemented)
│   │   │   └── DiskRecord.java          # On-disk record format + serialization
│   │   └── mem/
│   │       ├── MemStore.java            # Interface
│   │       └── UnsafeMemStore.java      # HashMap-based in-memory index
│   ├── replication/
│   │   └── ReplicationManager.java      # Async replication to secondaries
│   ├── client/
│   │   ├── MangoClient.java             # TCP client for node communication
│   │   └── MangoTreeClient.java         # Cluster-aware client (extends MangoClient)
│   ├── tree/
│   │   └── MangoTree.java               # Cluster coordinator process
│   ├── config/
│   │   └── ConfigManager.java           # Properties-based configuration
│   ├── cli/
│   │   └── MangoCLI.java                # Command-line interface (picocli)
│   ├── exceptions/
│   │   └── EngineException.java         # Custom exception type
│   └── legacy/                          # Deprecated older storage engine (unused)
├── main/resources/
│   └── config.properties                # Default configuration
└── benchmark/
    ├── writes.go                        # Go write throughput benchmark
    └── reads.go                         # Go read throughput benchmark

integration-tests/
└── test_mangodb.py                      # Python integration tests (pytest)
```

---

## Design Decisions

**Why BitCask?** BitCask's append-only design makes writes a sequential I/O operation, which is the fastest pattern for both HDDs and SSDs. The trade-off is that all keys must fit in memory (the MemStore). This is acceptable for workloads where the key set fits in RAM but values can be large.

**Why a single write thread?** Eliminates all write-path locking. The queue + batch pattern turns many small writes into fewer large sequential I/O operations, maximizing disk throughput.

**Why async replication?** Synchronous replication (waiting for secondaries before acknowledging) adds latency to every write. Async replication prioritizes write speed at the cost of potential data loss if the primary fails before replication completes.

**Why a text protocol?** Simple to debug with telnet/netcat. Easy to implement clients in any language. The trade-off is higher parsing overhead compared to a binary protocol.
