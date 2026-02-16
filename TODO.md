# MangoDB TODO

Roadmap to make MangoDB production-ready, organized by priority.

---

## P0 - Critical: Complete Core Functionality

- [ ] **Implement DELETE operation** - `SingleThreadedStorageEngine.delete()` currently returns null. Needs to write a tombstone record to disk and remove from MemStore.
- [ ] **Implement FLUSH operation** - `SingleThreadedStorageEngine.flush()` currently returns null. Needs to clear MemStore and write flush tombstones.
- [ ] **Implement STATUS command** - `getStatus()` returns empty string. Should report key count, file count, memory usage, uptime, replication lag.
- [ ] **Wire up DELETE/EXISTS/FLUSH/STATUS in CommandProcessor** - These commands are parsed but not routed to the storage engine.
- [ ] **Fix `!=` vs `.equals()` bug in MangoTree** - `primaryServerId != serverId` uses reference equality instead of `.equals()`. Will cause incorrect routing.

## P1 - Stability & Correctness

### Thread Safety
- [ ] **Synchronize `registeredServersInfo` in MangoTree** - HashMap accessed from multiple connection handler threads without synchronization. Use ConcurrentHashMap.
- [ ] **Fix ConcurrentModificationException in ReplicationManager** - Iterating `secondaryMangoClients` while potentially modifying it. Use CopyOnWriteArrayList or synchronize.
- [ ] **Guard MemStore access** - `UnsafeMemStore` is explicitly non-thread-safe but accessed from both the write thread and read threads. Add read/write locking or use ConcurrentHashMap.

### Error Handling
- [ ] **Add consistent exception handling** - Replace generic catch blocks with specific exception types. Never silently swallow exceptions.
- [ ] **Handle write queue overflow** - When the 1M-capacity `ArrayBlockingQueue` is full, writes will block silently. Add backpressure signaling or rejection policy.
- [ ] **Handle replication queue overflow** - The 1000-capacity replication queue can fill up under load with no feedback to the caller.
- [ ] **Return proper error responses** - Standardize error response format (e.g., `ERROR: <message>`) across all commands.

### Resource Management
- [ ] **Close FileChannels on shutdown** - `SerialDiskStore` opens channels but has no cleanup path. Implement `Closeable`.
- [ ] **Close socket connections properly** - Client sockets can leak if the handler thread throws. Use try-with-resources.
- [ ] **Handle thread interruption** - Background threads (write thread, replication thread) don't handle `InterruptedException` properly.
- [ ] **Implement graceful shutdown** - Drain write queue and replication queue before stopping. Ensure all buffered data is flushed to disk.

### Data Integrity
- [ ] **Add checksums to DiskRecord** - CRC32 or similar to detect corruption on reads.
- [ ] **Call `fsync()` after batch writes** - Currently relies on OS page cache. A crash can lose acknowledged writes.
- [ ] **Validate data on read** - Check that key length, value length, and actual data match.

## P2 - Replication & Distributed Systems

### Replication
- [ ] **Add write acknowledgments** - Primary should wait for at least one secondary to confirm before responding to client.
- [ ] **Implement retry with exponential backoff** - Failed replications are currently dropped.
- [ ] **Add replication lag monitoring** - Track how far behind each secondary is.
- [ ] **Ordering guarantees** - Use a monotonic sequence number instead of timestamps (clock skew vulnerability).
- [ ] **Initial sync for new secondaries** - New replicas need to catch up on existing data before accepting live replication.

### Consensus & Failover
- [ ] **Implement leader election** - When primary goes down, a secondary should be promoted. Consider Raft (e.g., Apache Ratis).
- [ ] **Add heartbeat protocol** - MangoTree has a health check executor but it's unused. Implement phi-accrual failure detection or simple heartbeats.
- [ ] **Split-brain prevention** - Add quorum-based decision making to prevent data divergence.
- [ ] **Fencing tokens** - Prevent stale primaries from accepting writes after failover.

### MangoTree Improvements
- [ ] **Make MangoTree address configurable** - Currently hardcoded to `localhost:9090` in `ReplicationManager`.
- [ ] **Persist cluster state** - Server registrations are lost on MangoTree restart.
- [ ] **Support multiple MangoTree instances** - Single coordinator is a single point of failure.

## P3 - Performance

- [ ] **Implement MultiThreadedStorageEngine** - Currently a stub. Should partition keyspace across multiple write threads/file channels.
- [ ] **Add connection pooling** - Both for client connections and inter-node replication connections.
- [ ] **Async client** - Replace blocking `MangoClient` with non-blocking I/O (Netty or Java NIO).
- [ ] **Request pipelining** - Allow clients to send multiple commands without waiting for responses.
- [ ] **Batch operations** - Support MPUT/MGET for multi-key operations in a single round-trip.
- [ ] **Compaction** - Background process to merge data files, remove tombstones, and reclaim disk space.
- [ ] **Memory-mapped I/O** - Consider mmap for reads instead of `FileChannel.read()`.
- [ ] **Binary protocol** - Replace text-based protocol with a binary format (or Protocol Buffers) to reduce parsing overhead and allocations.

## P4 - Observability & Operations

- [ ] **Structured logging** - Replace all `System.out.println` with SLF4J logger calls. Add consistent context (request ID, client address).
- [ ] **Metrics** - Integrate Micrometer or similar. Track: ops/sec, latency histograms, queue depths, replication lag, active connections, file count, memory usage.
- [ ] **Health endpoint** - Expose liveness and readiness probes (useful for container orchestration).
- [ ] **Admin commands** - Add COMPACT, BACKUP, CONFIG, STATS commands for operational tasks.
- [ ] **Distributed tracing** - Add trace/request IDs that propagate through replication.

## P5 - Security

- [ ] **Authentication** - Add client authentication (password, token, or certificate-based).
- [ ] **TLS encryption** - Encrypt all TCP connections (client-server and node-to-node).
- [ ] **Authorization** - Role-based access control (read-only, read-write, admin).
- [ ] **Input validation** - Enforce maximum key/value sizes to prevent memory exhaustion.
- [ ] **Rate limiting** - Protect against abusive clients.

## P6 - Testing

- [ ] **Unit tests** - JUnit tests exist in build config but there appear to be no Java unit tests. Add tests for: CommandParser, DiskRecord serialization, MemStore operations, StorageEngine read/write/delete lifecycle.
- [ ] **Replication tests** - Test replication under failures (secondary down, network partition simulation).
- [ ] **Crash recovery tests** - Kill process mid-write, verify data integrity on restart.
- [ ] **Load/stress tests** - Extend Go benchmarks with concurrent mixed read/write workloads.
- [ ] **Chaos testing** - Random network delays, process kills, disk full scenarios.

## P7 - Code Quality & Cleanup

- [ ] **Remove legacy code** - `legacy/engine/` package is deprecated and unused. Remove it.
- [ ] **Externalize magic numbers** - Move queue capacities (1M, 1000), file rotation size (64MB), batch size (1000), thread pool sizes to config.properties.
- [ ] **Use Optional instead of null** - Storage engine reads should return `Optional<String>` instead of null.
- [ ] **Consistent naming** - Some classes use "Mango" prefix inconsistently.
- [ ] **Reduce ANSI color coupling** - Move color formatting out of CommandProcessor into a presentation layer.
- [ ] **JavaDoc** - Document public APIs, especially StorageEngine interface contract and DiskRecord format.

## P8 - Features

- [ ] **TTL/expiration** - Allow keys to expire after a configurable duration.
- [ ] **Range queries** - Support prefix scans or key range iteration.
- [ ] **Consistent hashing** - Shard data across multiple nodes for horizontal scaling.
- [ ] **Pub/Sub** - Notify clients of key changes.
- [ ] **Backup/Restore** - Snapshot data files and MemStore for point-in-time recovery.
- [ ] **Docker support** - Dockerfile and docker-compose for easy deployment and clustering.
