# sluice — design spec

This is the spec the implementation follows. It was written before the code, and each section names the
tests that hold the implementation to it. When the code and this document disagree, one of them is a bug.

## Goal

A small Kafka-style message broker that is real enough to measure. Four questions drive it, each answered by
an experiment that anyone can rerun with one command:

| # | Question | Approaches compared |
|---|----------|---------------------|
| E1 | What does durability cost? | no fsync, fsync on an interval, fsync per write, group commit |
| E2 | Log or queue? | consumer groups (ordered, partition-bound) vs share groups (KIP-932 queue semantics) under slow messages |
| E3 | Which commit rule? | Kafka's ISR rule vs a majority quorum (Raft's rule) with one slow follower |
| E4 | Does zero-copy matter? | `sendfile(2)` from the segment file vs read-into-buffer-and-write |

## Non-goals

- Kafka wire compatibility. The protocol is Kafka-shaped but simpler (one partition per produce or fetch request).
- Leader failover and a metadata quorum. Leaders are fixed by the assignment; Section "Replication" says what
  failover would need.
- TLS, authentication, quotas, compression, transactions, compaction.

## Architecture

```
            clients (producer / consumer / share consumer / admin)
                         |  binary protocol over TCP (pipelined, correlation ids)
   +---------------------v----------------------+
   | broker: conn reader -> handlers -> conn writer (single writer goroutine per conn)
   |   topics -> partitions -> logstore.Log (segments + sparse index)
   |   group coordinator (consumer groups, offsets)
   |   share partitions (per group x partition, on the partition leader)
   |   replica fetchers (follower side) / replica state + HW (leader side)
   |   HTTP: /metrics (Prometheus text), /healthz, /debug/pprof
   +--------------------------------------------+
```

Packages:

- `internal/record` — record batch format: build, validate, iterate. Shared by broker and client.
- `internal/logstore` — the partition log: segments, index, recovery, retention, fsync policies.
- `internal/protocol` — framing and request/response encoding.
- `internal/broker` — server, topics, produce/fetch, replication, groups, share groups.
- `internal/metrics` — minimal Prometheus text exposition (no dependency).
- `client` — Go client: Producer, Consumer (group), ShareConsumer, Admin.
- `cmd/sluice` (broker), `cmd/sluice-bench` (experiments), `cmd/sluicectl` (CLI).

## Record batch format

On disk and on the wire the batch is byte-identical, so a fetch can be served straight from the file.
Big-endian, header 57 bytes:

```
off size field
  0   8  baseOffset       assigned by the leader; NOT covered by the CRC
  8   4  batchLength      bytes after this field
 12   4  crc              CRC-32C (Castagnoli) of bytes [16, end)
 16   1  magic = 1
 17   2  attributes       reserved (compression bits), 0
 19   4  lastOffsetDelta  recordCount - 1
 23   8  firstTimestamp   ms since epoch
 31   8  maxTimestamp
 39   8  producerId       -1 if not idempotent
 47   2  producerEpoch
 49   4  baseSequence     -1 if not idempotent
 53   4  recordCount
 57   …  records
```

Record (varints are zigzag, like Kafka): `length, attributes(int8), timestampDelta, offsetDelta, keyLen(-1=null),
key, valueLen(-1=null), value, headerCount, {keyLen, key, valueLen, value}…`.

Because `baseOffset` sits outside the CRC, the leader assigns offsets by writing 8 bytes in place, without
recomputing the checksum. Consumers verify the CRC end to end.

Tests: `internal/record` round trips, corruption detection, fuzzing of the decoder.

## Partition log (`internal/logstore`)

- Directory per partition: `<topic>-<partition>/`, segment files `<20-digit base offset>.log` and `.index`.
- The index is sparse: one 8-byte entry `(relativeOffset u32, position u32)` per `IndexIntervalBytes` (4 KiB)
  of log. A lookup binary-searches the index, then scans batch headers.
- The active segment rolls at `SegmentBytes`. A rolled segment is fsynced (log and index) before the roll returns.
- **Recovery.** Closed segments are trusted (they were fsynced at roll). The active segment is scanned from the
  start; every batch must have a sane length, a valid CRC and the expected base offset. The file is truncated
  at the first batch that fails, and its index is rebuilt. Tests write torn and corrupted tails and reopen.
- **Retention** by total bytes and by age; the active segment is never deleted. Segments are reference-counted
  so a fetch that is still sending from a file is not broken by retention deleting it.
- **Positions.** The log end offset (LEO) and the high watermark (HW) carry `(offset, segment, position)`,
  so a fetch bounded by the HW needs no extra index lookups.
- `TruncateTo(offset)` is used by a follower whose log is ahead of its leader's (possible only if the leader
  lost an unsynced tail).

### Durability policies (E1)

| Policy | When the producer's `acks=1` returns | What a crash can lose |
|--------|--------------------------------------|-----------------------|
| `none` | after `write(2)` | everything since the last roll |
| `interval` | after `write(2)`; a ticker fsyncs every `FsyncInterval` | up to one interval |
| `always` | after `write(2)` + `fdatasync(2)`, under the log lock | nothing acknowledged |
| `group` | after a shared `fdatasync(2)` that covers the write | nothing acknowledged |

Group commit: appends write to the page cache and release the lock; a flusher goroutine runs one fdatasync for
everything written so far and releases all waiters it covers. Concurrent producers share one fsync.
Linux uses `fdatasync`; macOS uses `F_FULLFSYNC` (Go's `File.Sync`).

A replica's **acknowledged LEO** is its durable LEO under `always`/`group` and its written LEO otherwise.
The high watermark is computed from acknowledged LEOs, so under `always`/`group` a consumer never reads a
record that a crash could take back.

## Protocol (`internal/protocol`)

Frames: `size u32 | body`. Request body: `apiKey i16 | apiVersion i16 | correlationId i32 | clientId string16 | payload`.
Response body: `correlationId i32 | payload`. Strings are `i16` length-prefixed, byte arrays `i32`, arrays `i32` count.

Requests on a connection are **applied in order** (appends from one connection keep their order) but
**complete in any order**: a produce waiting for replication or a fetch long-polling does not block the
requests behind it. Clients match responses by correlation id. A produce with `acks=0` gets no response.

| Key | API | Notes |
|-----|-----|-------|
| 0 | Produce | one partition; `acks` 0, 1, -1 (all) |
| 1 | Fetch | `replicaId` -1 for consumers (read up to HW), ≥0 for followers (read up to LEO, report position) |
| 2 | ListOffsets | -2 earliest, -1 latest (HW) |
| 3 | Metadata | brokers, partitions, leaders, replicas, ISR (as seen by the leader) |
| 4 | CreateTopic | forwarded to every peer |
| 5 | InitProducerId | idempotent producer id |
| 6 | FindCoordinator | group coordinator = brokers sorted by id, `fnv32(group) % n` |
| 7 | GroupHeartbeat | KIP-848 style membership and server-side assignment |
| 8 | OffsetCommit | |
| 9 | OffsetFetch | |
| 10 | ShareFetch | acquire records; acknowledgements can ride along |
| 11 | ShareAcknowledge | ACCEPT, RELEASE, REJECT, RENEW |

Tests: encode/decode round trips; `FuzzDecode` must never panic on arbitrary input.

## Produce path and idempotence

1. Validate the batch (bounds, magic, CRC, record count).
2. Idempotent producers (`producerId ≥ 0`): the partition keeps the last 5 batches per producer. A batch
   whose sequence matches one of them is a retry: the broker returns the original offset and appends nothing.
   A gap in sequences is `OUT_OF_ORDER_SEQUENCE`. State is rebuilt from the active segment on restart.
3. Append (offsets assigned), then wait: `acks=1` for the leader's acknowledged LEO, `acks=all` for the HW.

Tests: a retried batch gets the same offset and is stored once; a gap is rejected.

## Replication (E3)

- Static membership (`--peers`). Partition `p` of topic `t`: replicas are `RF` consecutive brokers starting at
  `(fnv32(t) + p) % n`; the first one leads.
- Followers pull: one fetcher per follower partition sends `Fetch(replicaId=self, offset=LEO)`, appends
  batches with the leader's offsets, waits for its own durability, and fetches again. The fetch offset is the
  follower's report of its acknowledged LEO. Fetches long-poll, so new data reaches followers at once.
- **ISR rule (Kafka):** a follower is in sync if it caught up to the leader's LEO within `ReplicaLagMax`.
  HW = min acknowledged LEO over the ISR. `acks=all` fails with `NOT_ENOUGH_REPLICAS` if
  `|ISR| < MinInsync`.
- **Quorum rule (Raft):** HW = the highest offset acknowledged by a majority of all replicas. No ISR.
- A slow follower that keeps up within `ReplicaLagMax` stays in the ISR and sets the pace for `acks=all`;
  under the quorum rule the fastest majority does. E3 measures exactly this.
- A follower that finds itself ahead of the leader truncates to the leader's LEO.
- **Out of scope: failover.** It would need leader epochs in the batches, a controller (or Raft) to pick a new
  leader from the ISR, and followers truncating to the epoch boundary instead of the leader's LEO.

Tests: in-process 3-broker clusters: acks=all waits for followers; a stopped follower leaves the ISR and
`acks=all` with `MinInsync=3` fails; the quorum rule commits with one follower down.

## Consumer groups (KIP-848 style)

- The coordinator owns membership and computes assignments; there is no client-side JoinGroup/SyncGroup.
- Members heartbeat with their subscription and the partitions they still own. The coordinator computes a
  round-robin target assignment over sorted members and partitions.
- **No double ownership:** a member is given a partition only after its previous owner has reported it
  released. Handover takes about two heartbeat intervals.
- Missed heartbeats for `SessionTimeout` remove the member. Offsets persist per group under `meta/groups/`.

Tests: two members split partitions; a third joins and partitions move without overlap; a member that stops
heartbeating is removed and its partitions reassigned.

## Share groups (KIP-932 style, E2)

Queue semantics on top of the log: any member can consume any record, records are acknowledged one by one,
and ordering is given up for parallelism. State lives on the partition leader, per (group, partition):

- `SPSO` (start offset): every record below it is finished. `SPEO` (end offset): next record to consider.
- In-flight records between them: `Available → Acquired (lock, deliveryCount++) → Acknowledged | Archived`.
- `ACCEPT` → Acknowledged. `RELEASE` → Available. `REJECT` → Archived. `RENEW` extends the lock (Kafka 4.2).
- An expired lock makes the record Available again; once `deliveryCount` reaches `MaxDeliveries` it is Archived.
- `SPSO` advances over the finished prefix and is checkpointed, so a broker restart redelivers at most the
  in-flight window (at-least-once).
- At most `MaxInFlight` records per share partition are in flight.

Tests: ack/release/reject; lock expiry redelivers; delivery limit archives; parallel members never get the same
record while its lock is held.

## Observability

`/metrics`: produce and fetch counts and bytes, fsync count and latency, records per fsync (group commit
efficiency), HW lag and ISR size per partition, share in-flight records, process CPU. `/debug/pprof` for profiles.

## Experiments

`sluice-bench experiment <e1|e2|e3|e4|all>` starts brokers in process (real TCP on loopback, real files) and
prints a Markdown table plus JSON. Each experiment fixes its workload in code so a rerun measures the same
thing. The README reports the numbers with the machine they came from.

## Deployment

- Docker: multi-stage build, static binary on distroless, non-root. `docker compose up` starts 3 brokers.
- AWS: Terraform for 3 Graviton EC2 brokers with gp3 data volumes, one bench instance, ECR, CloudWatch logs,
  SSM Session Manager (no SSH). Brokers are stateful, need stable addresses and a disk: EC2, not Fargate.
