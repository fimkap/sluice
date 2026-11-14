# Agent instructions for sluice

Read this before changing anything. It is the shared context for any coding agent (Claude Code reads it
through `CLAUDE.md`; other tools read `AGENTS.md` directly).

## What this is

A Kafka-style broker in Go, built to measure four trade-offs (durability, log vs queue, ISR vs quorum,
zero-copy). The spec is [`docs/design.md`](docs/design.md). **Change the spec first, then the code.**

## Map

| Path | What lives there |
|------|------------------|
| `internal/record` | batch format: build, validate, iterate |
| `internal/logstore` | segments, sparse index, recovery, retention, fsync policies |
| `internal/protocol` | framing, request/response types |
| `internal/broker` | server, produce/fetch, replication, consumer groups, share groups |
| `internal/metrics` | Prometheus text exposition |
| `client` | Go client used by tests, the CLI and the benchmarks |
| `cmd/sluice`, `cmd/sluice-bench`, `cmd/sluicectl` | binaries |
| `deploy/terraform` | AWS (never apply from an agent session) |

## Commands

```
make test        # go test ./...           (fast, no Docker)
make race        # go test -race ./...      (run before every commit that touches concurrency)
make fuzz        # 30 s of protocol and record fuzzing
make lint        # go vet + gofmt check
make bench       # all experiments, prints Markdown tables
make docker      # build the image; `docker compose up` for a 3-broker cluster
```

## Invariants — never break these

1. **Bytes on disk == bytes on the wire.** Never re-encode a batch in the produce or fetch path. The leader
   only writes `baseOffset` (outside the CRC).
2. **Appends from one connection keep their order.** Validation and append happen on the connection's read
   loop; only waiting (durability, replication, long-poll) moves to a goroutine.
3. **An acknowledged write survives a crash** under `always` and `group`. Don't reply before the durable
   offset covers the batch.
4. **The HW never moves backwards** and never passes a replica's acknowledged LEO in the commit rule.
5. **No partition is owned by two members of a consumer group at once.**
6. **A share-group record is held by at most one member while its lock is valid.**
7. Recovery truncates; it never "repairs" or skips over a bad batch in the active segment.

## Conventions

- Go 1.27, standard library first. The only dependency is `golang.org/x/sys`. Ask before adding another.
- Errors: wrap with `%w`; protocol errors are `protocol.ErrorCode` values, never strings on the wire.
- Logging: `log/slog`, structured, no `fmt.Println` in library code.
- Tests: table-driven where it helps; in-process brokers on `127.0.0.1:0` and `t.TempDir()`. No sleeps to
  wait for state: poll with a deadline (`waitFor` helpers).
- Every exported identifier in `client` has a doc comment.

## Workflow

1. Small steps. Each commit builds, passes `make race`, and does one thing. Imperative commit subjects.
2. A new behaviour comes with the test that pins it, ideally written first.
3. Benchmarks: numbers in the README come only from `make bench` output, pasted with the machine they ran
   on. Never edit a number by hand, never round in our favour.
4. When unsure about semantics, check `docs/design.md`; if it's silent, ask instead of guessing.

## Guardrails

- Never `git push`, never `terraform apply`/`destroy`, never touch AWS credentials.
- Don't delete data directories outside `t.TempDir()`.
- Don't weaken a test to make it pass. If a test is wrong, say why in the commit message.
