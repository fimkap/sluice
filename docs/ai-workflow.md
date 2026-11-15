# How this repository was built with an AI agent

sluice was built with [Claude Code](https://claude.com/claude-code), Anthropic's coding agent, working
from a short brief: a Kafka-like broker that compares approaches and measures them, runs in Docker and
deploys to AWS. This page shows the setup that kept the agent on track and what the checks caught. The
setup is in the repository, so anyone can reuse it.

## 1. Context before code

- **The spec came first.** [`docs/design.md`](design.md) was the first commit: record format, protocol,
  durability and replication semantics, the four experiments, and which test pins each rule. Later
  commits implement it step by step, and every one builds and passes the tests on its own.
- **[`AGENTS.md`](../AGENTS.md)** is the standing brief any agent reads first: a map of the packages, the
  commands, the conventions, and seven invariants that must never break (bytes on disk equal bytes on the
  wire, per-connection append order, no reply before durability, a monotonic high watermark, no partition
  with two owners, one holder per share-group lock, recovery that truncates rather than repairs).
- **[`CLAUDE.md`](../CLAUDE.md)** imports `AGENTS.md` and adds what is specific to Claude Code.

## 2. Guardrails

[`.claude/settings.json`](../.claude/settings.json) pre-approves the safe, frequent commands (`go test`,
`go build`, `make`, read-only git) and denies the irreversible or sensitive ones: `git push`,
`terraform apply` and `destroy`, the AWS CLI, and reading `.env` files, keys and Terraform state. Nothing
leaves the machine without a person doing it.

A `PostToolUse` hook ([`.claude/hooks/gofmt.sh`](../.claude/hooks/gofmt.sh)) formats every Go file the
agent edits, so formatting never takes a review round.

## 3. Repeatable workflows

- **Skills** ([`.claude/skills/`](../.claude/skills)): `run-experiments` (how to measure, and the rule
  that README numbers are copied verbatim from result files in `docs/results/`) and `add-protocol-api` (the
  seven-step checklist from the spec to fuzz corpus to client).
- **A reviewer subagent** ([`.claude/agents/durability-reviewer.md`](../.claude/agents/durability-reviewer.md))
  reviews diffs to the storage and replication code against a checklist of crash-consistency and
  concurrency failures: acknowledging before fsync, lost wake-ups, file lifetimes, high-watermark
  monotonicity.

## 4. Verification instead of trust

Each step had to pass the following before it was committed:

- `go test -race ./...`, with in-process clusters on loopback TCP and temp directories;
- crash tests that tear and corrupt log tails and reopen;
- fuzzing of the record decoder and every protocol message (millions of inputs per run);
- invariant tests that assert behaviour rather than return codes, such as "no partition owned by two
  consumers at any poll" and "every share-group record delivered exactly once while locks hold";
- experiments run from a single command, with results saved in `docs/results/`.

## 5. What the checks caught

These are real issues from this repository's history:

| Caught by | Issue | Fix |
|-----------|-------|-----|
| `TestGroupRebalanceNeverDoubleAssigns` | The group coordinator counted only partitions a member had *reported* owning. Right after a member's first poll, before its next heartbeat, a joining member was handed the same partition: two owners at once. | The coordinator also counts partitions it assigned that the member hasn't reported yet, as KIP-848 does. |
| `go vet` | `Registry.WriteTo` clashed with the `io.WriterTo` signature. | Renamed to `Write`. |
| Review of the producer | `Close` could close a channel while `Produce` was sending on it, and a `defer` inside the send loop leaked timers. | A close lock guards the channel; the timer is stopped explicitly. |
| Running the cluster in compose | Followers exported an ISR size of 0 for partitions they don't lead, which read like an outage. | Only the leader exports the gauge. |
| A storage test | The test expected at least 10 segments; the data only fills 9 of that size. | The test was wrong, and the commit says why. |

## 6. What stays with a person

The agent wrote and tested the code. A person still owns the decisions the tests can't make:

- which semantics to choose (the ISR rule's `MinInsync` check after the append, at-least-once share groups);
- whether a measurement is meaningful (loopback understates the value of zero-copy; Docker Desktop's
  virtual disk makes fsync cheaper than real hardware);
- what to leave out (failover, TLS, compression), stated plainly under "Limits" in the README.
