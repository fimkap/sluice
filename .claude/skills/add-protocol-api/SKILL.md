---
name: add-protocol-api
description: Checklist for adding a request/response pair to sluice's protocol, end to end (codec, broker handler, client, tests, fuzz corpus). Use when a feature needs a new API.
---

# Adding a protocol API

Work through these in order; each step should compile before the next.

1. **Spec first.** Add the API to the table in `docs/design.md` with its semantics and error codes.
2. **Key and types.** `internal/protocol/types.go`: new `APIKey` constant and name. Reuse existing
   `ErrorCode`s; add one only if no existing code describes the failure.
3. **Messages.** `internal/protocol/messages.go`: request and response structs with `Encode`/`Decode`.
   Arrays go through `ArrayLen(minElementSize)` so hostile counts are rejected. Register both in
   `NewRequest`/`NewResponse`.
4. **Round trip and fuzz.** Add a populated pair to `samples()` in `protocol_test.go`; the round-trip test
   and `FuzzDecode` pick it up automatically. Run `make fuzz`.
5. **Broker.** Dispatch in `internal/broker/conn.go`. Anything that can wait (replication, long-polls,
   peers) must go through `c.async`, never block the read loop. Count it in `sluice_requests_total`
   (automatic if the key is in range).
6. **Client.** A method in `client/` with a doc comment; map `ErrorCode`s to errors, retry only
   `Retriable()` ones.
7. **Tests.** An in-process broker test in `internal/broker/*_test.go` that pins the semantics, including
   one error path. Then `make race`.
