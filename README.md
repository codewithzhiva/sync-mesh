# SyncMesh

An offline-first replication engine in Go with resumable cursors, idempotent mutations, deterministic conflict resolution, an append-only WAL, and an HTTP/WebSocket sync surface.

## Quick start

```bash
docker compose up --build
```

- Sync API: <http://localhost:8087>
- Two-device simulator: <http://localhost:8088>

Clients upload mutations to `POST /v1/mutations`, resume with `GET /v1/sync?cursor=N`, inspect compact state at `GET /v1/snapshot`, or request the same change page through `/v1/sync/ws?cursor=N`.

## Conflict semantics

- `lww_set` and `delete` use timestamp, then client ID as a deterministic tie-breaker.
- `gset_add` is monotonic and converges by union.
- Mutation IDs are idempotent.
- A response cursor acknowledges every mutation through that sequence.
- The local WAL is fsync'd before an accepted mutation is returned.

The WebSocket endpoint provides server-to-client change frames; mutation upload remains HTTP in v0.1. Property tests exercise order-independent convergence, resumability, deduplication, and restart recovery.

Apache-2.0 licensed.
