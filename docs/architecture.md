# Architecture and failure semantics

Every accepted mutation receives a monotonic server sequence and is fsync'd to an append-only WAL. Materialized LWW registers and grow-only sets can always be rebuilt by replay. Clients persist the returned cursor and request changes after it; a cursor beyond the retained log triggers a full replay in v0.1.

Arrival order does not affect materialized state because LWW conflicts use timestamp/client ID and grow-only sets use union. Wall clocks are accepted as input in v0.1; production clients should use hybrid logical clocks to reduce clock-skew anomalies.
