# Event Sourcing Demo — Go

A minimal but complete Event Sourcing example using:

- **PostgreSQL** — append-only event store (the single source of truth)
- **NATS** — event bus between write and read sides
- **Redis** — read store (denormalised projections)
- **Go** — single binary running the command handler, query handler, and projection worker

This demo extends the patterns from [cqrs-demo](https://github.com/paulja/cqrs-demo).
The infrastructure is identical; the key addition is the **aggregate** that
rebuilds its state by replaying events, plus an **event store** with optimistic
concurrency.

---

## How Event Sourcing differs from CQRS

In the CQRS demo, events are a side-effect of commands — the implicit source of
truth is a mutable state row. In Event Sourcing, **the event log is the only
source of truth**. There is no current-state row. When a command arrives, the
aggregate is rebuilt by replaying all its past events; only then is the new
event appended.

| Concern | CQRS demo | This demo |
|---|---|---|
| Write store | `events` table (already append-only) | Same table, **no** separate state row |
| Load aggregate | Query a state row | **Replay events** for that aggregate ID |
| Command handler | Validate → insert event | **Load by replay** → validate → append |
| Optimistic concurrency | Not demonstrated | **expectedVersion** on every append |
| Snapshot | n/a | Optional — persisted to `snapshots` table |
| Projection | NATS → Redis | Unchanged — NATS → Redis |
| Query | Redis read | Unchanged — Redis read |

---

## Project layout

```
.
├── cmd/
│   └── server/
│       └── main.go         # entry point — wires all packages together, runs HTTP server
├── aggregate/
│   └── order.go            # Order aggregate: Apply(event) + Create/Cancel command methods
├── eventstore/
│   └── postgres.go         # append-only event store: Load, Append (optimistic concurrency), snapshots
├── command/
│   └── handler.go          # write side: load aggregate by replay → validate → append → publish
├── query/
│   └── handler.go          # read side: GetOrder and ListOrders served from Redis
├── projection/
│   └── worker.go           # NATS subscriber: projects events into Redis; catch-up replay on start
├── domain/
│   └── events.go           # shared types: Event envelope, Metadata, payload structs, commands, OrderView
├── go.mod
├── infra.yaml              # Kubernetes manifests: PostgreSQL, NATS, Redis (for kind)
├── setup.sh                # create kind cluster, apply infra.yaml, start port-forwards
└── cleanup.sh              # stop port-forwards, delete kind cluster
```

Note: unlike the CQRS demo where you could query Postgres directly for current
state in a pinch, here Postgres only holds raw events — there is no state row to
fall back on. The query handler and Redis projections are the **only** way to
read current state. This is Event Sourcing's stricter contract.

---

## Architecture

Three distinct paths through the system — write, async projection, and read —
each shown left to right.

**Write path** — a command causes the aggregate to be rebuilt from the event log,
validated, and a new event appended. There is no state row; Postgres holds only
immutable events.

```mermaid
flowchart LR
    A([curl]) -->|POST /orders\nDELETE /orders/id| B[Command handler]
    B -->|Load id| C[Aggregate]
    C -->|SELECT events| D[(PostgreSQL\nevent log)]
    D -->|replay| C
    C -->|current state| B
    B -->|INSERT event\nexpectedVersion| D
    B -->|publish| E([NATS])
```

**Async projection path** — the projection worker subscribes to NATS and builds
Redis read models in the background. On restart it catches up from Postgres
using the `global_seq` pointer stored in Redis.

```mermaid
flowchart LR
    A([NATS]) -->|deliver event| B[Projection worker]
    C[(PostgreSQL\nevent log)] -->|catch-up replay\non restart| B
    B -->|SET order:\nSADD user_orders:| D[(Redis\nprojections)]
    B -->|SET projection:last_seq| D
```

**Read path** — queries are served entirely from Redis. Postgres is never touched
on the read side.

```mermaid
flowchart LR
    A([curl]) -->|GET /orders/id\nGET /orders?user_id=| B[Query handler]
    B -->|GET order:| C[(Redis\nprojections)]
    C -->|OrderView JSON| B
    B -->|200 OK| A
```

**Debug path** — the `/events` endpoint exposes the raw event stream for any
aggregate, useful for inspecting history and verifying replay.

```mermaid
flowchart LR
    A([curl]) -->|GET /events?aggregate_id=| B[Debug endpoint]
    B -->|SELECT events| C[(PostgreSQL\nevent log)]
    C -->|versioned events| B
    B -->|200 OK + event log| A
```

### Sequence: POST /orders (write side)

```mermaid
sequenceDiagram
    participant C as curl
    participant CMD as command handler
    participant AGG as aggregate
    participant PG as postgres
    participant N as NATS

    rect rgb(238, 237, 254)
        note over C,N: POST /orders — create order
        C->>CMD: POST /orders {user_id, item, total}
        CMD->>AGG: Load(id)
        AGG->>PG: SELECT events WHERE aggregate_id = id
        PG-->>AGG: [] (empty — new aggregate)
        AGG-->>CMD: Order{version=0}
        CMD->>AGG: Create(cmd)
        AGG-->>CMD: Order{version=1, changes=[order.created]}
        CMD->>PG: INSERT event (version=1, expectedVersion=0)
        PG-->>CMD: ok
        CMD->>N: publish order.created
        CMD-->>C: 202 Accepted + {id}
    end
```

### Sequence: Async projection (background)

```mermaid
sequenceDiagram
    participant N as NATS
    participant P as projection worker
    participant R as redis

    rect rgb(225, 245, 238)
        note over N,R: async projection (background)
        N->>P: deliver order.created
        P->>R: SET order:<id> OrderView
        P->>R: SADD user_orders:<uid> <id>
        P->>R: SET projection:last_seq <global_seq>
    end
```

### Sequence: GET /orders/{id} (read side)

```mermaid
sequenceDiagram
    participant C as curl
    participant QRY as query handler
    participant R as redis

    rect rgb(238, 237, 254)
        note over C,R: GET /orders/{id} — read order
        C->>QRY: GET /orders/{id}
        QRY->>R: GET order:<id>
        R-->>QRY: OrderView JSON
        QRY-->>C: 200 OK + order JSON
    end
```

---

## Demo vs AWS-native equivalent

| This demo | AWS equivalent | Notes |
|---|---|---|
| **PostgreSQL** (event store) | **DynamoDB** | Partition key = aggregate_id, sort key = version. Conditional write for optimistic concurrency. |
| **NATS** (event bus) | **EventBridge** or **Kinesis** | EventBridge for fan-out routing; Kinesis for ordered high-throughput streams. |
| **Redis** (projections) | **ElastiCache (Redis)** or **DynamoDB** | ElastiCache is a drop-in swap. DynamoDB if you want projections to survive cache eviction. |
| **Snapshots table** (Postgres) | **DynamoDB** (separate table) | Same pattern — partition key = aggregate_id. |
| **kind / kubectl** (local infra) | **ECS** or **EKS** | The Go binary runs as a single container task. |
| **Hand-rolled event store** | **KurrentDB on ECS** or **Kurrent Cloud** | Purpose-built alternative — handles streams, concurrency, and subscriptions natively. Available on AWS Marketplace. |

The AWS assembly (DynamoDB + EventBridge + ElastiCache) gives you managed
infrastructure but you still write the event sourcing semantics yourself.
KurrentDB/Kurrent Cloud is the purpose-built path if you want those semantics
out of the box.

---

## Prerequisites

- Go 1.22+
- `kind` + `kubectl`
- Podman or Docker (for kind)

---

## Running locally

### 1. Start the infrastructure

```bash
./setup.sh
```

Creates the kind cluster (or wipes and redeploys it), applies `infra.yaml`,
waits for pods to be ready, and starts port-forwards on:

| Service    | Address        |
|------------|----------------|
| PostgreSQL | localhost:5432  |
| NATS       | localhost:4222  |
| Redis      | localhost:6379  |

### 2. Start the Go service

```bash
go mod tidy
go run ./cmd/server
```

The server listens on `:8080` by default. Override with `HTTP_ADDR=:9090`.

---

## Try it out

### Create an order (command — write side)

```bash
curl -s -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"alice","item":"keyboard","total":129.99}' | jq
```

Response:
```json
{ "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" }
```

### Read the order (query — read side, served from Redis)

```bash
curl -s http://localhost:8080/orders/<id> | jq
```

### List all orders for a user

```bash
curl -s "http://localhost:8080/orders?user_id=alice" | jq
```

### Cancel an order (with optional reason)

```bash
curl -s -X DELETE http://localhost:8080/orders/<id> \
  -H 'Content-Type: application/json' \
  -d '{"reason":"changed my mind"}'
```

### Inspect the raw event stream

This endpoint is unique to the ES demo — it exposes the underlying event log
for a given aggregate. This is what makes point-in-time queries possible.

```bash
curl -s "http://localhost:8080/events?aggregate_id=<id>" | jq
```

You will see each event as a versioned record with its payload, timestamp, and
global sequence number.

---

## Observing Event Sourcing in action

### The event log is the source of truth

```bash
psql "postgres://postgres:secret@localhost:5432/events?sslmode=disable" \
  -c "SELECT global_seq, aggregate_id, type, version, occurred_at FROM events ORDER BY global_seq;"
```

Notice that there is **no current-state row** — only events. The aggregate's
state exists only in memory, derived by replaying this log.

### Optimistic concurrency in action

Try cancelling the same order twice concurrently:

```bash
ID=<your-order-id>
curl -s -X DELETE http://localhost:8080/orders/$ID &
curl -s -X DELETE http://localhost:8080/orders/$ID &
wait
```

One will succeed with `202 Accepted`; the other will return `409 Conflict`
because the version check failed. No locks required.

### Rebuild a projection from scratch

Stop the server, flush Redis, restart:

```bash
redis-cli FLUSHALL
go run ./cmd/server
```

On startup the projection worker reads `projection:last_seq` from Redis
(now 0), fetches all events from Postgres via `LoadFrom`, and replays them —
rebuilding every `OrderView` from the raw event log without touching the
command side.

### Add a new projection without touching the write side

The event log is permanent. If you wanted a new read model — say,
`cancelled_orders:<date>` — you would:

1. Add a handler for `order.cancelled` in `projection/worker.go`
2. Run a one-off replay from `global_seq = 0`

The write side (Postgres schema, command handler, aggregate) is completely
unchanged.

### Point-in-time state

To see what an order looked like after only its first event:

```bash
psql "postgres://postgres:secret@localhost:5432/events?sslmode=disable" \
  -c "SELECT payload FROM events WHERE aggregate_id = '<id>' AND version = 1;"
```

---

## Inspect Redis projections directly

```bash
redis-cli GET order:<id>
redis-cli SMEMBERS user_orders:alice
redis-cli GET projection:last_seq
redis-cli KEYS "*"
```

---

## Environment variables

| Variable       | Default                                                            |
|----------------|--------------------------------------------------------------------|
| `POSTGRES_DSN` | `postgres://postgres:secret@localhost:5432/events?sslmode=disable` |
| `NATS_URL`     | `nats://localhost:4222`                                            |
| `REDIS_ADDR`   | `localhost:6379`                                                   |
| `HTTP_ADDR`    | `:8080`                                                            |

---

## Key concepts illustrated

**Aggregate rebuilt by replay** — `aggregate/order.go` has a single `Apply`
method called both during history replay and when raising new events. State
transitions live in one place.

**Optimistic concurrency** — `eventstore/postgres.go` checks
`MAX(version) == expectedVersion` inside a transaction before inserting. If
two commands race on the same aggregate, one gets `409 Conflict` with no
deadlock or table lock.

**Global sequence** — the `global_seq BIGSERIAL` column in Postgres gives a
total ordering across all aggregates. The projection worker stores
`projection:last_seq` in Redis so it can resume exactly where it left off
after a restart.

**Projection catch-up** — `projection/worker.go` calls `LoadFrom(lastSeq)`
on startup. This means you can restart the server with a flushed Redis and
all projections rebuild automatically — no separate migration tool needed.

**Separation of concerns** — the query handler and projection worker are
completely unaware of aggregates. The command handler is completely unaware of
Redis. The event envelope (`domain.Event`) is the only shared contract.

## What has this approach got over CQRS?

There are three things that CQRS alone doesn't give us:

1. The event log is the only truth. There's no state row anywhere. Every aggregate's current state is derived by replaying its event history. This means Postgres is purely append-only — nothing is ever updated or deleted.
2. Retroactive projections. Because the full history is always there, you can add a brand new read model today and backfill it from the beginning of time by replaying from global_seq = 0. In the CQRS demo, if you added a new projection, you could only populate it from events that arrive from that point forward.
3. Optimistic concurrency without locks. The expectedVersion check means two concurrent commands on the same aggregate can't silently overwrite each other. One succeeds, one gets a 409 and retries. No database locks, no transactions spanning the whole request.

Everything else — the NATS bus, Redis projections, write/read separation — we already had with CQRS.

The real win is when you realise the only thing in Postgres you add is **immutable** facts about what happened, you get:

- A complete audit trail for free — you never accidentally overwrite the history of an order
- The ability to answer "what did this aggregate look like at version 3?" without any extra instrumentation
- Confidence that your read models are derived and therefore always reconstructable — if Redis gets corrupted or a projection had a bug, nothing is lost

A traditional database is a current state machine — it tells you where things are now. An event log is a record of what happened — it tells you the complete story. You can always derive the former from the latter, but you can never go the other way.

Immutability is what makes the audit trail, retroactive projections, and point-in-time queries possible — they're just consequences of the same fundamental property. Nobody can go into the database and "fix" a record. The only thing you can ever do is append what happened next.

In regulated industries, that's not just useful, it's often a legal requirement. But even outside of that, there's a class of production bug that simply cannot exist — the one where someone asks "how did we end up in this state?" and the answer is "we don't know, the data was overwritten."