# GridBank

GridBank is a production-style Go backend for operating a multi-tenant compute-resource exchange. It coordinates provider pools, capacity offers, workload leases, usage metering, credit reservations, immutable settlement records, scheduler jobs, durable outbox delivery, audit trails, and restart recovery.

The product theme is inspired by China News Service reporting on “算力超市” and “算力银行”. GridBank is an independent fictional system and does not claim affiliation with organizations named in that report.

## Requirements

- Go 1.26 with `GOTOOLCHAIN=local`
- Docker for the container workflow
- A writable path for the SQLite database

## Run locally

```bash
GRIDBANK_DATABASE_PATH=./gridbank.db GOTOOLCHAIN=local go run ./cmd/server
```

The server listens on `:8080` by default. Configuration uses environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `GRIDBANK_ADDRESS` | `:8080` | HTTP listen address |
| `GRIDBANK_DATABASE_PATH` | `gridbank.db` | SQLite database path |
| `GRIDBANK_SHUTDOWN_TIMEOUT` | `10s` | graceful shutdown deadline |
| `GRIDBANK_SESSION_TTL` | `12h` | login session lifetime |
| `GRIDBANK_LEASE_TTL` | `90s` | workload, scheduler, and outbox lease lifetime |
| `GRIDBANK_WORKER_RETRY_BASE` | `250ms` | exponential retry base |
| `GRIDBANK_WORKER_MAX_ATTEMPTS` | `5` | terminal attempt limit |
| `GRIDBANK_DATABASE_MAX_OPEN_CONNS` | `8` | SQLite connection pool bound |

`GET /healthz` reports process liveness. `GET /readyz` verifies the database dependency before reporting ready.

## Docker

```bash
docker build -t gridbank:local .
docker run --rm -p 8080:8080 -v gridbank-data:/data gridbank:local
```

The multi-stage image builds the real `./cmd/server` entry with Go 1.26 and runs it as a non-root user. It does not pin a CPU architecture.

## Core workflows

1. Bootstrap a tenant administrator, log in, create operator/reviewer/steward/worker users, then register a provider, compute pool, and capacity offer.
2. Open a credit account, deposit compute credits, submit an idempotent workload, match it to a compatible pool, and reserve both capacity and credits atomically.
3. Record usage segments, submit metering, and settle the workload while preserving an auditable credit ledger.
4. Freeze an immutable settlement release, enqueue a scheduler job, and execute it through leased attempts, checkpoints, retry, and outbox delivery.

Every mutation propagates the HTTP request context and request ID into services and SQL, and couples its business state with durable audit/outbox effects in one transaction. Conditional updates bind claims to owner, token, version, and expiry to prevent stale workers from changing current ownership.

## Verification

```bash
GOTOOLCHAIN=local go test ./... -count=1
GOTOOLCHAIN=local go test -race ./... -count=1
GOTOOLCHAIN=local go vet ./...
GOTOOLCHAIN=local go build ./...
```

Tests use temporary real SQLite databases and cover migration/restart persistence, state transitions, rollback, tenant isolation, idempotency, deterministic concurrent ownership, context cancellation, error mapping, worker retry/stop, response-body lifecycle, pagination, and resource blockers.
