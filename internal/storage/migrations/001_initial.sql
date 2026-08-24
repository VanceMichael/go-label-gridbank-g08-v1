PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    email TEXT NOT NULL,
    display_name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('tenant_admin','operator','reviewer','data_steward','worker')),
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, email)
);

CREATE INDEX IF NOT EXISTS users_tenant_role_idx ON users(tenant_id, role, active);

CREATE TABLE IF NOT EXISTS auth_sessions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    user_id TEXT NOT NULL REFERENCES users(id),
    token_hash TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS auth_sessions_user_idx ON auth_sessions(tenant_id, user_id, expires_at);

CREATE TABLE IF NOT EXISTS providers (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    name TEXT NOT NULL,
    timezone TEXT NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS compute_pools (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    provider_id TEXT NOT NULL REFERENCES providers(id),
    name TEXT NOT NULL,
    capabilities INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, provider_id, name)
);

CREATE INDEX IF NOT EXISTS compute_pools_available_idx ON compute_pools(tenant_id, provider_id, active);

CREATE TABLE IF NOT EXISTS capacity_offers (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    name TEXT NOT NULL,
    environment TEXT NOT NULL,
    required_capabilities INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS workloads (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    provider_id TEXT NOT NULL REFERENCES providers(id),
    capacity_offer_id TEXT NOT NULL REFERENCES capacity_offers(id),
    pool_id TEXT NOT NULL REFERENCES compute_pools(id),
    operator_id TEXT NOT NULL REFERENCES users(id),
    status TEXT NOT NULL CHECK (status IN ('planned','ready','executing','metering','settled','failed','canceled','archived')),
    revision INTEGER NOT NULL DEFAULT 1,
    reservation_ref TEXT NOT NULL,
    started_at TEXT,
    submitted_at TEXT,
    settled_at TEXT,
    canceled_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS workloads_tenant_status_idx ON workloads(tenant_id, status, updated_at);

CREATE TABLE IF NOT EXISTS leases (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    pool_id TEXT NOT NULL REFERENCES compute_pools(id),
    workload_id TEXT NOT NULL REFERENCES workloads(id),
    owner TEXT NOT NULL,
    token TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, pool_id, workload_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS leases_one_live_idx
ON leases(tenant_id, pool_id) WHERE released_at IS NULL;

CREATE INDEX IF NOT EXISTS leases_expiry_idx ON leases(tenant_id, expires_at, released_at);

CREATE TABLE IF NOT EXISTS capacity_streams (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    workload_id TEXT NOT NULL REFERENCES workloads(id),
    kind TEXT NOT NULL CHECK (kind IN ('gpu','cpu','memory','storage','network')),
    status TEXT NOT NULL CHECK (status IN ('open','sealed','aligned','invalid')),
    segment_count INTEGER NOT NULL DEFAULT 0,
    first_nanos INTEGER NOT NULL DEFAULT 0,
    last_nanos INTEGER NOT NULL DEFAULT 0,
    digest TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, workload_id, kind)
);

CREATE TABLE IF NOT EXISTS usage_records (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    stream_id TEXT NOT NULL REFERENCES capacity_streams(id),
    sequence INTEGER NOT NULL,
    start_nanos INTEGER NOT NULL,
    end_nanos INTEGER NOT NULL,
    object_uri TEXT NOT NULL,
    checksum TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (tenant_id, stream_id, sequence),
    UNIQUE (tenant_id, stream_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS usage_records_order_idx ON usage_records(tenant_id, stream_id, sequence);

CREATE TABLE IF NOT EXISTS usage_batches (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    workload_id TEXT NOT NULL REFERENCES workloads(id),
    status TEXT NOT NULL CHECK (status IN ('open','claimed','submitted','accepted','rework','canceled')),
    owner TEXT NOT NULL DEFAULT '',
    lease_token TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT,
    submitted_at TEXT,
    reviewed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS usage_batches_claim_idx ON usage_batches(tenant_id, status, lease_expires_at, created_at);

CREATE TABLE IF NOT EXISTS usage_items (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    batch_id TEXT NOT NULL REFERENCES usage_batches(id),
    segment_id TEXT NOT NULL REFERENCES usage_records(id),
    label TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL DEFAULT '',
    complete INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, batch_id, segment_id)
);

CREATE TABLE IF NOT EXISTS quality_reviews (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    reviewer_id TEXT NOT NULL REFERENCES users(id),
    outcome TEXT NOT NULL CHECK (outcome IN ('approved','rejected','rework')),
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, object_type, object_id, version)
);

CREATE INDEX IF NOT EXISTS quality_reviews_object_idx ON quality_reviews(tenant_id, object_type, object_id, created_at);

CREATE TABLE IF NOT EXISTS credit_plans (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('draft','frozen','approved','published','revoked','archived')),
    revision INTEGER NOT NULL DEFAULT 1,
    digest TEXT NOT NULL DEFAULT '',
    item_count INTEGER NOT NULL DEFAULT 0,
    frozen_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, name, revision)
);

CREATE TABLE IF NOT EXISTS credit_plan_items (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    ledger_id TEXT NOT NULL REFERENCES credit_plans(id),
    workload_id TEXT NOT NULL REFERENCES workloads(id),
    revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (tenant_id, ledger_id, workload_id)
);

CREATE INDEX IF NOT EXISTS credit_plan_items_ledger_idx ON credit_plan_items(tenant_id, ledger_id, created_at);

CREATE TABLE IF NOT EXISTS credit_releases (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    ledger_id TEXT NOT NULL REFERENCES credit_plans(id),
    revision INTEGER NOT NULL,
    digest TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('approved','published','revoked','archived')),
    published_at TEXT,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, ledger_id, revision)
);

CREATE TABLE IF NOT EXISTS credit_release_items (
    release_id TEXT NOT NULL REFERENCES credit_releases(id),
    ledger_item_id TEXT NOT NULL REFERENCES credit_plan_items(id),
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    created_at TEXT NOT NULL,
    PRIMARY KEY (release_id, ledger_item_id)
);

CREATE TABLE IF NOT EXISTS compute_jobs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    release_id TEXT NOT NULL REFERENCES credit_releases(id),
    status TEXT NOT NULL CHECK (status IN ('queued','running','retrying','succeeded','failed','canceled')),
    owner TEXT NOT NULL DEFAULT '',
    lease_token TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    checkpoint TEXT NOT NULL DEFAULT '',
    output_uri TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    next_attempt_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS compute_jobs_claim_idx ON compute_jobs(tenant_id, status, next_attempt_at, lease_expires_at);

CREATE TABLE IF NOT EXISTS compute_attempts (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    job_id TEXT NOT NULL REFERENCES compute_jobs(id),
    attempt INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    outcome TEXT NOT NULL DEFAULT 'running',
    error_text TEXT NOT NULL DEFAULT '',
    UNIQUE (tenant_id, job_id, attempt)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    topic TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','delivering','delivered','dead')),
    owner TEXT NOT NULL DEFAULT '',
    lease_token TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_attempt_at TEXT NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS outbox_claim_idx ON outbox_events(tenant_id, status, next_attempt_at, lease_expires_at);

CREATE TABLE IF NOT EXISTS idempotency_records (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    response BLOB NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    UNIQUE (tenant_id, method, path, key)
);

CREATE INDEX IF NOT EXISTS idempotency_expiry_idx ON idempotency_records(expires_at);

CREATE TABLE IF NOT EXISTS audit_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    request_id TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS audit_object_idx ON audit_events(tenant_id, object_type, object_id, created_at);
CREATE INDEX IF NOT EXISTS audit_request_idx ON audit_events(tenant_id, request_id, created_at);

CREATE TABLE IF NOT EXISTS credit_accounts (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    currency TEXT NOT NULL,
    balance INTEGER NOT NULL CHECK (balance >= 0),
    held INTEGER NOT NULL CHECK (held >= 0 AND held <= balance),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, currency)
);

CREATE TABLE IF NOT EXISTS credit_entries (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    account_id TEXT NOT NULL REFERENCES credit_accounts(id),
    workload_id TEXT REFERENCES workloads(id),
    settlement_id TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('deposit','hold','release','charge','reversal')),
    amount INTEGER NOT NULL CHECK (amount > 0),
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (tenant_id, account_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS credit_entries_workload_idx
ON credit_entries(tenant_id, workload_id, created_at);

CREATE TABLE IF NOT EXISTS settlement_runs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    workload_id TEXT NOT NULL REFERENCES workloads(id),
    account_id TEXT NOT NULL REFERENCES credit_accounts(id),
    status TEXT NOT NULL CHECK (status IN ('pending','running','completed','failed','reversed')),
    amount INTEGER NOT NULL CHECK (amount > 0),
    error_text TEXT NOT NULL DEFAULT '',
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, workload_id)
);

CREATE INDEX IF NOT EXISTS settlement_runs_status_idx
ON settlement_runs(tenant_id, status, updated_at);
