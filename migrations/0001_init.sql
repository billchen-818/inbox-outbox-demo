CREATE TABLE IF NOT EXISTS accounts (
    account_id      TEXT PRIMARY KEY,
    balance_cents   BIGINT NOT NULL CHECK (balance_cents >= 0)
);

CREATE TABLE IF NOT EXISTS inbox_events (
    consumer_name   TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL CHECK (amount_cents > 0),
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id              BIGSERIAL PRIMARY KEY,
    consumer_name   TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    account_id      TEXT NOT NULL REFERENCES accounts(account_id),
    amount_cents    BIGINT NOT NULL CHECK (amount_cents > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (consumer_name, event_id)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL UNIQUE,
    topic           TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    claim_token     TEXT,
    lease_until     TIMESTAMPTZ,
    published_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox_events (id)
    WHERE published_at IS NULL;

