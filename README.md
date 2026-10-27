# Inbox and transactional outbox demo

This project accompanies article 142. It demonstrates a PostgreSQL-backed billing
consumer with these invariants:

- one `(consumer_name, event_id)` can debit an account at most once;
- the Inbox row, ledger entry, account update, and Outbox event commit atomically;
- concurrent Relay workers claim different rows with `FOR UPDATE SKIP LOCKED`;
- only the current lease attempt can mark its claimed row as published.

Start PostgreSQL and run the demo:

```bash
docker compose up -d --wait
cargo run
```

The default connection string is:

```text
postgres://inbox_outbox:inbox_outbox@127.0.0.1:55442/inbox_outbox
```

Override it with `DATABASE_URL` when needed. Unit tests need no database. The
ignored integration tests use the PostgreSQL container:

```bash
cargo test
cargo test --test postgres -- --ignored --test-threads=1
```

The demo truncates its four tables on startup and in each integration test. Never
point it at a database containing valuable tables with the same names.

