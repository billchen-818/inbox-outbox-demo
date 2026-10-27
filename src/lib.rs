use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};
use thiserror::Error;

pub const CONSUMER_NAME: &str = "billing-worker-v1";
pub const DEFAULT_DATABASE_URL: &str =
    "postgres://inbox_outbox:inbox_outbox@127.0.0.1:55442/inbox_outbox";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChargeRequested {
    pub event_id: String,
    pub account_id: String,
    pub amount_cents: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleOutcome {
    Applied { balance_cents: i64 },
    Duplicate { balance_cents: i64 },
}

#[derive(Debug, Error)]
pub enum HandleError {
    #[error("amount_cents must be positive")]
    InvalidAmount,
    #[error("account does not exist: {0}")]
    AccountNotFound(String),
    #[error("account {account_id} has insufficient funds")]
    InsufficientFunds { account_id: String },
    #[error("event_id {event_id} was reused with different business fields")]
    EventIdCollision { event_id: String },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Encode(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct OutboxMessage {
    pub id: i64,
    pub event_id: String,
    pub topic: String,
    pub payload: String,
    pub attempts: i32,
}

#[derive(Serialize)]
struct ChargeDebited<'a> {
    event_id: String,
    source_event_id: &'a str,
    account_id: &'a str,
    amount_cents: i64,
    balance_cents: i64,
}

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
}

pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!().run(pool).await
}

pub async fn reset_demo(pool: &PgPool, balance_cents: i64) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("TRUNCATE outbox_events, ledger_entries, inbox_events, accounts RESTART IDENTITY")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO accounts (account_id, balance_cents) VALUES ($1, $2)")
        .bind("alice")
        .bind(balance_cents)
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

pub async fn handle_charge(
    pool: &PgPool,
    event: &ChargeRequested,
) -> Result<HandleOutcome, HandleError> {
    if event.amount_cents <= 0 {
        return Err(HandleError::InvalidAmount);
    }

    let mut tx = pool.begin().await?;

    let inserted: Option<i32> = sqlx::query_scalar(
        r#"
        INSERT INTO inbox_events (consumer_name, event_id, account_id, amount_cents)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        RETURNING 1
        "#,
    )
    .bind(CONSUMER_NAME)
    .bind(&event.event_id)
    .bind(&event.account_id)
    .bind(event.amount_cents)
    .fetch_optional(&mut *tx)
    .await?;

    if inserted.is_none() {
        let original: Option<(String, i64)> = sqlx::query_as(
            r#"
            SELECT account_id, amount_cents
            FROM inbox_events
            WHERE consumer_name = $1 AND event_id = $2
            "#,
        )
        .bind(CONSUMER_NAME)
        .bind(&event.event_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some((account_id, amount_cents)) = original else {
            return Err(HandleError::EventIdCollision {
                event_id: event.event_id.clone(),
            });
        };

        if account_id != event.account_id || amount_cents != event.amount_cents {
            return Err(HandleError::EventIdCollision {
                event_id: event.event_id.clone(),
            });
        }

        let balance_cents = account_balance(&mut tx, &event.account_id)
            .await?
            .ok_or_else(|| HandleError::AccountNotFound(event.account_id.clone()))?;
        tx.commit().await?;
        return Ok(HandleOutcome::Duplicate { balance_cents });
    }

    let balance_cents: Option<i64> = sqlx::query_scalar(
        r#"
        UPDATE accounts
        SET balance_cents = balance_cents - $2
        WHERE account_id = $1 AND balance_cents >= $2
        RETURNING balance_cents
        "#,
    )
    .bind(&event.account_id)
    .bind(event.amount_cents)
    .fetch_optional(&mut *tx)
    .await?;

    let balance_cents = match balance_cents {
        Some(balance) => balance,
        None if account_balance(&mut tx, &event.account_id).await?.is_none() => {
            return Err(HandleError::AccountNotFound(event.account_id.clone()));
        }
        None => {
            return Err(HandleError::InsufficientFunds {
                account_id: event.account_id.clone(),
            });
        }
    };

    sqlx::query(
        r#"
        INSERT INTO ledger_entries
            (consumer_name, event_id, account_id, amount_cents)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(CONSUMER_NAME)
    .bind(&event.event_id)
    .bind(&event.account_id)
    .bind(event.amount_cents)
    .execute(&mut *tx)
    .await?;

    let outbox_event_id = format!("charge-debited:{}", event.event_id);
    let payload = serde_json::to_string(&ChargeDebited {
        event_id: outbox_event_id.clone(),
        source_event_id: &event.event_id,
        account_id: &event.account_id,
        amount_cents: event.amount_cents,
        balance_cents,
    })?;

    sqlx::query(
        r#"
        INSERT INTO outbox_events (event_id, topic, payload)
        VALUES ($1, 'payments.debited', $2)
        "#,
    )
    .bind(outbox_event_id)
    .bind(payload)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(HandleOutcome::Applied { balance_cents })
}

async fn account_balance(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: &str,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar("SELECT balance_cents FROM accounts WHERE account_id = $1")
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await
}

pub async fn claim_outbox_batch(
    pool: &PgPool,
    claim_token: &str,
    batch_size: i64,
    lease_seconds: i64,
) -> Result<Vec<OutboxMessage>, sqlx::Error> {
    sqlx::query_as(
        r#"
        WITH claimable AS (
            SELECT id
            FROM outbox_events
            WHERE published_at IS NULL
              AND (lease_until IS NULL OR lease_until < now())
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT $1
        )
        UPDATE outbox_events AS outbox
        SET claim_token = $2,
            lease_until = now() + ($3::bigint * INTERVAL '1 second'),
            attempts = attempts + 1
        FROM claimable
        WHERE outbox.id = claimable.id
        RETURNING outbox.id, outbox.event_id, outbox.topic,
                  outbox.payload, outbox.attempts
        "#,
    )
    .bind(batch_size.max(1))
    .bind(claim_token)
    .bind(lease_seconds.max(1))
    .fetch_all(pool)
    .await
}

pub async fn mark_published(
    pool: &PgPool,
    outbox_id: i64,
    claim_token: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
        UPDATE outbox_events
        SET published_at = now(), claim_token = NULL, lease_until = NULL
        WHERE id = $1 AND claim_token = $2 AND published_at IS NULL
        "#,
    )
    .bind(outbox_id)
    .bind(claim_token)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() == 1)
}

pub async fn counts(pool: &PgPool) -> Result<(i64, i64, i64), sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT
            (SELECT count(*) FROM inbox_events),
            (SELECT count(*) FROM ledger_entries),
            (SELECT count(*) FROM outbox_events)
        "#,
    )
    .fetch_one(pool)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_json_keeps_the_stable_business_id() {
        let event = ChargeRequested {
            event_id: "evt-42".to_owned(),
            account_id: "alice".to_owned(),
            amount_cents: 500,
        };

        let json = serde_json::to_string(&event).unwrap();
        let decoded: ChargeRequested = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, event);
    }
}
