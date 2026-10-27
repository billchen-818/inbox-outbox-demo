use std::{env, error::Error};

use inbox_outbox_demo::{
    ChargeRequested, DEFAULT_DATABASE_URL, claim_outbox_batch, connect, counts, handle_charge,
    mark_published, migrate, reset_demo,
};

type MainResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> MainResult {
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned());
    let pool = connect(&database_url).await?;
    migrate(&pool).await?;
    reset_demo(&pool, 10_000).await?;

    let event = ChargeRequested {
        event_id: "charge-requested-42".to_owned(),
        account_id: "alice".to_owned(),
        amount_cents: 2_500,
    };

    let first = handle_charge(&pool, &event).await?;
    let duplicate = handle_charge(&pool, &event).await?;
    println!("first delivery: {first:?}");
    println!("duplicate delivery: {duplicate:?}");
    println!("inbox, ledger, outbox counts: {:?}", counts(&pool).await?);

    let claim_token = "relay-1-attempt-1";
    for message in claim_outbox_batch(&pool, claim_token, 100, 30).await? {
        // Replace this line with a broker publish and wait for its acknowledgement.
        println!(
            "publish {} to {}: {}",
            message.event_id, message.topic, message.payload
        );
        let marked = mark_published(&pool, message.id, claim_token).await?;
        assert!(marked, "the relay lost its lease before marking the row");
    }

    pool.close().await;
    Ok(())
}
