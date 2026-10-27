use inbox_outbox_demo::{
    ChargeRequested, DEFAULT_DATABASE_URL, HandleError, HandleOutcome, claim_outbox_batch, connect,
    counts, handle_charge, mark_published, migrate, reset_demo,
};

async fn test_pool() -> sqlx::PgPool {
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned());
    let pool = connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    reset_demo(&pool, 10_000).await.unwrap();
    pool
}

fn charge() -> ChargeRequested {
    ChargeRequested {
        event_id: "evt-42".to_owned(),
        account_id: "alice".to_owned(),
        amount_cents: 2_500,
    }
}

#[tokio::test]
#[ignore = "requires the PostgreSQL container from compose.yaml"]
async fn duplicate_delivery_debits_once() {
    let pool = test_pool().await;

    assert_eq!(
        handle_charge(&pool, &charge()).await.unwrap(),
        HandleOutcome::Applied {
            balance_cents: 7_500
        }
    );
    assert_eq!(
        handle_charge(&pool, &charge()).await.unwrap(),
        HandleOutcome::Duplicate {
            balance_cents: 7_500
        }
    );
    assert_eq!(counts(&pool).await.unwrap(), (1, 1, 1));
}

#[tokio::test]
#[ignore = "requires the PostgreSQL container from compose.yaml"]
async fn concurrent_duplicate_delivery_debits_once() {
    let pool = test_pool().await;
    let first = tokio::spawn({
        let pool = pool.clone();
        async move { handle_charge(&pool, &charge()).await }
    });
    let second = tokio::spawn({
        let pool = pool.clone();
        async move { handle_charge(&pool, &charge()).await }
    });

    let outcomes = [
        first.await.unwrap().unwrap(),
        second.await.unwrap().unwrap(),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, HandleOutcome::Applied { .. }))
            .count(),
        1
    );
    assert_eq!(counts(&pool).await.unwrap(), (1, 1, 1));
}

#[tokio::test]
#[ignore = "requires the PostgreSQL container from compose.yaml"]
async fn rejects_reused_id_with_different_business_fields() {
    let pool = test_pool().await;
    handle_charge(&pool, &charge()).await.unwrap();

    let changed = ChargeRequested {
        amount_cents: 2_501,
        ..charge()
    };
    assert!(matches!(
        handle_charge(&pool, &changed).await,
        Err(HandleError::EventIdCollision { .. })
    ));
    assert_eq!(counts(&pool).await.unwrap(), (1, 1, 1));
}

#[tokio::test]
#[ignore = "requires the PostgreSQL container from compose.yaml"]
async fn only_the_current_lease_owner_can_mark_published() {
    let pool = test_pool().await;
    handle_charge(&pool, &charge()).await.unwrap();

    let messages = claim_outbox_batch(&pool, "claim-a", 10, 30).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert!(
        !mark_published(&pool, messages[0].id, "claim-b")
            .await
            .unwrap()
    );
    assert!(
        mark_published(&pool, messages[0].id, "claim-a")
            .await
            .unwrap()
    );
}
