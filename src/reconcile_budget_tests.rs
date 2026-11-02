use super::*;
use crate::rate_limiter::{Admission, CredentialRateLimiter, RequestClass, TokenBucketConfig};

#[test]
fn reconcile_budget_strict_header_units_and_cap() {
    for (raw, nanos) in [
        ("0S", 0),
        ("1n", 0),
        ("2n", 1),
        ("1u", 900),
        ("1m", 900_000),
        ("1S", 900_000_000),
        ("1M", 54_000_000_000),
        ("1H", 89_000_000_000),
        ("99999999H", 89_000_000_000),
        ("90000m", 81_000_000_000),
    ] {
        assert_eq!(
            timeout_value(raw),
            Some(Duration::from_nanos(nanos)),
            "{raw}"
        );
    }
    let start = Instant::now();
    assert_eq!(
        from_metadata(start, &MetadataMap::new()),
        start + SERVER_CAP
    );
    for raw in [
        "",
        "1",
        "S",
        "-1S",
        "+1S",
        "1.5S",
        " 1S",
        "1S ",
        "1s",
        "1ms",
        "123456789S",
        "9999999999999999999999999999H",
    ] {
        assert_eq!(timeout_value(raw), None, "{raw}");
        let mut metadata = MetadataMap::new();
        metadata.insert("grpc-timeout", raw.parse().unwrap());
        assert_eq!(from_metadata(start, &metadata), start + SERVER_CAP, "{raw}");
    }
}

#[tokio::test]
async fn reconcile_budget_expired_never_polls_work_or_reports_success() {
    let polled = std::sync::atomic::AtomicBool::new(false);
    let result = run(Instant::now(), async {
        polled.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    })
    .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
    let result = run(Instant::now() + Duration::from_millis(15), async {
        std::thread::sleep(Duration::from_millis(25));
        Ok(())
    })
    .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
}

#[tokio::test]
async fn reconcile_budget_scope_cancels_waiter_without_leaking_or_refunding() {
    let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(10.0, 0.0));
    let scope = format!("k08-scope-{}", ulid::Ulid::new());
    let started = Instant::now();
    let result = run(started + Duration::from_millis(80), async {
        assert!(current_deadline().is_some());
        assert!(tokio::spawn(async { current_deadline().is_none() })
            .await
            .unwrap());
        // Exit still spends the reserved slice immediately. Normal must wait.
        assert_eq!(
            limiter
                .acquire_prioritized(&scope, "exit", 1.0, RequestClass::Exit, 0.0)
                .await,
            Admission::Admitted
        );
        assert_eq!(
            limiter
                .acquire_prioritized(&scope, "normal", 1.0, RequestClass::PeriodicReconcile, 10.0)
                .await,
            Admission::TimedOut
        );
        std::future::pending::<Result<(), Status>>().await
    })
    .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    assert!(started.elapsed() < Duration::from_millis(400));
    assert_eq!(limiter.queued(&scope), 0);
    assert_eq!(limiter.stored_tokens(&scope).await, Some(9.0));
    assert!(current_deadline().is_none());
    assert_eq!(
        limiter
            .acquire_prioritized(&scope, "next", 1.0, RequestClass::EventTruth, 0.0)
            .await,
        Admission::Admitted
    );
}

#[tokio::test]
async fn reconcile_budget_unscoped_background_keeps_fifteen_seconds() {
    let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(1.0, 0.0));
    let scope = format!("k08-unscoped-{}", ulid::Ulid::new());
    let started = Instant::now();
    let admission = tokio::time::timeout(
        Duration::from_secs(17),
        limiter.acquire_prioritized(
            &scope,
            "background",
            2.0,
            RequestClass::PeriodicReconcile,
            0.0,
        ),
    )
    .await
    .expect("background queue keeps its configured cap");
    assert_eq!(admission, Admission::TimedOut);
    assert!(started.elapsed() >= Duration::from_millis(14_900));
    assert_eq!(limiter.queued(&scope), 0);
}
