//! A bounded budget owned only by an explicit v2 ReconcileNow handler.
//! Spawned safety settlements intentionally do not inherit this task local.
use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;
use tonic::{metadata::MetadataMap, Status};

pub(crate) const SERVER_CAP: Duration = Duration::from_millis(89_000);

tokio::task_local! {
    static DEADLINE: Instant;
}

/// gRPC timeout is 1..=8 decimal digits followed by exactly one case-sensitive
/// unit. Integer nanoseconds keep zero/sub-millisecond deadlines meaningful.
fn timeout_value(value: &str) -> Option<Duration> {
    let bytes = value.as_bytes();
    if !(2..=9).contains(&bytes.len()) || !bytes[..bytes.len() - 1].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let multiplier: u128 = match bytes[bytes.len() - 1] {
        b'H' => 3_600_000_000_000,
        b'M' => 60_000_000_000,
        b'S' => 1_000_000_000,
        b'm' => 1_000_000,
        b'u' => 1_000,
        b'n' => 1,
        _ => return None,
    };
    let number = value[..value.len() - 1].parse::<u128>().ok()?;
    let nanos = number.checked_mul(multiplier)?.checked_mul(9)? / 10;
    Some(Duration::from_nanos(nanos.min(SERVER_CAP.as_nanos()) as u64))
}

pub(crate) fn from_metadata(started: Instant, metadata: &MetadataMap) -> Instant {
    let budget = metadata
        .get("grpc-timeout")
        .and_then(|value| value.to_str().ok())
        .and_then(timeout_value)
        .unwrap_or(SERVER_CAP);
    started + budget
}

pub(crate) fn current_deadline() -> Option<Instant> {
    DEADLINE.try_with(|deadline| *deadline).ok()
}

fn exceeded() -> Status {
    Status::deadline_exceeded(
        "ReconcileNow budget exhausted; completed writes remain committed; reconcile again to obtain current truth",
    )
}

/// Also checked immediately before transport dispatch: an always-ready future
/// must not begin another outbound request after the monotonic deadline.
pub(crate) fn ensure_live() -> Result<(), Status> {
    if current_deadline().is_some_and(|deadline| Instant::now() >= deadline) {
        Err(exceeded())
    } else {
        Ok(())
    }
}

pub(crate) async fn run<T>(
    deadline: Instant,
    work: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    DEADLINE
        .scope(deadline, async {
            ensure_live()?;
            let result = tokio::time::timeout_at(deadline, work)
                .await
                .map_err(|_| exceeded())?;
            ensure_live()?;
            result
        })
        .await
}

#[cfg(test)]
#[path = "reconcile_budget_tests.rs"]
mod tests;
