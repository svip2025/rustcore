//! Request-local deadlines. This does not alter admission, priority or reserve.
use std::{future::Future, time::Duration};
use tokio::time::Instant;
use tonic::{metadata::MetadataMap, Status};

#[derive(Clone, Copy, Debug)]
pub(super) enum RefreshOutcome {
    Local,
    Confirmed,
    Unavailable,
    DeadlineLocal,
    DeadlineAfterPersistence,
    TotalDeadline,
}

impl RefreshOutcome {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Confirmed => "confirmed",
            Self::Unavailable => "unavailable",
            Self::DeadlineLocal => "deadline_local",
            Self::DeadlineAfterPersistence => "deadline_after_persistence",
            Self::TotalDeadline => "total_deadline",
        }
    }
}

pub(super) struct GetOrderBudget {
    pub(super) entered: Instant,
    pub(super) total: Instant,
    refresh: Instant,
}

impl GetOrderBudget {
    pub(super) fn new(entered: Instant, metadata: &MetadataMap) -> Self {
        let header = metadata.get("grpc-timeout").and_then(|v| v.to_str().ok());
        let (total, refresh) = durations(header);
        Self {
            entered,
            total: entered + total,
            refresh: entered + refresh,
        }
    }

    pub(super) async fn finish<F, T>(&self, future: F) -> Result<T, Status>
    where
        F: Future<Output = Result<T, Status>>,
    {
        // timeout_at polls an immediately ready future before the timer. An
        // exhausted (including zero) budget must not start work at all.
        if Instant::now() >= self.total {
            return Err(total_deadline());
        }
        let result = tokio::time::timeout_at(self.total, future)
            .await
            .map_err(|_| total_deadline())?;
        if Instant::now() >= self.total {
            return Err(total_deadline());
        }
        result
    }

    fn expired(
        &self,
        persistence_started: bool,
        outcome: &mut RefreshOutcome,
    ) -> Result<bool, Status> {
        if Instant::now() < self.refresh {
            return Ok(false);
        }
        Self::refresh_deadline(persistence_started, outcome)
    }

    fn refresh_deadline(
        persistence_started: bool,
        outcome: &mut RefreshOutcome,
    ) -> Result<bool, Status> {
        if persistence_started {
            *outcome = RefreshOutcome::DeadlineAfterPersistence;
            Err(Status::deadline_exceeded(
                "GetOrder refresh deadline after persistence began; read durable state again",
            ))
        } else {
            *outcome = RefreshOutcome::DeadlineLocal;
            Ok(true)
        }
    }

    pub(super) async fn read<F, T>(
        &self,
        future: F,
        persistence_started: bool,
        outcome: &mut RefreshOutcome,
    ) -> Result<Option<T>, Status>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        if self.expired(persistence_started, outcome)? {
            return Ok(None);
        }
        let result = tokio::time::timeout_at(self.refresh, future).await;
        // This check also rejects a ready response first polled after expiry.
        // Dropping the owned future cancels queued admission/gate/HTTP work;
        // there is deliberately no spawned refresh task.
        if self.expired(persistence_started, outcome)? {
            return Ok(None);
        }
        match result {
            Ok(Ok(value)) => Ok(Some(value)),
            Ok(Err(_)) => {
                *outcome = RefreshOutcome::Unavailable;
                if persistence_started {
                    Err(Status::unavailable(
                        "GetOrder refresh failed after persistence began; read durable state again",
                    ))
                } else {
                    Ok(None)
                }
            }
            Err(_) => {
                Self::refresh_deadline(persistence_started, outcome)?;
                Ok(None)
            }
        }
    }

    pub(super) fn begin_persistence(
        &self,
        persistence_started: &mut bool,
        outcome: &mut RefreshOutcome,
    ) -> Result<bool, Status> {
        if self.expired(*persistence_started, outcome)? {
            return Ok(false);
        }
        // Conservatively include waiting to BEGIN. After this point a timeout
        // is an error, never a successful fallback or a claim of rollback.
        *persistence_started = true;
        Ok(true)
    }
}

fn total_deadline() -> Status {
    Status::deadline_exceeded(
        "GetOrder total deadline; any started persistence must be checked by a later read",
    )
}

fn durations(header: Option<&str>) -> (Duration, Duration) {
    const TOTAL_NS: u128 = 9_000_000_000;
    const REFRESH_NS: u128 = 8_000_000_000;
    // Invalid headers use the application cap, matching tonic's treatment of
    // an invalid timeout as no transport deadline. Strict digits reject signs,
    // whitespace, non-ASCII and more than eight digits before multiplication.
    let caller_ns = header.and_then(timeout_ns).unwrap_or(10_000_000_000);
    let total_ns = TOTAL_NS.min(caller_ns * 9 / 10);
    let refresh_ns = REFRESH_NS.min(total_ns * 8 / 9);
    (
        Duration::from_nanos(total_ns as u64),
        Duration::from_nanos(refresh_ns as u64),
    )
}

fn timeout_ns(raw: &str) -> Option<u128> {
    let bytes = raw.as_bytes();
    if !(2..=9).contains(&bytes.len()) {
        return None;
    }
    let (unit, digits) = bytes.split_last()?;
    if !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let factor = match unit {
        b'H' => 3_600_000_000_000,
        b'M' => 60_000_000_000,
        b'S' => 1_000_000_000,
        b'm' => 1_000_000,
        b'u' => 1_000,
        b'n' => 1,
        _ => return None,
    };
    // The largest valid value (99999999H) is well within u128.
    let number = digits
        .iter()
        .fold(0_u128, |n, digit| n * 10 + u128::from(digit - b'0'));
    Some(number * factor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_header_units_caps_and_integer_rounding() {
        for header in [None, Some("10S"), Some("99999999H"), Some("1M")] {
            assert_eq!(
                durations(header),
                (Duration::from_secs(9), Duration::from_secs(8))
            );
        }
        for invalid in [
            "",
            "S",
            "+1S",
            "-1S",
            " 1S",
            "1S ",
            "1s",
            "1.5S",
            "100000000S",
            "１S",
        ] {
            assert_eq!(durations(Some(invalid)), durations(None), "{invalid}");
        }
        for (header, total, refresh) in [
            ("1S", 900_000_000, 800_000_000),
            ("1500m", 1_350_000_000, 1_200_000_000),
            ("10u", 9_000, 8_000),
            ("11n", 9, 8),
            ("1n", 0, 0),
            ("0S", 0, 0),
        ] {
            assert_eq!(
                durations(Some(header)),
                (Duration::from_nanos(total), Duration::from_nanos(refresh))
            );
        }
    }
}
