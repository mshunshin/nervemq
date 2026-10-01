//! AWS SQS's documented ranges for request parameters and queue attributes,
//! kept in one place so every entry point enforces the same bounds.

use std::ops::RangeInclusive;

/// `VisibilityTimeout` (queue attribute, ReceiveMessage override and
/// ChangeMessageVisibility), in seconds: up to 12 hours.
pub const VISIBILITY_TIMEOUT: RangeInclusive<u64> = 0..=43_200;

/// `DelaySeconds` (queue attribute and per message), in seconds: up to
/// 15 minutes.
pub const DELAY_SECONDS: RangeInclusive<u64> = 0..=900;

/// `MaximumMessageSize` queue attribute, in bytes: 1 KiB to 1 MiB.
pub const MAXIMUM_MESSAGE_SIZE: RangeInclusive<u64> = 1_024..=1_048_576;

/// `MessageRetentionPeriod` queue attribute, in seconds: 1 minute to
/// 14 days. NerveMQ also accepts 0, meaning "retain forever".
pub const MESSAGE_RETENTION_PERIOD: RangeInclusive<u64> = 60..=1_209_600;

/// `ReceiveMessageWaitTimeSeconds` queue attribute, in seconds.
pub const RECEIVE_MESSAGE_WAIT_TIME_SECONDS: RangeInclusive<u64> = 0..=20;

/// Checks `value` against `range`, describing a failure for the caller to
/// wrap in the error its API reports.
pub fn check_range(
    name: &str,
    value: u64,
    range: &RangeInclusive<u64>,
    unit: &str,
) -> Result<(), String> {
    if range.contains(&value) {
        Ok(())
    } else {
        Err(format!(
            "{name}: must be between {} and {} {unit}, got {value}",
            range.start(),
            range.end()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_are_inclusive() {
        assert!(check_range("VisibilityTimeout", 0, &VISIBILITY_TIMEOUT, "seconds").is_ok());
        assert!(check_range("VisibilityTimeout", 43_200, &VISIBILITY_TIMEOUT, "seconds").is_ok());
        assert_eq!(
            check_range("VisibilityTimeout", 43_201, &VISIBILITY_TIMEOUT, "seconds"),
            Err("VisibilityTimeout: must be between 0 and 43200 seconds, got 43201".to_string())
        );
    }
}
