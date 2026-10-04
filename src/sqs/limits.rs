//! AWS SQS's documented ranges for request parameters, queue attributes and
//! batches, kept in one place so every entry point enforces the same bounds.

use std::collections::HashMap;
use std::ops::RangeInclusive;

use crate::error::{AwsCode, BatchFault, Error};

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

/// Entries in one `SendMessageBatch`, `DeleteMessageBatch` or
/// `ChangeMessageVisibilityBatch`.
pub const MAX_BATCH_ENTRIES: usize = 10;

/// Longest batch entry `Id`, in characters.
pub const MAX_BATCH_ENTRY_ID_LENGTH: usize = 80;

/// Checks a batch request's entry `Id`s as AWS does: there is at least one
/// and at most [`MAX_BATCH_ENTRIES`], and each is 1 to
/// [`MAX_BATCH_ENTRY_ID_LENGTH`] letters, digits, hyphens and underscores,
/// different from the others. A failure refuses the whole request.
pub fn check_batch<'a>(ids: impl ExactSizeIterator<Item = &'a str>) -> Result<(), Error> {
    let count = ids.len();
    if count == 0 {
        return Err(Error::invalid_batch(
            BatchFault::Empty,
            "The request must contain at least one batch entry.",
        ));
    }
    if count > MAX_BATCH_ENTRIES {
        // AWS's wording.
        return Err(Error::invalid_batch(
            BatchFault::TooManyEntries,
            format!(
                "Maximum number of entries per request are {MAX_BATCH_ENTRIES}. \
                 You have sent {count}."
            ),
        ));
    }
    let mut seen = std::collections::HashSet::with_capacity(count);
    for id in ids {
        let well_formed = (1..=MAX_BATCH_ENTRY_ID_LENGTH).contains(&id.len())
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
        if !well_formed {
            // AWS's wording.
            return Err(Error::invalid_batch(
                BatchFault::InvalidEntryId,
                format!(
                    "A batch entry id can only contain alphanumeric characters, \
                     hyphens and underscores. It can be at most \
                     {MAX_BATCH_ENTRY_ID_LENGTH} letters long."
                ),
            ));
        }
        if !seen.insert(id) {
            return Err(Error::invalid_batch(
                BatchFault::IdsNotDistinct,
                format!("Id {id} is repeated: batch entry ids must be distinct."),
            ));
        }
    }
    Ok(())
}

/// Longest queue tag key, in characters.
pub const MAX_TAG_KEY_LENGTH: usize = 128;

/// Longest queue tag value, in characters.
pub const MAX_TAG_VALUE_LENGTH: usize = 256;

/// Checks queue tags against AWS's rules: a key of 1 to
/// [`MAX_TAG_KEY_LENGTH`] characters and a value of at most
/// [`MAX_TAG_VALUE_LENGTH`], both of letters and digits (any script),
/// whitespace and `_ . : / = + - @`, and neither starting with `aws:` in any
/// case. AWS only recommends at most 50 tags a queue, so the number isn't
/// checked.
pub fn check_tags(tags: &HashMap<String, String>) -> Result<(), Error> {
    let allowed = |text: &str| {
        text.chars()
            .all(|c| c.is_alphanumeric() || c.is_whitespace() || "_.:/=+-@".contains(c))
    };
    let reserved = |text: &str| text.get(..4).is_some_and(|p| p.eq_ignore_ascii_case("aws:"));

    // In key order, so a request with several faults always gets the same
    // answer.
    let mut tags: Vec<_> = tags.iter().collect();
    tags.sort();
    for (key, value) in tags {
        let problem = if key.is_empty() || key.chars().count() > MAX_TAG_KEY_LENGTH {
            format!("a key must be 1 to {MAX_TAG_KEY_LENGTH} characters")
        } else if value.chars().count() > MAX_TAG_VALUE_LENGTH {
            format!("a value can be at most {MAX_TAG_VALUE_LENGTH} characters")
        } else if !allowed(key) || !allowed(value) {
            "keys and values can contain only letters, digits, whitespace and _ . : / = + - @"
                .to_owned()
        } else if reserved(key) || reserved(value) {
            "keys and values can't start with aws:, which AWS reserves".to_owned()
        } else {
            continue;
        };
        return Err(Error::aws(
            AwsCode::InvalidParameterValue,
            format!("Invalid tag {key:?}: {problem}."),
        ));
    }
    Ok(())
}

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

    fn batch_fault(ids: &[&str]) -> Option<BatchFault> {
        match check_batch(ids.iter().copied()) {
            Ok(()) => None,
            Err(Error::InvalidBatch { fault, .. }) => Some(fault),
            Err(other) => panic!("{other:?}"),
        }
    }

    #[test]
    fn tags_follow_aws_s_rules() {
        let check = |key: &str, value: &str| {
            check_tags(&HashMap::from([(key.to_owned(), value.to_owned())]))
        };
        for (key, value) in [
            ("team", "payments"),
            ("cost centre", "a.b:c/d=e+f-g@h_i"),
            ("équipe", "日本"),
            (&"k".repeat(128), &"v".repeat(256)),
            ("empty value", ""),
            ("awsome", "aws"),
        ] {
            check(key, value).unwrap_or_else(|e| panic!("{key:?}={value:?}: {e}"));
        }
        for (key, value) in [
            ("", "v"),
            (&"k".repeat(129), "v"),
            ("k", &"v".repeat(257)),
            ("semi;colon", "v"),
            ("k", "comma,"),
            ("aws:owner", "v"),
            ("AWS:owner", "v"),
            ("k", "Aws:value"),
        ] {
            let err = check(key, value).expect_err(&format!("{key:?}={value:?}"));
            assert!(
                matches!(err, Error::Aws { code: AwsCode::InvalidParameterValue, .. }),
                "{err:?}"
            );
        }
        // AWS only recommends at most 50.
        let many = (0..60).map(|i| (i.to_string(), String::new())).collect();
        check_tags(&many).unwrap();
    }

    #[test]
    fn batches_hold_one_to_ten_entries() {
        let ids: Vec<String> = (0..=MAX_BATCH_ENTRIES).map(|i| i.to_string()).collect();
        let ids: Vec<&str> = ids.iter().map(String::as_str).collect();

        assert_eq!(batch_fault(&[]), Some(BatchFault::Empty));
        assert_eq!(batch_fault(&ids[..1]), None);
        assert_eq!(batch_fault(&ids[..MAX_BATCH_ENTRIES]), None);
        assert_eq!(batch_fault(&ids), Some(BatchFault::TooManyEntries));
        assert_eq!(
            check_batch(ids.iter().copied()).unwrap_err().to_string(),
            "Maximum number of entries per request are 10. You have sent 11."
        );
    }

    #[test]
    fn batch_entry_ids_are_well_formed_and_distinct() {
        let longest = "a".repeat(80);
        let too_long = "a".repeat(81);

        assert_eq!(batch_fault(&["Ab-0_", &longest]), None);
        for bad in ["", "invalid:id", "with space", "é", &too_long] {
            assert_eq!(batch_fault(&[bad]), Some(BatchFault::InvalidEntryId), "{bad:?}");
        }
        assert_eq!(batch_fault(&["a", "b", "a"]), Some(BatchFault::IdsNotDistinct));
        // Compared exactly.
        assert_eq!(batch_fault(&["a", "A"]), None);
    }

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
