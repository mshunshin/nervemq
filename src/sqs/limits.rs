//! AWS SQS's documented ranges for request parameters, queue attributes and
//! batches, kept in one place so every entry point enforces the same bounds.

use std::collections::HashMap;
use std::ops::RangeInclusive;

use crate::error::{AwsCode, BatchFault, Error};
use crate::sqs::types::{AttributeKind, SqsMessageAttribute};

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

/// Most message attributes one message can carry.
pub const MAX_MESSAGE_ATTRIBUTES: usize = 10;

/// Longest message attribute name, and longest data type (custom label
/// included), in characters.
pub const MAX_ATTRIBUTE_NAME_LENGTH: usize = 256;

/// Most significant digits a `Number` attribute can have.
pub const MAX_NUMBER_DIGITS: usize = 38;

/// Whether `c` may appear in a message body or a `String` attribute: the
/// characters XML allows, `#x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD |
/// #x10000-#x10FFFF`.
pub fn is_message_char(c: char) -> bool {
    matches!(c,
        '\u{9}' | '\u{A}' | '\u{D}'
        | '\u{20}'..='\u{D7FF}'
        | '\u{E000}'..='\u{FFFD}'
        | '\u{10000}'..='\u{10FFFF}')
}

/// Checks a message before it is stored, as AWS does:
///
/// - the body is at least one character (`MissingParameter`), of the
///   characters [`is_message_char`] allows (`InvalidMessageContents`);
/// - at most [`MAX_MESSAGE_ATTRIBUTES`] attributes;
/// - each attribute's name is 1 to [`MAX_ATTRIBUTE_NAME_LENGTH`] letters
///   and digits (any script: AWS accepts `attr.1øßä`), `_`, `-` and `.`,
///   doesn't start with `AWS.` or `Amazon.` in any case, and doesn't start,
///   end or double up on `.`;
/// - its data type is `String`, `Number` or `Binary`, optionally followed
///   by a period and a custom label, which AWS doesn't interpret and
///   restricts only to a message body's characters, at most
///   [`MAX_ATTRIBUTE_NAME_LENGTH`] characters in all;
/// - it carries a non-empty value of its kind, a `String`'s of allowed
///   characters and a `Number`'s a number AWS can hold ([`check_number`]).
///
/// The attribute faults are `InvalidParameterValue`. Attributes are checked
/// in name order, so a message with several faults always gets the same
/// answer.
pub fn check_message(
    body: &str,
    attributes: &HashMap<String, SqsMessageAttribute>,
) -> Result<(), Error> {
    if body.is_empty() {
        // AWS's wording.
        return Err(Error::aws(
            AwsCode::MissingParameter,
            "The request must contain the parameter MessageBody.",
        ));
    }
    if !body.chars().all(is_message_char) {
        return Err(Error::aws(
            AwsCode::InvalidMessageContents,
            "Invalid characters found. Valid unicode characters are #x9 | #xA | #xD | \
             #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF",
        ));
    }
    if attributes.len() > MAX_MESSAGE_ATTRIBUTES {
        return Err(Error::aws(
            AwsCode::InvalidParameterValue,
            format!(
                "Number of message attributes [{}] exceeds the allowed maximum \
                 [{MAX_MESSAGE_ATTRIBUTES}].",
                attributes.len()
            ),
        ));
    }

    let mut attributes: Vec<_> = attributes.iter().collect();
    attributes.sort_by_key(|(name, _)| *name);
    for (name, attribute) in attributes {
        check_attribute(name, attribute)
            .map_err(|problem| Error::aws(AwsCode::InvalidParameterValue, problem))?;
    }
    Ok(())
}

/// One message attribute's fault, worded for the caller.
fn check_attribute(name: &str, attribute: &SqsMessageAttribute) -> Result<(), String> {
    let lower = name.to_lowercase();
    if name.is_empty()
        || name.chars().count() > MAX_ATTRIBUTE_NAME_LENGTH
        || !name
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
        || name.starts_with('.')
        || name.ends_with('.')
        || name.contains("..")
        || lower.starts_with("aws.")
        || lower.starts_with("amazon.")
    {
        return Err(format!(
            "Message (user) attribute name '{name}' is invalid. A name is 1 to \
             {MAX_ATTRIBUTE_NAME_LENGTH} letters, digits, underscores, hyphens and periods, \
             doesn't start or end with a period or have two in a row, and doesn't start \
             with AWS. or Amazon."
        ));
    }

    let data_type = attribute.data_type.as_str();
    let label_ok = data_type
        .split_once('.')
        .is_none_or(|(_, label)| label.chars().all(is_message_char));
    let Some(kind) = attribute
        .kind()
        .filter(|_| label_ok && data_type.chars().count() <= MAX_ATTRIBUTE_NAME_LENGTH)
    else {
        return Err(format!(
            "Message (user) attribute '{name}' has an invalid data type '{data_type}'. \
             It must be String, Number or Binary, optionally followed by a period and a \
             custom label, at most {MAX_ATTRIBUTE_NAME_LENGTH} characters."
        ));
    };

    // AWS's wording.
    let empty = || {
        format!(
            "Message (user) attribute '{name}' must contain a non-empty value of type '{}'.",
            kind.name()
        )
    };
    match kind {
        AttributeKind::Binary => {
            if attribute.binary_value.as_ref().is_none_or(Vec::is_empty) {
                return Err(empty());
            }
        }
        AttributeKind::String | AttributeKind::Number => {
            let value = attribute
                .string_value
                .as_deref()
                .filter(|v| !v.is_empty())
                .ok_or_else(empty)?;
            if !value.chars().all(is_message_char) {
                return Err(format!(
                    "Message (user) attribute '{name}' contains characters a message can't."
                ));
            }
            if kind == AttributeKind::Number {
                check_number(value).map_err(|problem| {
                    format!("Message (user) attribute '{name}' is not a valid number: {problem}.")
                })?;
            }
        }
    }
    Ok(())
}

const OUT_OF_RANGE: &str = "it is outside 10^-128 to 10^126 in magnitude";

/// Checks a `Number` attribute's value as AWS describes its numbers: a
/// decimal (`-1`, `3.14`, `.5`, `6.02e23`) of at most [`MAX_NUMBER_DIGITS`]
/// significant digits, between 10^-128 and 10^126 in magnitude, or zero.
pub fn check_number(value: &str) -> Result<(), &'static str> {
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit())
    {
        return Err("it isn't a decimal number");
    }
    let exponent_digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
    if exponent_digits.is_empty() || !exponent_digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err("its exponent isn't a whole number");
    }
    // Past any magnitude AWS holds, whatever the mantissa.
    let exponent: i64 = exponent.parse().map_err(|_| OUT_OF_RANGE)?;

    let digits: Vec<u8> = whole.bytes().chain(fraction.bytes()).collect();
    let Some(first) = digits.iter().position(|&d| d != b'0') else {
        return Ok(()); // Zero.
    };
    let last = digits.iter().rposition(|&d| d != b'0').unwrap_or(first);
    if last - first + 1 > MAX_NUMBER_DIGITS {
        return Err("it has more than 38 significant digits");
    }
    // The power of ten of the leading digit.
    let magnitude = exponent.saturating_add(whole.len() as i64 - 1 - first as i64);
    let only_a_one = digits[first] == b'1' && first == last;
    if !(-128..=126).contains(&magnitude) || (magnitude == 126 && !only_a_one) {
        return Err(OUT_OF_RANGE);
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
    fn numbers_are_aws_s_numbers() {
        let digits38 = "1".repeat(38);
        let digits39 = "1".repeat(39);
        for valid in [
            "0", "-0", "0.000", "+7", "-1", "3.14", ".5", "5.", "00042", "6.02e23", "6.02E+23",
            "1e126", "-1e126", "1e-128", "100e124", "0.01e-126", &digits38, "1.10000000000000000000000000000000000000000",
        ] {
            assert_eq!(check_number(valid), Ok(()), "{valid}");
        }
        for invalid in [
            "", "-", "+", ".", "e5", "abc", "1e", "1e+", "1.2.3", "1,000", " 1", "0x10", "NaN",
            "Infinity", "1e127", "2e126", "1.1e126", "1e-129", "0.1e-128", &digits39,
            "1e99999999999999999999",
        ] {
            assert!(check_number(invalid).is_err(), "{invalid}");
        }
        // The reason reaches the client, so it must be the right one.
        for (invalid, reason) in [
            ("ten", "it isn't a decimal number"),
            ("e5", "it isn't a decimal number"),
            ("1.2.3", "it isn't a decimal number"),
            ("1e", "its exponent isn't a whole number"),
            ("1e1.5", "its exponent isn't a whole number"),
            ("1e127", "it is outside 10^-128 to 10^126 in magnitude"),
            ("1e99999999999999999999", "it is outside 10^-128 to 10^126 in magnitude"),
            (&digits39, "it has more than 38 significant digits"),
        ] {
            assert_eq!(check_number(invalid), Err(reason), "{invalid}");
        }
    }

    #[test]
    fn messages_are_checked_as_aws_checks_them() {
        let attributes = |pairs: &[(&str, SqsMessageAttribute)]| -> HashMap<String, SqsMessageAttribute> {
            pairs.iter().map(|(n, a)| (n.to_string(), a.clone())).collect()
        };
        let code = |body: &str, attrs: &HashMap<String, SqsMessageAttribute>| match check_message(body, attrs) {
            Ok(()) => None,
            Err(Error::Aws { code, message }) => Some((code, message)),
            Err(other) => panic!("{other:?}"),
        };
        let none = HashMap::new();

        assert_eq!(code("héllo 日本 🦀\t\n\r", &none), None);
        assert_eq!(
            code("", &none),
            Some((
                AwsCode::MissingParameter,
                "The request must contain the parameter MessageBody.".to_owned()
            ))
        );
        for bad in ["\u{0}", "a\u{8}", "\u{B}", "\u{1F}", "\u{FFFE}", "\u{FFFF}"] {
            assert_eq!(
                code(bad, &none).map(|(c, _)| c),
                Some(AwsCode::InvalidMessageContents),
                "{bad:?}"
            );
        }

        let ok = |pairs: &[(&str, SqsMessageAttribute)]| code("body", &attributes(pairs));
        let invalid = |pairs: &[(&str, SqsMessageAttribute)]| {
            assert_eq!(
                ok(pairs).map(|(c, _)| c),
                Some(AwsCode::InvalidParameterValue),
                "{pairs:?}"
            )
        };
        let s = SqsMessageAttribute::string("v");

        // Names.
        assert_eq!(ok(&[("SOME_Valid.attribute-Name", s.clone()), ("123", s.clone())]), None);
        assert_eq!(ok(&[(&"n".repeat(256), s.clone())]), None);
        // Letters of any script: AWS accepts `attr.1øßä` (LocalStack's
        // AWS-validated baseline). The limit counts characters, not bytes.
        assert_eq!(ok(&[("attr.1øßä", s.clone()), ("é", s.clone()), ("größe", s.clone())]), None);
        assert_eq!(ok(&[(&"é".repeat(256), s.clone())]), None);
        for name in [
            "", ".lead", "trail.", "two..dots", "aWs.x", "AMAZON.x", "amazon.", "bang!",
            "sp ace", "Invalid-§-attr", "Invalid-\"-attr", "Invalid-(-attr", "Invalid-?-attr",
            &"n".repeat(257), &"é".repeat(257),
        ] {
            invalid(&[(name, s.clone())]);
        }
        // `AWS` without the period is just a name.
        assert_eq!(ok(&[("AWSome", s.clone()), ("Amazonian", s.clone())]), None);

        // How many.
        let ten: Vec<(String, SqsMessageAttribute)> =
            (0..10).map(|i| (format!("a{i}"), s.clone())).collect();
        let ten: Vec<(&str, SqsMessageAttribute)> =
            ten.iter().map(|(n, a)| (n.as_str(), a.clone())).collect();
        assert_eq!(ok(&ten), None);
        let mut eleven = ten.clone();
        eleven.push(("a10", s.clone()));
        invalid(&eleven);

        // Data types, custom labels included.
        let typed = |data_type: &str, string: Option<&str>, binary: Option<&[u8]>| {
            SqsMessageAttribute {
                data_type: data_type.to_owned(),
                string_value: string.map(str::to_owned),
                binary_value: binary.map(<[u8]>::to_vec),
            }
        };
        // AWS doesn't interpret a custom label, and restricts it only to a
        // message body's characters.
        for good in [
            typed("String.json", Some("{}"), None),
            typed("Number.int", Some("42"), None),
            typed("Binary.png", None, Some(&[0x89])),
            typed("Number.float.v2", Some("1.5"), None),
            typed("String.a b", Some("v"), None),
            typed("Binary.image/png", None, Some(&[0x89])),
            typed("String.application/json", Some("{}"), None),
            typed("String.日本", Some("v"), None),
            typed("Number.", Some("1"), None),
            // 256 characters in all.
            typed(&format!("Number.{}", "L".repeat(249)), Some("1"), None),
        ] {
            assert_eq!(ok(&[("a", good.clone())]), None, "{good:?}");
        }
        for bad in [
            typed("Invalid", Some("v"), None),
            typed("string", Some("v"), None),
            typed("String.bell\u{7}", Some("v"), None),
            typed(&format!("Number.{}", "L".repeat(250)), Some("1"), None),
        ] {
            invalid(&[("a", bad)]);
        }

        // Values are checked by kind, whatever the label.
        for bad in [
            typed("Number.int", Some("abc"), None),
            typed("Number.int", Some("1e127"), None),
            typed("String.json", Some("a\u{7}"), None),
            typed("Binary.png", None, Some(&[])),
            typed("Binary.png", Some("v"), None),
        ] {
            invalid(&[("a", bad)]);
        }
        assert_eq!(
            ok(&[("a", typed("String.json", Some(""), None))]).map(|(_, m)| m),
            Some(
                "Message (user) attribute 'a' must contain a non-empty value of type 'String'."
                    .to_owned()
            )
        );

        // Values: present, non-empty, of the attribute's kind.
        assert_eq!(
            ok(&[("ErrorDetails", SqsMessageAttribute::string(""))]),
            Some((
                AwsCode::InvalidParameterValue,
                "Message (user) attribute 'ErrorDetails' must contain a non-empty value of \
                 type 'String'."
                    .to_owned()
            ))
        );
        invalid(&[("a", SqsMessageAttribute::binary(Vec::new()))]);
        invalid(&[("a", typed("String", None, Some(b"v")))]);
        invalid(&[("a", typed("Binary", Some("v"), None))]);
        invalid(&[("a", SqsMessageAttribute::string("bell\u{7}"))]);
        invalid(&[("a", SqsMessageAttribute::number("12 apples"))]);
        invalid(&[("a", SqsMessageAttribute::number("1e127"))]);

        // The first fault wins: the body, then how many attributes, then the
        // attributes in name order, whatever order they arrive in.
        let eleven = attributes(&eleven);
        assert_eq!(code("", &eleven).map(|(c, _)| c), Some(AwsCode::MissingParameter));
        assert_eq!(code("\u{1}", &eleven).map(|(c, _)| c), Some(AwsCode::InvalidMessageContents));
        // A fresh map each time: each has its own random iteration order.
        for _ in 0..20 {
            let two_faults = attributes(&[
                ("b", typed("Text", Some("v"), None)),
                ("a", SqsMessageAttribute::string("")),
            ]);
            let message = code("x", &two_faults).unwrap().1;
            assert!(message.contains("'a'"), "{message}");
        }
    }

    /// The edges of the characters AWS allows in a body or a `String` value.
    #[test]
    fn message_characters_are_xml_s() {
        for (c, allowed) in [
            ('\u{8}', false),
            ('\u{9}', true),
            ('\u{A}', true),
            ('\u{B}', false),
            ('\u{D}', true),
            ('\u{1F}', false),
            ('\u{20}', true),
            ('\u{D7FF}', true),
            ('\u{E000}', true),
            ('\u{FFFD}', true),
            ('\u{FFFE}', false),
            ('\u{FFFF}', false),
            ('\u{10000}', true),
            ('\u{10FFFF}', true),
        ] {
            assert_eq!(is_message_char(c), allowed, "U+{:04X}", c as u32);
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
