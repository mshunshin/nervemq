//! AWS Signature Version 4 (SigV4) authentication implementation.
//!
//! This module provides functionality to authenticate requests using the AWS SigV4 protocol.
//! It verifies request signatures created using AWS-style credentials, following the same
//! signing process as AWS services.
//!
//! # Protocol Overview
//! SigV4 authentication involves:
//! 1. Creating a canonical request from the HTTP request
//! 2. Creating a string to sign using the canonical request
//! 3. Calculating the signature using a signing key
//! 4. Comparing the calculated signature with the provided signature
//!
//! For more details, see [AWS Signature Version 4 signing process](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)

use std::{pin::Pin, time::SystemTime};

use actix_web::{
    dev::ServiceRequest,
    web::{self},
    HttpMessage,
};
use aws_sigv4::sign::v4::generate_signing_key;
use bytes::BytesMut;
use futures_util::TryStreamExt;
use hmac::{digest::FixedOutput, Mac};
use itertools::Itertools;
use secrecy::ExposeSecret;
use sha2::Sha256;
use tracing::instrument;

use crate::{
    api::auth::User,
    auth::{
        credential::{AuthorizedNamespace, KeyAccess},
        crypto::sha256_hex,
    },
    error::Error,
};

/// How far a request's `X-Amz-Date` may be from the server's clock, either
/// way. AWS allows 15 minutes; NerveMQ allows clients' clocks to drift
/// further. It also bounds replay: a captured request stops authenticating
/// this long after its timestamp.
pub const MAX_CLOCK_DRIFT_SECS: i64 = 2 * 60 * 60;

/// Checks a request's `X-Amz-Date` against the server's clock `now` and
/// against the date in its credential scope, and returns the request time.
///
/// - Malformed, or a scope date that is not the timestamp's date:
///   `InvalidHeader` (answered as `IncompleteSignature`).
/// - More than [`MAX_CLOCK_DRIFT_SECS`] before or after `now`:
///   `SignatureExpired` (answered as `SignatureDoesNotMatch`, with AWS's
///   wording, which SDKs recognise as clock skew).
pub(crate) fn check_request_time(
    amz_date: &str,
    scope_date: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<chrono::DateTime<chrono::Utc>, Error> {
    const FORMAT: &str = "%Y%m%dT%H%M%SZ";

    let time = chrono::NaiveDateTime::parse_from_str(amz_date, FORMAT)
        .map_err(|_| Error::InvalidHeader {
            header: format!("x-amz-date: {amz_date:?} is not in ISO 8601 basic format"),
        })?
        .and_utc();

    if time.format("%Y%m%d").to_string() != scope_date {
        return Err(Error::InvalidHeader {
            header: format!(
                "authorization: credential scope date {scope_date} does not match \
                 X-Amz-Date {amz_date}"
            ),
        });
    }

    let window = chrono::Duration::seconds(MAX_CLOCK_DRIFT_SECS);
    if time < now - window {
        return Err(Error::SignatureExpired {
            message: format!(
                "Signature expired: {amz_date} is now earlier than {} ({} - 2 hours.)",
                (now - window).format(FORMAT),
                now.format(FORMAT),
            ),
        });
    }
    if time > now + window {
        return Err(Error::SignatureExpired {
            message: format!(
                "Signature not yet current: {amz_date} is still later than {} ({} + 2 hours.)",
                (now + window).format(FORMAT),
                now.format(FORMAT),
            ),
        });
    }

    Ok(time)
}

/// Represents the parsed components of an AWS SigV4 Authorization header.
///
/// This struct contains all the necessary information extracted from the
/// Authorization header required to verify the request signature.
#[derive(Debug)]
pub struct SigV4Header<'a> {
    /// The signing algorithm (typically "AWS4-HMAC-SHA256")
    pub algorithm: &'a str,
    /// The access key ID used to sign the request
    pub key_id: &'a str,
    /// The date when the signature was created (YYYYMMDD format)
    pub date: &'a str,
    /// List of headers included in the signature
    pub signed_headers: Vec<&'a str>,
    /// The request signature to verify
    pub signature: &'a str,
    /// AWS region name used in signing
    pub region: &'a str,
    /// AWS service name used in signing
    pub service: &'a str,
}

/// Authenticates a request using AWS Signature Version 4.
///
/// This function verifies the signature of an incoming request and returns the associated
/// user and namespace if authentication succeeds.
///
/// # Arguments
/// * `service` - Application service instance containing KMS and other configurations
/// * `req` - The incoming service request to authenticate
/// * `header` - Parsed SigV4 authorization header components
///
/// # Returns
/// * `Ok((User, AuthorizedNamespace, KeyAccess))` - The authenticated user, the key's
///   namespace, and the most the key may do
/// * `Err(Error)` - If authentication fails for any reason
///
/// # Authentication Process
/// 1. Retrieves and validates the API key from the database
/// 2. Decrypts the signing key using KMS
/// 3. Reconstructs the canonical request
/// 4. Generates the signature using the same process as the client
/// 5. Compares the generated signature with the provided signature
///
/// # Errors
/// * `Error::IdentityNotFound` - If the provided key ID doesn't exist
/// * `Error::MissingHeader` - If a required header is missing
/// * `Error::InvalidHeader` - If a header value is invalid
/// * `Error::Unauthorized` - If the signature verification fails
///
///
/// For implementation details, see [The AWS Signature Version 4 Signing Process](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html)
// Only the key id: the header also carries the signature.
#[instrument(skip_all, fields(key_id = header.key_id))]
pub async fn authenticate_sigv4(
    service: web::Data<crate::service::Service>,
    req: &mut ServiceRequest,
    header: SigV4Header<'_>,
) -> Result<(User, AuthorizedNamespace, KeyAccess), Error> {
    let payload = {
        let payload = req.take_payload();

        let bytes = payload
            .try_fold(BytesMut::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .map_err(|e| {
                tracing::error!("Error reading request payload: {}", e);
                Error::internal(e)
            })?
            .freeze();

        bytes
    };

    // One cached lookup resolves the decrypted signing secret, the key's
    // namespace and its owning user — on a cache hit this whole
    // authentication path touches the database zero times.
    let Some(credential) = service.signing_key(header.key_id).await? else {
        return Err(Error::IdentityNotFound {
            key_id: header.key_id.to_string(),
        }
        .into());
    };

    let x_amz_date = req
        .headers()
        .get("x-amz-date")
        .ok_or_else(|| Error::MissingHeader {
            header: "x-amz-date".to_string(),
        })?
        .to_str()
        .map_err(|_| Error::InvalidHeader {
            header: "x-amz-date".to_string(),
        })?;

    // Within MAX_CLOCK_DRIFT of the server's clock, either way; see
    // "Clock drift" in docs/architecture/namespaces.md.
    let request_time = check_request_time(x_amz_date, header.date, chrono::Utc::now())?;

    let payload_hash = sha256_hex(&payload);

    // Derived from the request's own date, as AWS does, not the server's:
    // a client a little slow or fast across midnight UTC signs with a
    // different date than the server's, and is still within the window.
    let signing_key = generate_signing_key(
        credential.secret.expose_secret(),
        SystemTime::from(request_time),
        header.region,
        header.service,
    );

    // The URL path, url-encoded, with the leading slash left in place (not encoded)
    let canonical_uri = req.uri().path();

    // Alphabetically-sorted query string parameters, url-encoded.
    //
    // Query parameters without values should be included with an equal sign (e.g., `key=` for `/?key`).
    let canonical_query = req
        .query_string()
        .split('&')
        .filter(|param| !param.is_empty())
        .map(|param| {
            let mut parts = param.split('=');
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");
            (urlencoding::encode(key), urlencoding::encode(value))
        })
        .sorted_by_key(|(k, _)| k.to_string())
        .map(|(k, v)| format!("{}={}", k, v))
        .join("&");

    // Alphabetically sort list of included headers
    let sorted_signed_headers = header
        .signed_headers
        .into_iter()
        .sorted()
        .map(|h| h.to_lowercase())
        .collect_vec();

    // Alphabetically-sorted headers, with the header name in lowercase, followed by a colon and
    // then the header value, separated by a newline.
    let canonical_headers = sorted_signed_headers
        .iter()
        .map(|header| {
            let value = req
                .headers()
                .get(header)
                .ok_or_else(|| Error::MissingHeader {
                    header: header.to_string(),
                })?
                .to_str()
                .map_err(|e| {
                    tracing::error!("Invalid header value: {}", e);

                    Error::InvalidHeader {
                        header: header.to_string(),
                    }
                })?;

            let canonical_value = value.trim().split_whitespace().join(" ");

            Ok(format!("{}:{}\n", header, canonical_value))
        })
        .collect::<Result<Vec<String>, Error>>()?
        .join("");

    // The list of included headers, separated by semicolon
    let signed_headers = sorted_signed_headers.join(";");

    let canonical_request = [
        &req.method().to_string(),
        &*canonical_uri,
        &canonical_query,
        &canonical_headers,
        &signed_headers,
        &payload_hash,
    ]
    .join("\n");

    let canonical_request_hash = sha256_hex(canonical_request.as_bytes());

    let credential_scope = [header.date, header.region, header.service, "aws4_request"].join("/");

    // Final string to sign; signature = HEX(HMAC-SHA256(string_to_sign))
    let string_to_sign = [
        header.algorithm,
        x_amz_date,
        &credential_scope,
        &canonical_request_hash,
    ]
    .join("\n");

    let generated_signature = {
        let mut mac = hmac::Hmac::<Sha256>::new_from_slice(signing_key.as_ref())
            .map_err(|e| Error::internal(e))?;

        mac.update(string_to_sign.as_bytes());

        hex::encode(mac.finalize_fixed())
    };

    // IMPORTANT: We must duplicate the payload and return it to the request,
    // since it may be needed by route handlers or other middleware.
    //
    // We probably don't need this if authorization fails, but return it to the request before
    // validating the hash just for consistency/sanity.
    req.set_payload(actix_web::dev::Payload::Stream {
        payload: Box::pin(futures_util::stream::once(std::future::ready(Ok(payload))))
            as Pin<Box<dyn futures_util::Stream<Item = Result<_, actix_web::error::PayloadError>>>>,
    });

    if header.signature != generated_signature {
        tracing::debug!(
            provided = header.signature,
            generated = generated_signature,
            "Invalid signature for request",
        );

        return Err(Error::Unauthorized);
    }

    tracing::debug!(
        key_id = header.key_id,
        namespace = credential.namespace,
        user_email = credential.user.email,
        "Request authenticated successfully"
    );

    Ok((
        credential.user,
        AuthorizedNamespace(credential.namespace),
        credential.access,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    /// 00:30 UTC: a two-hour window either way crosses midnight backwards.
    fn now() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 2, 0, 30, 0).unwrap()
    }

    fn check(amz_date: &str) -> Result<chrono::DateTime<Utc>, Error> {
        check_request_time(amz_date, &amz_date[..8], now())
    }

    #[test]
    fn requests_within_two_hours_either_way_are_accepted() {
        for amz_date in [
            "20261002T003000Z", // exactly now
            "20261001T223000Z", // exactly 2h slow, on the previous UTC day
            "20261001T235500Z", // a few minutes slow, across midnight
            "20261002T023000Z", // exactly 2h fast
        ] {
            assert!(check(amz_date).is_ok(), "{amz_date}");
        }
    }

    #[test]
    fn requests_beyond_two_hours_are_refused_with_awss_wording() {
        let err = check("20261001T222959Z").unwrap_err();
        assert!(
            matches!(&err, Error::SignatureExpired { message } if message.starts_with("Signature expired: 20261001T222959Z")),
            "{err}"
        );
        let err = check("20261002T023001Z").unwrap_err();
        assert!(
            matches!(&err, Error::SignatureExpired { message } if message.starts_with("Signature not yet current: 20261002T023001Z")),
            "{err}"
        );
        assert!(matches!(check("19990101T000000Z"), Err(Error::SignatureExpired { .. })));
    }

    #[test]
    fn the_scope_date_must_be_the_timestamps_date() {
        // Signed with today's scope but yesterday's timestamp, within the
        // window: still refused, as AWS does.
        assert!(matches!(
            check_request_time("20261001T235500Z", "20261002", now()),
            Err(Error::InvalidHeader { .. })
        ));
    }

    #[test]
    fn malformed_timestamps_are_refused() {
        for amz_date in ["2026-10-02T00:30:00Z", "20261002", "", "20261302T003000Z"] {
            assert!(
                matches!(check_request_time(amz_date, "20261002", now()), Err(Error::InvalidHeader { .. })),
                "{amz_date:?}"
            );
        }
    }
}
