//! Benchmarks NerveMQ's SQS-compatible API with the official AWS SDK for
//! Rust: the counterpart of `examples/python/benchmark.py`, with the same
//! scenarios, options and report. The defaults are larger (2000 messages,
//! 500 round trips, against Python's 300 and 100): the Rust SDK is fast
//! enough that 300 messages finish too quickly to measure reliably.
//!
//! Against a throwaway server (no setup, nothing left behind):
//!
//! ```sh
//! just bench                                   # from the repository root
//! cargo run --release --bin benchmark -- --spawn ../../target/release/nervemq
//! ```
//!
//! `--spawn` starts the given `nervemq` binary on a free port with a
//! temporary database, mints an API key with the CLI, benchmarks it, then
//! stops it and deletes the database.
//!
//! Against a running server, with a NerveMQ API key:
//!
//! ```sh
//! AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!     cargo run --release --bin benchmark -- --endpoint http://localhost:8080/api/sqs
//! ```
//!
//! Scenarios, each on a freshly purged queue:
//!
//! ```text
//! send_message (sequential)      one message per request, one at a time
//! send_message_batch (10/req)    10 messages per request
//! send_message (N concurrent)    one message per request, N in flight
//! receive + delete drain         pre-filled queue: receive 10, delete each
//! receive + batch delete drain   pre-filled queue: receive 10, one batch delete
//! send -> receive -> delete      one message at a time, end to end
//! ```
//!
//! Latency percentiles are per request (per batch for batch sends, per
//! receive-and-delete cycle for drains); msg/s counts messages, not
//! requests, per wall-clock second.

use std::{
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_sqs::{
    config::{retry::RetryConfig, Credentials},
    types::{DeleteMessageBatchRequestEntry, SendMessageBatchRequestEntry},
    Client,
};
use eyre::{bail, eyre, Context, Result};

/// AWS SQS's maximum entries per batch request.
const BATCH_SIZE: usize = 10;

const USAGE: &str = "\
usage: benchmark (--spawn <nervemq binary> | --endpoint <url>) [options]

  --spawn <path>         benchmark a throwaway server started from this binary
  --endpoint <url>       benchmark a running server (default: $NERVEMQ_ENDPOINT or
                         http://localhost:8080/api/sqs); needs AWS_ACCESS_KEY_ID and
                         AWS_SECRET_ACCESS_KEY set to a NerveMQ API key
  --messages <n>         messages per send/drain scenario (default: 2000)
  --round-trips <n>      iterations of the round-trip scenario (default: 500)
  --concurrency <n>      requests in flight for the concurrent send (default: 8)
  --payload-bytes <n>    message body size in bytes (default: 1024)";

struct Args {
    spawn: Option<PathBuf>,
    endpoint: Option<String>,
    messages: usize,
    round_trips: usize,
    concurrency: usize,
    payload_bytes: usize,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        spawn: None,
        endpoint: None,
        messages: 2000,
        round_trips: 500,
        concurrency: 8,
        payload_bytes: 1024,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        if flag == "-h" || flag == "--help" {
            println!("{USAGE}");
            std::process::exit(0);
        }
        let value = argv
            .next()
            .ok_or_else(|| eyre!("{flag} needs a value\n\n{USAGE}"))?;
        let number = || -> Result<usize> {
            match value.parse() {
                Ok(n) if n > 0 => Ok(n),
                _ => Err(eyre!("{flag} must be a positive number, got {value:?}")),
            }
        };
        match flag.as_str() {
            "--spawn" => args.spawn = Some(PathBuf::from(&value)),
            "--endpoint" => args.endpoint = Some(value.clone()),
            "--messages" => args.messages = number()?,
            "--round-trips" => args.round_trips = number()?,
            "--concurrency" => args.concurrency = number()?,
            "--payload-bytes" => args.payload_bytes = number()?,
            _ => bail!("unknown option {flag}\n\n{USAGE}"),
        }
    }
    if args.spawn.is_some() && args.endpoint.is_some() {
        bail!("give --spawn or --endpoint, not both");
    }
    Ok(args)
}

// ---------------------------------------------------------------------------
// A throwaway server
// ---------------------------------------------------------------------------

/// A server started with `--spawn`: killed, and its data deleted, on drop.
struct Server {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

/// Starts `binary` on a free port with a fresh database and an API key, and
/// returns it with its endpoint and the key.
fn spawn_server(binary: &PathBuf) -> Result<(Server, String, String, String)> {
    let binary = binary
        .canonicalize()
        .wrap_err_with(|| format!("no nervemq binary at {}", binary.display()))?;
    let port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let data_dir = std::env::temp_dir().join(format!("nervemq-bench-{}-{nonce}", std::process::id()));
    std::fs::create_dir_all(&data_dir)?;
    let (access_key, secret_key) = (format!("BENCH{nonce}"), format!("bench-secret-{nonce}"));

    // A clean, fully specified environment: nothing from the caller's shell
    // (e.g. NERVEMQ_DB_PATH) can point it at a real database.
    let nervemq = || {
        let mut cmd = Command::new(&binary);
        cmd.current_dir(&data_dir)
            .env_clear()
            .env("NERVEMQ_BIND_ADDRESS", format!("127.0.0.1:{port}"))
            .env("NERVEMQ_HOST", format!("http://127.0.0.1:{port}"))
            .env("NERVEMQ_ROOT_EMAIL", "bench@example.com")
            .env("NERVEMQ_ROOT_PASSWORD", &secret_key)
            .arg("--data-dir")
            .arg(&data_dir);
        if let Ok(log) = std::env::var("NERVEMQ_LOG") {
            cmd.env("NERVEMQ_LOG", log);
        }
        cmd
    };
    let cli = |args: &[&str]| -> Result<()> {
        let out = nervemq().args(args).output()?;
        if !out.status.success() {
            bail!(
                "`nervemq {}` failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    };
    cli(&["namespace", "add", "bench"])?;
    cli(&[
        "apikey", "add", "--name", "bench", "--namespace", "bench",
        "--access-key", &access_key, "--secret-key", &secret_key,
    ])?;

    let log = std::fs::File::create(data_dir.join("server.log"))?;
    let child = nervemq()
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()?;
    let mut server = Server {
        child,
        data_dir: data_dir.clone(),
    };

    let deadline = Instant::now() + Duration::from_secs(30);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        if let Some(status) = server.child.try_wait()? {
            let log = std::fs::read_to_string(data_dir.join("server.log")).unwrap_or_default();
            bail!("the server exited ({status}) while starting:\n{log}");
        }
        if Instant::now() > deadline {
            bail!("the server did not start listening within 30s");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Detach the guard's data_dir from the returned path for the report.
    let endpoint = format!("http://127.0.0.1:{port}/api/sqs");
    server.data_dir = data_dir;
    Ok((server, endpoint, access_key, secret_key))
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

struct Result_ {
    scenario: String,
    messages: usize,
    wall: Duration,
    /// Per request, or per batch / cycle.
    latencies: Vec<Duration>,
}

impl Result_ {
    fn throughput(&self) -> f64 {
        self.messages as f64 / self.wall.as_secs_f64().max(f64::EPSILON)
    }

    /// The same nearest-rank percentile as the Python benchmark.
    fn percentile_ms(&self, pct: f64) -> f64 {
        let mut ordered = self.latencies.clone();
        ordered.sort();
        if ordered.is_empty() {
            return 0.0;
        }
        let index = ((pct / 100.0 * (ordered.len() - 1) as f64).round() as usize).min(ordered.len() - 1);
        ordered[index].as_secs_f64() * 1000.0
    }
}

fn batch(size: usize, body: &str) -> Result<Vec<SendMessageBatchRequestEntry>> {
    (0..size)
        .map(|i| {
            SendMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .message_body(body)
                .build()
                .map_err(Into::into)
        })
        .collect()
}

/// Pre-loads a queue with batch sends (untimed).
async fn fill(sqs: &Client, url: &str, count: usize, body: &str) -> Result<()> {
    let mut sent = 0;
    while sent < count {
        let size = BATCH_SIZE.min(count - sent);
        sqs.send_message_batch()
            .queue_url(url)
            .set_entries(Some(batch(size, body)?))
            .send()
            .await?;
        sent += size;
    }
    Ok(())
}

async fn sequential_send(sqs: &Client, url: &str, count: usize, body: &str) -> Result<Result_> {
    let mut latencies = Vec::with_capacity(count);
    let started = Instant::now();
    for _ in 0..count {
        let t = Instant::now();
        sqs.send_message().queue_url(url).message_body(body).send().await?;
        latencies.push(t.elapsed());
    }
    Ok(Result_ {
        scenario: "send_message (sequential)".into(),
        messages: count,
        wall: started.elapsed(),
        latencies,
    })
}

async fn batch_send(sqs: &Client, url: &str, count: usize, body: &str) -> Result<Result_> {
    let mut latencies = Vec::new();
    let mut sent = 0;
    let started = Instant::now();
    while sent < count {
        let size = BATCH_SIZE.min(count - sent);
        let entries = batch(size, body)?;
        let t = Instant::now();
        sqs.send_message_batch()
            .queue_url(url)
            .set_entries(Some(entries))
            .send()
            .await?;
        latencies.push(t.elapsed());
        sent += size;
    }
    Ok(Result_ {
        scenario: format!("send_message_batch ({BATCH_SIZE}/req)"),
        messages: count,
        wall: started.elapsed(),
        latencies,
    })
}

async fn concurrent_send(
    sqs: &Client,
    url: &str,
    count: usize,
    body: &str,
    concurrency: usize,
) -> Result<Result_> {
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let workers: Vec<_> = (0..concurrency)
        .map(|_| {
            let (sqs, url, body, next) = (sqs.clone(), url.to_owned(), body.to_owned(), next.clone());
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                while next.fetch_add(1, Ordering::Relaxed) < count {
                    let t = Instant::now();
                    sqs.send_message().queue_url(&url).message_body(&body).send().await?;
                    latencies.push(t.elapsed());
                }
                Ok::<_, eyre::Report>(latencies)
            })
        })
        .collect();
    let mut latencies = Vec::with_capacity(count);
    for worker in workers {
        latencies.extend(worker.await??);
    }
    Ok(Result_ {
        scenario: format!("send_message ({concurrency} concurrent)"),
        messages: count,
        wall: started.elapsed(),
        latencies,
    })
}

/// Receives up to 10 at a time until the queue is empty, deleting them one
/// by one, or in one batch per receive.
async fn drain(sqs: &Client, url: &str, count: usize, body: &str, batched: bool) -> Result<Result_> {
    fill(sqs, url, count, body).await?;

    let mut latencies = Vec::new();
    let mut drained = 0;
    let started = Instant::now();
    loop {
        let t = Instant::now();
        let received = sqs
            .receive_message()
            .queue_url(url)
            .max_number_of_messages(BATCH_SIZE as i32)
            .send()
            .await?;
        let messages = received.messages();
        if batched && !messages.is_empty() {
            let entries = messages
                .iter()
                .enumerate()
                .map(|(i, m)| {
                    DeleteMessageBatchRequestEntry::builder()
                        .id(i.to_string())
                        .receipt_handle(m.receipt_handle().unwrap_or_default())
                        .build()
                        .map_err(eyre::Report::from)
                })
                .collect::<Result<Vec<_>>>()?;
            sqs.delete_message_batch()
                .queue_url(url)
                .set_entries(Some(entries))
                .send()
                .await?;
        } else {
            for m in messages {
                sqs.delete_message()
                    .queue_url(url)
                    .receipt_handle(m.receipt_handle().unwrap_or_default())
                    .send()
                    .await?;
            }
        }
        latencies.push(t.elapsed());
        if messages.is_empty() {
            break;
        }
        drained += messages.len();
    }
    Ok(Result_ {
        scenario: if batched {
            "receive + batch delete drain".into()
        } else {
            "receive + delete drain".into()
        },
        messages: drained,
        wall: started.elapsed(),
        latencies,
    })
}

async fn round_trip(sqs: &Client, url: &str, count: usize, body: &str) -> Result<Result_> {
    let mut latencies = Vec::with_capacity(count);
    let started = Instant::now();
    for _ in 0..count {
        let t = Instant::now();
        sqs.send_message().queue_url(url).message_body(body).send().await?;
        let received = sqs.receive_message().queue_url(url).send().await?;
        let [message] = received.messages() else {
            bail!("expected exactly one message, got {}", received.messages().len());
        };
        sqs.delete_message()
            .queue_url(url)
            .receipt_handle(message.receipt_handle().unwrap_or_default())
            .send()
            .await?;
        latencies.push(t.elapsed());
    }
    Ok(Result_ {
        scenario: "send -> receive -> delete".into(),
        messages: count,
        wall: started.elapsed(),
        latencies,
    })
}

// ---------------------------------------------------------------------------

fn report(endpoint: &str, payload_bytes: usize, results: &[Result_]) {
    println!();
    println!("NerveMQ SQS benchmark (Rust, aws-sdk-sqs) — {endpoint}");
    println!("payload: {payload_bytes} bytes per message");
    println!();
    let header = format!(
        "{:<32} {:>6} {:>8} {:>9} {:>8} {:>8} {:>8}",
        "scenario", "msgs", "wall s", "msg/s", "p50 ms", "p95 ms", "p99 ms"
    );
    println!("{header}");
    println!("{}", "-".repeat(header.len()));
    for r in results {
        println!(
            "{:<32} {:>6} {:>8.2} {:>9.1} {:>8.2} {:>8.2} {:>8.2}",
            r.scenario,
            r.messages,
            r.wall.as_secs_f64(),
            r.throughput(),
            r.percentile_ms(50.0),
            r.percentile_ms(95.0),
            r.percentile_ms(99.0),
        );
    }
    println!();
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;

    let (_server, endpoint, access_key, secret_key) = match &args.spawn {
        Some(binary) => {
            let (server, endpoint, access, secret) = spawn_server(binary)?;
            println!("started a throwaway server: {}", binary.display());
            (Some(server), endpoint, access, secret)
        }
        None => {
            let endpoint = args
                .endpoint
                .clone()
                .or_else(|| std::env::var("NERVEMQ_ENDPOINT").ok())
                .unwrap_or_else(|| "http://localhost:8080/api/sqs".into());
            let (Ok(access), Ok(secret)) = (
                std::env::var("AWS_ACCESS_KEY_ID"),
                std::env::var("AWS_SECRET_ACCESS_KEY"),
            ) else {
                bail!(
                    "set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to a NerveMQ API key \
                     (or use --spawn to benchmark a throwaway server)"
                );
            };
            (None, endpoint, access, secret)
        }
    };

    let sqs = Client::from_conf(
        aws_sdk_sqs::Config::builder()
            .region(Region::new("us-west-1"))
            .credentials_provider(Credentials::new(access_key, secret_key, None, None, "bench"))
            .endpoint_url(&endpoint)
            .retry_config(RetryConfig::disabled())
            .behavior_version(BehaviorVersion::latest())
            .build(),
    );

    let body: String = "0123456789abcdef"
        .chars()
        .cycle()
        .take(args.payload_bytes)
        .collect();
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() % 1_000_000_000_000;
    let url = sqs
        .create_queue()
        .queue_name(format!("bench{nonce}"))
        .send()
        .await
        .wrap_err("CreateQueue failed; is the server up, and the key an owner's or admin's?")?
        .queue_url
        .ok_or_else(|| eyre!("CreateQueue returned no URL"))?;
    println!("benchmarking against queue {url}");

    let run = async {
        // Warm up connections and the SQLite write path.
        for _ in 0..5 {
            sqs.send_message().queue_url(&url).message_body(&body).send().await?;
        }
        sqs.purge_queue().queue_url(&url).send().await?;

        let mut results = Vec::new();
        for scenario in 0..6 {
            let result = match scenario {
                0 => sequential_send(&sqs, &url, args.messages, &body).await?,
                1 => batch_send(&sqs, &url, args.messages, &body).await?,
                2 => concurrent_send(&sqs, &url, args.messages, &body, args.concurrency).await?,
                3 => drain(&sqs, &url, args.messages, &body, false).await?,
                4 => drain(&sqs, &url, args.messages, &body, true).await?,
                _ => round_trip(&sqs, &url, args.round_trips, &body).await?,
            };
            println!("  {}: done", result.scenario);
            results.push(result);
            sqs.purge_queue().queue_url(&url).send().await?;
        }
        Ok::<_, eyre::Report>(results)
    };
    let results = run.await;
    // Clean up even if a scenario failed (a spawned server is dropped anyway).
    let _ = sqs.delete_queue().queue_url(&url).send().await;

    report(&endpoint, args.payload_bytes, &results?);
    Ok(())
}
