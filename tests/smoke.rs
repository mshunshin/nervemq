//! Smoke test: the real `nervemq` executable, used the way a deployment
//! uses it.
//!
//! The in-crate tests build the app inside the test process, so they never
//! cover startup itself: reading configuration from the environment and
//! `--data-dir`, running migrations on a real file, binding the port, serving
//! the embedded UI, and the CLI sharing the server's database. This test
//! does exactly that, and only along happy paths. Behaviour in depth is the
//! job of the in-crate suites.
//!
//! Run just this with `cargo test --test smoke`.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

use aws_sdk_sqs::config::{BehaviorVersion, Credentials, Region};

const BIN: &str = env!("CARGO_BIN_EXE_nervemq");
const ROOT_EMAIL: &str = "root@example.com";
const ROOT_PASSWORD: &str = "smoke-root-password";
const ACCESS_KEY: &str = "SMOKEACCESSKEY";
const SECRET_KEY: &str = "smoke-secret-key";

/// A `nervemq` command with a clean, fully specified environment, so
/// variables from the developer's shell or CI cannot change the outcome. It
/// runs in the temporary directory, so a path that ignored `--data-dir`
/// would land there, never in the checkout (which may hold a real
/// `nervemq.db`).
fn nervemq(data_dir: &Path, port: u16) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.current_dir(data_dir.parent().unwrap())
        .env_clear()
        .env("NERVEMQ_ROOT_EMAIL", ROOT_EMAIL)
        .env("NERVEMQ_ROOT_PASSWORD", ROOT_PASSWORD)
        .env("NERVEMQ_BIND_ADDRESS", format!("127.0.0.1:{port}"))
        .env("NERVEMQ_HOST", format!("http://127.0.0.1:{port}"))
        .arg("--data-dir")
        .arg(data_dir);
    cmd
}

/// Runs a CLI command to completion and requires it to succeed.
fn cli(data_dir: &Path, port: u16, args: &[&str]) -> Output {
    let out = nervemq(data_dir, port).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "`nervemq {}` failed: {}\n{}",
        args.join(" "),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// A port nothing is listening on right now.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The running server; killed when dropped, so a failing test never leaves
/// it behind.
struct Server {
    child: Child,
    log: std::path::PathBuf,
}

impl Server {
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts the server and waits until it answers HTTP, failing with its log
/// if it exits or does not come up.
fn start(data_dir: &Path, port: u16) -> Server {
    let log = data_dir.join("server.log");
    let file = std::fs::File::create(&log).unwrap();
    let child = nervemq(data_dir, port)
        .stdout(Stdio::from(file.try_clone().unwrap()))
        .stderr(Stdio::from(file))
        .spawn()
        .unwrap();
    let mut server = Server { child, log };

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            panic!("the server exited ({status}) while starting:\n{}", server.log());
        }
        if http(port, "GET", "/api/admin/auth/verify", &[], "").is_ok() {
            return server;
        }
        assert!(
            Instant::now() < deadline,
            "the server did not answer within 30s:\n{}",
            server.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A minimal HTTP/1.1 request: enough for a smoke test without adding an
/// HTTP client dependency. Returns (status, headers, body).
fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> std::io::Result<(u16, String, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\
         Content-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes())?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok((status, head.to_owned(), body.to_owned()))
}

fn sqs_client(port: u16, access_key: &str, secret_key: &str) -> aws_sdk_sqs::Client {
    let config = aws_sdk_sqs::Config::builder()
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(access_key, secret_key, None, None, "smoke"))
        .endpoint_url(format!("http://127.0.0.1:{port}/api/sqs"))
        .behavior_version(BehaviorVersion::latest())
        .build();
    aws_sdk_sqs::Client::from_conf(config)
}

#[tokio::test]
async fn the_binary_starts_and_serves_sqs_the_admin_api_and_the_ui() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let port = free_port();

    // The binary runs at all.
    let version = cli(&data_dir, port, &["--version"]);
    assert!(String::from_utf8_lossy(&version.stdout).contains("nervemq"));

    // Set up through the CLI, as an operator would before first start. The
    // credentials are supplied so the test knows the secret.
    cli(&data_dir, port, &["namespace", "add", "smoke"]);
    cli(
        &data_dir,
        port,
        &[
            "apikey", "add", "--name", "smoke", "--namespace", "smoke",
            "--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY,
        ],
    );
    assert!(data_dir.join("nervemq.db").exists(), "--data-dir was not used");

    let server = start(&data_dir, port);

    // SQS, through the official SDK.
    let sqs = sqs_client(port, ACCESS_KEY, SECRET_KEY);
    let url = sqs
        .create_queue()
        .queue_name("jobs")
        .send()
        .await
        .unwrap_or_else(|e| panic!("CreateQueue: {e:?}\n{}", server.log()))
        .queue_url
        .unwrap();
    assert_eq!(url, format!("http://127.0.0.1:{port}/api/sqs/smoke/jobs"));

    sqs.send_message()
        .queue_url(&url)
        .message_body("hello from the smoke test")
        .send()
        .await
        .unwrap();
    let received = sqs.receive_message().queue_url(&url).send().await.unwrap();
    let messages = received.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body(), Some("hello from the smoke test"));
    sqs.delete_message()
        .queue_url(&url)
        .receipt_handle(messages[0].receipt_handle().unwrap())
        .send()
        .await
        .unwrap();

    // Authentication is on: a wrong secret is refused.
    let forged = sqs_client(port, ACCESS_KEY, "not-the-secret");
    assert!(forged.list_queues().send().await.is_err());

    // The admin API: log in as root and see the namespace.
    let login = format!(r#"{{"email":"{ROOT_EMAIL}","password":"{ROOT_PASSWORD}"}}"#);
    let (status, head, body) = http(
        port,
        "POST",
        "/api/admin/auth/login",
        &[("Content-Type", "application/json")],
        &login,
    )
    .unwrap();
    assert_eq!(status, 200, "login: {body}");
    let cookie = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("set-cookie")
                .then(|| value.trim().split(';').next().unwrap().to_owned())
        })
        .expect("login set no session cookie");
    let (status, _, body) = http(port, "GET", "/api/admin/stats/ns", &[("Cookie", &cookie)], "").unwrap();
    assert_eq!(status, 200);
    assert!(body.contains(r#""name":"smoke""#), "{body}");

    // The embedded UI, including a deep link the SPA handles.
    if cfg!(feature = "embed-ui") {
        for path in ["/", "/login", "/queues/smoke/jobs"] {
            let (status, _, body) = http(port, "GET", path, &[], "").unwrap();
            assert_eq!(status, 200, "{path}");
            assert!(body.to_ascii_lowercase().contains("<html"), "{path} is not the UI");
        }
    }

    // The CLI works against the running server's database.
    let users = cli(&data_dir, port, &["user", "list"]);
    assert!(String::from_utf8_lossy(&users.stdout).contains(ROOT_EMAIL));

    drop(server);
}
