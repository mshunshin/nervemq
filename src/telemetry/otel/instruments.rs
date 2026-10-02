//! The metric instruments, named after the semantic conventions where one
//! fits and `nervemq.*` otherwise.

use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use opentelemetry::{
    metrics::{Counter, Histogram, Meter, UpDownCounter},
    KeyValue,
};
use sqlx::SqlitePool;

use crate::telemetry::messages::{
    now, MessageFacts, Queue, QueueGauge, Removal, SentMessage, VisibilityChange,
};

pub struct Instruments {
    meter: Meter,
    http_duration: Histogram<f64>,
    http_active: UpDownCounter<i64>,
    sent: Counter<u64>,
    delivered: Counter<u64>,
    removed: Counter<u64>,
    visibility_changed: Counter<u64>,
    body_size: Histogram<u64>,
    queue_time: Histogram<f64>,
    lifetime: Histogram<f64>,
    delivery_attempts: Histogram<u64>,
    /// The latest snapshot for the queue gauges: their callbacks run on the
    /// SDK's thread and can't query the database.
    queues: Arc<RwLock<Vec<QueueGauge>>>,
}

/// Buckets for message times, which are stored in whole seconds: from a
/// second to a day.
const MESSAGE_TIME_BUCKETS: [f64; 11] = [
    1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0, 14400.0, 86400.0,
];

fn queue_attributes(queue: Queue<'_>) -> [KeyValue; 2] {
    [
        KeyValue::new("nervemq.namespace", queue.namespace.to_owned()),
        KeyValue::new(
            "messaging.destination.name",
            format!("{}/{}", queue.namespace, queue.name),
        ),
    ]
}

/// One request's attributes on `http.server.request.duration`.
pub struct RequestAttributes<'a> {
    pub method: &'a str,
    /// The matched route pattern; `None` for a page of the UI, or a miss.
    pub route: Option<&'a str>,
    pub status: u16,
    /// The SQS action, for SQS requests.
    pub rpc_method: Option<&'static str>,
}

impl Instruments {
    pub fn new(meter: Meter) -> Self {
        let http_duration = meter
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .with_description("How long the server took to answer a request")
            // The conventions' boundaries, plus 20 s: a ReceiveMessage long
            // poll waits up to 20 s.
            .with_boundaries(vec![
                0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
                20.0,
            ])
            .build();
        let http_active = meter
            .i64_up_down_counter("http.server.active_requests")
            .with_unit("{request}")
            .with_description("Requests being answered")
            .build();

        let counter = |name: &'static str, description: &'static str| {
            meter
                .u64_counter(name)
                .with_unit("{message}")
                .with_description(description)
                .build()
        };
        let seconds = |name: &'static str, description: &'static str| {
            meter
                .f64_histogram(name)
                .with_unit("s")
                .with_description(description)
                .with_boundaries(MESSAGE_TIME_BUCKETS.to_vec())
                .build()
        };

        let queues = Arc::new(RwLock::new(Vec::new()));
        Self::observe_queues(&meter, &queues);

        Self {
            sent: counter("nervemq.messages.sent", "Messages stored by sends"),
            delivered: counter(
                "nervemq.messages.delivered",
                "Deliveries of messages to consumers, redeliveries included",
            ),
            removed: counter("nervemq.messages.removed", "Messages that left their queue"),
            visibility_changed: counter(
                "nervemq.messages.visibility_changed",
                "Visibility timeouts changed on messages in flight",
            ),
            body_size: meter
                .u64_histogram("nervemq.message.body.size")
                .with_unit("By")
                .with_description("Sizes of the message bodies sent")
                .with_boundaries(vec![
                    256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0,
                ])
                .build(),
            queue_time: seconds(
                "nervemq.message.queue_time",
                "How long messages waited for their first delivery",
            ),
            lifetime: seconds(
                "nervemq.message.lifetime",
                "How long acknowledged messages lived, from send to delete",
            ),
            delivery_attempts: meter
                .u64_histogram("nervemq.message.delivery_attempts")
                .with_unit("{delivery}")
                .with_description("Deliveries an acknowledged message took")
                .with_boundaries(vec![1.0, 2.0, 3.0, 5.0, 10.0])
                .build(),
            queues,
            meter,
            http_duration,
            http_active,
        }
    }

    pub fn sent(&self, queue: Queue<'_>, messages: &[SentMessage]) {
        let attributes = queue_attributes(queue);
        self.sent.add(messages.len() as u64, &attributes);
        for message in messages {
            self.body_size.record(message.body_bytes as u64, &attributes);
        }
    }

    pub fn delivered(&self, queue: Queue<'_>, messages: &[MessageFacts], now: u64) {
        let [namespace, destination] = queue_attributes(queue);
        for redelivery in [false, true] {
            let count = messages.iter().filter(|m| (m.tries > 1) == redelivery).count();
            if count > 0 {
                self.delivered.add(
                    count as u64,
                    &[
                        namespace.clone(),
                        destination.clone(),
                        KeyValue::new("nervemq.redelivery", redelivery),
                    ],
                );
            }
        }
        let attributes = [namespace, destination];
        for message in messages.iter().filter(|m| m.tries == 1) {
            if let Some(sent_at) = message.sent_at {
                self.queue_time
                    .record(now.saturating_sub(sent_at) as f64, &attributes);
            }
        }
    }

    pub fn removed(&self, queue: Queue<'_>, reason: Removal, messages: &[MessageFacts], now: u64) {
        self.removed_count(queue, reason, messages.len() as u64);
        if reason != Removal::Acknowledged {
            return;
        }
        let attributes = queue_attributes(queue);
        for message in messages {
            self.delivery_attempts.record(message.tries, &attributes);
            if let Some(sent_at) = message.sent_at {
                self.lifetime
                    .record(now.saturating_sub(sent_at) as f64, &attributes);
            }
        }
    }

    pub fn removed_count(&self, queue: Queue<'_>, reason: Removal, count: u64) {
        if count == 0 {
            return;
        }
        let [namespace, destination] = queue_attributes(queue);
        self.removed.add(
            count,
            &[
                namespace,
                destination,
                KeyValue::new("nervemq.removal.reason", reason.label()),
            ],
        );
    }

    pub fn visibility_changed(&self, queue: Queue<'_>, change: VisibilityChange, count: u64) {
        if count == 0 {
            return;
        }
        let [namespace, destination] = queue_attributes(queue);
        self.visibility_changed.add(
            count,
            &[
                namespace,
                destination,
                KeyValue::new("nervemq.visibility.change", change.label()),
            ],
        );
    }

    pub fn set_queue_gauges(&self, queues: Vec<QueueGauge>) {
        *self.queues.write().unwrap() = queues;
    }

    /// The per-queue gauges, read from the snapshot at each export. The
    /// age of the oldest message is worked out then, so it keeps growing
    /// between snapshots.
    fn observe_queues(meter: &Meter, queues: &Arc<RwLock<Vec<QueueGauge>>>) {
        let attributes = |gauge: &QueueGauge| {
            queue_attributes(Queue {
                namespace: &gauge.namespace,
                name: &gauge.queue,
            })
        };

        let snapshot = queues.clone();
        meter
            .u64_observable_gauge("nervemq.queue.messages")
            .with_unit("{message}")
            .with_description("Messages in each queue, by state")
            .with_callback(move |observer| {
                for gauge in snapshot.read().unwrap().iter() {
                    let [namespace, destination] = attributes(gauge);
                    for (state, count) in [
                        ("available", gauge.available),
                        ("in_flight", gauge.in_flight),
                        ("delayed", gauge.delayed),
                        ("failed", gauge.failed),
                    ] {
                        observer.observe(
                            count,
                            &[
                                namespace.clone(),
                                destination.clone(),
                                KeyValue::new("nervemq.message.state", state),
                            ],
                        );
                    }
                }
            })
            .build();

        let snapshot = queues.clone();
        meter
            .u64_observable_gauge("nervemq.queue.oldest_message.age")
            .with_unit("s")
            .with_description(
                "Age of each queue's oldest available message, like AWS's ApproximateAgeOfOldestMessage",
            )
            .with_callback(move |observer| {
                let now = now();
                for gauge in snapshot.read().unwrap().iter() {
                    if let Some(sent_at) = gauge.oldest_available_at {
                        observer.observe(now.saturating_sub(sent_at), &attributes(gauge));
                    }
                }
            })
            .build();

        let snapshot = queues.clone();
        meter
            .u64_observable_gauge("nervemq.queue.paused")
            .with_description("1 while a queue is paused, else 0")
            .with_callback(move |observer| {
                for gauge in snapshot.read().unwrap().iter() {
                    observer.observe(u64::from(gauge.paused), &attributes(gauge));
                }
            })
            .build();
    }

    pub fn request_started(&self, method: &str) {
        self.http_active
            .add(1, &[KeyValue::new("http.request.method", method.to_owned())]);
    }

    pub fn request_ended(&self, method: &str) {
        self.http_active
            .add(-1, &[KeyValue::new("http.request.method", method.to_owned())]);
    }

    pub fn request_answered(&self, request: RequestAttributes<'_>, duration: Duration) {
        let mut attributes = vec![
            KeyValue::new("http.request.method", request.method.to_owned()),
            KeyValue::new("http.response.status_code", i64::from(request.status)),
        ];
        if let Some(route) = request.route {
            attributes.push(KeyValue::new("http.route", route.to_owned()));
        }
        if let Some(rpc_method) = request.rpc_method {
            attributes.push(KeyValue::new("rpc.method", rpc_method));
        }
        if request.status >= 500 {
            attributes.push(KeyValue::new("error.type", request.status.to_string()));
        }
        self.http_duration.record(duration.as_secs_f64(), &attributes);
    }

    /// Gauges for the connection pools and the database files. The SDK
    /// calls these on its own thread at each export, so they only read
    /// cheap, synchronous values.
    pub fn observe_databases(&self, pools: Vec<(&'static str, SqlitePool)>, files: Vec<(&'static str, PathBuf)>) {
        self.meter
            .i64_observable_up_down_counter("db.client.connection.count")
            .with_unit("{connection}")
            .with_description("Connections in each SQLite pool, by state")
            .with_callback(move |observer| {
                for (pool_name, pool) in &pools {
                    let open = i64::from(pool.size());
                    let idle = pool.num_idle() as i64;
                    for (state, count) in [("idle", idle), ("used", open - idle)] {
                        observer.observe(
                            count,
                            &[
                                KeyValue::new("db.client.connection.pool.name", *pool_name),
                                KeyValue::new("db.client.connection.state", state),
                            ],
                        );
                    }
                }
            })
            .build();

        self.meter
            .u64_observable_gauge("nervemq.db.file.size")
            .with_unit("By")
            .with_description("Size of each SQLite file, write-ahead logs included")
            .with_callback(move |observer| {
                for (file, path) in &files {
                    // A WAL can be absent between checkpoints.
                    if let Ok(metadata) = std::fs::metadata(path) {
                        observer.observe(metadata.len(), &[KeyValue::new("nervemq.db.file", *file)]);
                    }
                }
            })
            .build();
    }
}
