//! The metric instruments, named after the semantic conventions where one
//! fits and `nervemq.*` otherwise.

use std::{path::PathBuf, time::Duration};

use opentelemetry::{
    metrics::{Histogram, Meter, UpDownCounter},
    KeyValue,
};
use sqlx::SqlitePool;

pub struct Instruments {
    meter: Meter,
    http_duration: Histogram<f64>,
    http_active: UpDownCounter<i64>,
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
        Self {
            meter,
            http_duration,
            http_active,
        }
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
