//! # algolog
//!
//! Shared application log client for `algosolltd` apps.
//!
//! One event is one JSON line on stdout and, when `ALGOSTATUS_LOG_URL` and
//! `ALGOSTATUS_LOG_TOKEN` are set, part of a batch sent to
//! `POST {ALGOSTATUS_LOG_URL}/api/log/ingest`.
//!
//! The client is framework-agnostic: no async runtime, no tracing subscriber.
//! Emitting only takes a lock for an O(1) push, never waits for the network.
//!
//! ```no_run
//! use serde_json::json;
//!
//! let _guard = algolog::init(algolog::Config::from_env());
//! algolog::emit(algolog::Level::Info, "app.start", "started", json!({"version": "1.0"}));
//! algolog::info!("deploy.done", "image up");
//! algolog::warn!("queue.backlog", "depth above threshold", json!({"depth": 900}));
//! ```
//!
//! Keep the [`Guard`] alive for the process lifetime: dropping it flushes the
//! queue and stops the worker.

mod client;
mod config;
mod event;
mod queue;

use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

pub use client::Client;
pub use config::{Config, Level};
pub use event::Event;

use client::write_line;

/// The active client, if one was initialized with a usable transport.
static CLIENT: OnceLock<Arc<Client>> = OnceLock::new();
/// Configuration used by `emit` before/without a sending client, so stdout
/// keeps working when the URL or token is missing.
static FALLBACK: OnceLock<Config> = OnceLock::new();
static INIT: Mutex<()> = Mutex::new(());

/// Handle owning the worker thread.
///
/// Dropping or calling [`Guard::shutdown`] flushes what is queued and stops the
/// worker. A guard returned when config is incomplete is inert.
pub struct Guard {
    client: Option<Arc<Client>>,
}

impl Guard {
    /// Flush the queue and stop the worker. Idempotent.
    pub fn shutdown(&self) {
        if let Some(client) = self.client.as_ref() {
            client.shutdown();
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Install the process-wide client.
///
/// A second call returns an inert guard. When URL or token is missing the
/// client is a no-op for transport but still writes JSON lines to stdout.
pub fn init(config: Config) -> Guard {
    let _init = INIT.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if CLIENT.get().is_some() {
        return Guard { client: None };
    }
    let _ = FALLBACK.set(config.clone());
    if !config.can_send() {
        return Guard { client: None };
    }
    let client = Client::new(config);
    if CLIENT.set(Arc::clone(&client)).is_err() {
        client.shutdown();
        return Guard { client: None };
    }
    Guard {
        client: Some(client),
    }
}

/// Emit one event. Never blocks the caller, never throws.
///
/// `data` is a JSON object; pass [`serde_json::Value::Null`] for no data.
pub fn emit(level: Level, event: &str, msg: &str, data: Value) {
    if let Some(client) = CLIENT.get() {
        client.emit(level, event, msg, data);
        return;
    }
    let cfg = FALLBACK.get_or_init(Config::from_env);
    if cfg.disabled || !level.allows(cfg.level) {
        return;
    }
    let ev = Event::build(
        level,
        event,
        msg,
        data,
        cfg.project.as_deref(),
        &cfg.platform,
        &cfg.source,
    );
    write_line(&ev);
}

/// Submit an already-built event (for wrappers that attach `trace_id`, `actor`
/// or `dedupe_key`). Writes stdout and, when configured, enqueues for upload.
pub fn submit(event: Event) {
    if let Some(client) = CLIENT.get() {
        client.submit(event);
        return;
    }
    let cfg = FALLBACK.get_or_init(Config::from_env);
    if !cfg.disabled {
        write_line(&event);
    }
}

/// Current queue depth, 0 when no client is active.
pub fn queued() -> usize {
    CLIENT.get().map(|client| client.queue_len()).unwrap_or(0)
}

/// Events dropped by the bounded queue since start.
pub fn dropped() -> u64 {
    CLIENT.get().map(|client| client.dropped()).unwrap_or(0)
}

#[doc(hidden)]
pub fn __null() -> Value {
    Value::Null
}

/// Convenience macro. Two forms: `(event, msg)` and `(event, msg, data)`.
#[macro_export]
macro_rules! debug {
    ($event:expr, $msg:expr $(,)?) => {
        $crate::emit($crate::Level::Debug, $event, $msg, $crate::__null())
    };
    ($event:expr, $msg:expr, $data:expr $(,)?) => {
        $crate::emit($crate::Level::Debug, $event, $msg, $data)
    };
}

/// Convenience macro. Two forms: `(event, msg)` and `(event, msg, data)`.
#[macro_export]
macro_rules! info {
    ($event:expr, $msg:expr $(,)?) => {
        $crate::emit($crate::Level::Info, $event, $msg, $crate::__null())
    };
    ($event:expr, $msg:expr, $data:expr $(,)?) => {
        $crate::emit($crate::Level::Info, $event, $msg, $data)
    };
}

/// Convenience macro. Two forms: `(event, msg)` and `(event, msg, data)`.
#[macro_export]
macro_rules! warn {
    ($event:expr, $msg:expr $(,)?) => {
        $crate::emit($crate::Level::Warn, $event, $msg, $crate::__null())
    };
    ($event:expr, $msg:expr, $data:expr $(,)?) => {
        $crate::emit($crate::Level::Warn, $event, $msg, $data)
    };
}

/// Convenience macro. Two forms: `(event, msg)` and `(event, msg, data)`.
#[macro_export]
macro_rules! error {
    ($event:expr, $msg:expr $(,)?) => {
        $crate::emit($crate::Level::Error, $event, $msg, $crate::__null())
    };
    ($event:expr, $msg:expr, $data:expr $(,)?) => {
        $crate::emit($crate::Level::Error, $event, $msg, $data)
    };
}
