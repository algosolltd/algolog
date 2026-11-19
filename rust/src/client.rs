use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use serde_json::{json, Value};

use crate::config::{Config, Level};
use crate::event::{epoch_minute, Event};
use crate::queue::{BoundedQueue, BATCH_THRESHOLD, CAPACITY, MAX_BATCH};

/// Flush interval: a non-empty queue is sent within one second.
pub const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// Heartbeat interval.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
/// Retry backoff bounds for 5xx and transport errors.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// Attempts allowed while shutting down before giving up on a failing server.
/// One attempt: a dead server must not hold the process open.
const SHUTDOWN_ATTEMPTS: u32 = 1;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

enum SendOutcome {
    Ok,
    Reject(u16),
    Retry,
}

/// The sending half. One worker thread per client: it drains the bounded queue,
/// writes one HTTP request per batch and sends the heartbeat.
pub struct Client {
    cfg: Config,
    queue: Arc<BoundedQueue>,
    handle: Mutex<Option<JoinHandle<()>>>,
    sent: AtomicU64,
    rejected: AtomicU64,
}

impl Client {
    pub(crate) fn new(cfg: Config) -> Arc<Client> {
        let queue = Arc::new(BoundedQueue::new(CAPACITY));
        let worker_queue = Arc::clone(&queue);
        let worker_cfg = cfg.clone();
        let handle = thread::Builder::new()
            .name("algolog".to_string())
            .spawn(move || run_worker(worker_cfg, worker_queue))
            .ok();
        Arc::new(Client {
            cfg,
            queue,
            handle: Mutex::new(handle),
            sent: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn dropped(&self) -> u64 {
        self.queue.dropped()
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Build, print and enqueue one event. Never blocks on the network and never panics.
    pub fn emit(&self, level: Level, event: &str, msg: &str, data: Value) {
        if self.cfg.disabled || !level.allows(self.cfg.level) {
            return;
        }
        let ev = Event::build(
            level,
            event,
            msg,
            data,
            self.cfg.project.as_deref(),
            &self.cfg.platform,
            &self.cfg.source,
        );
        self.record(ev);
    }

    /// Enqueue an already-built event (used by wrappers that add trace/actor fields).
    pub fn submit(&self, event: Event) {
        if self.cfg.disabled {
            return;
        }
        self.record(event);
    }

    fn record(&self, event: Event) {
        write_line(&event);
        self.queue.push(event);
    }

    /// Flush the queue and stop the worker. Idempotent.
    pub fn shutdown(&self) {
        self.queue.close();
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

/// Build a heartbeat carrying the queue counters. The dedupe key is stable
/// inside one minute, so a duplicate tick collapses server-side.
pub(crate) fn heartbeat_event(cfg: &Config, queue: &BoundedQueue) -> Event {
    let now = SystemTime::now();
    let mut event = Event::build_at(
        now,
        Level::Info,
        "heartbeat",
        "heartbeat",
        json!({
            "heartbeat_s": HEARTBEAT_INTERVAL.as_secs(),
            "dropped": queue.dropped(),
            "queued": queue.len(),
        }),
        cfg.project.as_deref(),
        &cfg.platform,
        &cfg.source,
    );
    if let Some(project) = cfg.project.as_deref() {
        event = event.with_dedupe_key(format!(
            "hb:{}:{}:{}:{}",
            project,
            cfg.platform,
            cfg.source,
            epoch_minute(now)
        ));
    }
    event
}

fn run_worker(cfg: Config, queue: Arc<BoundedQueue>) {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build();
    let mut next_heartbeat = Instant::now() + HEARTBEAT_INTERVAL;
    let mut backoff = BACKOFF_MIN;

    loop {
        if Instant::now() >= next_heartbeat {
            let event = heartbeat_event(&cfg, &queue);
            write_line(&event);
            queue.push(event);
            next_heartbeat = Instant::now() + HEARTBEAT_INTERVAL;
        }

        let wait = (next_heartbeat.saturating_duration_since(Instant::now())).min(FLUSH_INTERVAL);
        let batch = match queue.wait_batch(wait, BATCH_THRESHOLD, MAX_BATCH) {
            Some(batch) if !batch.is_empty() => batch,
            Some(_) => continue,
            None => break,
        };

        let mut attempt: u32 = 0;
        loop {
            match send_batch(&agent, &cfg, &batch) {
                SendOutcome::Ok => {
                    backoff = BACKOFF_MIN;
                    break;
                }
                SendOutcome::Reject(status) => {
                    backoff = BACKOFF_MIN;
                    // One local line per rejected batch: it is dropped, not retried.
                    let event = Event::build(
                        Level::Error,
                        "log.rejected",
                        "batch rejected by algostatus",
                        json!({ "status": status, "events": batch.len() }),
                        cfg.project.as_deref(),
                        &cfg.platform,
                        &cfg.source,
                    );
                    write_line(&event);
                    break;
                }
                SendOutcome::Retry => {
                    attempt += 1;
                    if queue.is_closed() && attempt >= SHUTDOWN_ATTEMPTS {
                        break;
                    }
                    sleep_interruptible(backoff, &queue);
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
    }
}

fn sleep_interruptible(duration: Duration, queue: &BoundedQueue) {
    let deadline = Instant::now() + duration;
    loop {
        if queue.is_closed() {
            return;
        }
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        thread::sleep((deadline - now).min(Duration::from_millis(100)));
    }
}

fn send_batch(agent: &ureq::Agent, cfg: &Config, batch: &[Event]) -> SendOutcome {
    let url = match cfg.url.as_deref() {
        Some(url) => format!("{url}/api/log/ingest"),
        None => return SendOutcome::Retry,
    };
    let token = match cfg.token.as_deref() {
        Some(token) => token,
        None => return SendOutcome::Retry,
    };
    let body = match serde_json::to_string(&json!({ "events": batch })) {
        Ok(body) => body,
        Err(_) => return SendOutcome::Reject(0),
    };

    match agent
        .post(&url)
        .set("Authorization", &format!("Bearer {token}"))
        .set("Content-Type", "application/json")
        .send_string(&body)
    {
        Ok(_) => SendOutcome::Ok,
        Err(ureq::Error::Status(status, _)) if (400..500).contains(&status) => {
            SendOutcome::Reject(status)
        }
        Err(_) => SendOutcome::Retry,
    }
}

/// One JSON line on stdout. The same bytes are sent to the server, so
/// `docker logs` and Railway keep working. Write failures are ignored.
pub(crate) fn write_line(event: &Event) {
    let line = event.to_line();
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let _ = handle.write_all(line.as_bytes());
    let _ = handle.write_all(b"\n");
    let _ = handle.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;

    fn test_config(addr: std::net::SocketAddr) -> Config {
        Config {
            url: Some(format!("http://{addr}")),
            token: Some("test-token".to_string()),
            project: Some("algolog".to_string()),
            platform: "laptop".to_string(),
            source: "test".to_string(),
            level: Level::Debug,
            disabled: false,
        }
    }

    fn read_request(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            if let Some(pos) = find(&buf, b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos]).to_lowercase();
                let length = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                let body_start = pos + 4;
                while buf.len() < body_start + length {
                    let read = stream.read(&mut chunk).unwrap_or(0);
                    if read == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..read]);
                }
                return String::from_utf8_lossy(&buf).to_string();
            }
            let read = stream.read(&mut chunk).unwrap_or(0);
            if read == 0 {
                return String::from_utf8_lossy(&buf).to_string();
            }
            buf.extend_from_slice(&chunk[..read]);
        }
    }

    fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    fn respond(stream: &mut TcpStream, status: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    fn body_of(raw: &str) -> Value {
        let pos = raw.find("\r\n\r\n").expect("headers");
        serde_json::from_str(&raw[pos + 4..]).expect("json body")
    }

    #[test]
    fn sends_one_batch_with_auth_header_and_event_shape() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let raw = read_request(&mut stream);
            respond(
                &mut stream,
                "202 Accepted",
                "{\"ok\":true,\"accepted\":1,\"rejected\":0,\"stored\":1,\"mode\":\"memory\"}",
            );
            let _ = tx.send(raw);
        });

        let client = Client::new(test_config(addr));
        client.emit(Level::Info, "test.event", "hello", json!({ "a": 1 }));
        client.shutdown();

        let raw = rx.recv_timeout(Duration::from_secs(10)).expect("request");
        let head = raw.split("\r\n\r\n").next().unwrap_or("").to_lowercase();
        assert!(head.starts_with("post /api/log/ingest http/1.1"), "{head}");
        assert!(head.contains("authorization: bearer test-token"), "{head}");
        assert!(head.contains("content-type: application/json"), "{head}");

        let body = body_of(&raw);
        let events = body["events"].as_array().expect("events array");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event["level"], "info");
        assert_eq!(event["event"], "test.event");
        assert_eq!(event["msg"], "hello");
        assert_eq!(event["project"], "algolog");
        assert_eq!(event["platform"], "laptop");
        assert_eq!(event["source"], "test");
        assert_eq!(event["data"]["a"], 1);
        assert!(event["ts"].as_str().expect("ts").ends_with('Z'));
        assert!(event.get("trace_id").is_none());
    }

    #[test]
    fn a_4xx_batch_is_dropped_without_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = mpsc::channel::<bool>();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let _ = read_request(&mut stream);
            respond(&mut stream, "422 Unprocessable Entity", "{\"ok\":false}");
            let _ = tx.send(false);
            // A retry would arrive within a couple of seconds; there must be none.
            listener.set_nonblocking(true).ok();
            thread::sleep(Duration::from_secs(2));
            let second = listener.accept();
            let _ = tx.send(second.is_ok());
        });

        let client = Client::new(test_config(addr));
        client.emit(Level::Info, "test.reject", "no retry", Value::Null);
        rx.recv_timeout(Duration::from_secs(5))
            .expect("first request");
        let retried = rx.recv_timeout(Duration::from_secs(5)).expect("probe");
        assert!(!retried, "a 4xx batch must not be retried");
        client.shutdown();
    }

    #[test]
    fn level_filter_and_disabled_are_respected() {
        let cfg = Config {
            level: Level::Warn,
            url: Some("http://127.0.0.1:1".to_string()),
            ..Config::default()
        };
        let client = Client::new(cfg);
        client.emit(Level::Debug, "test.debug", "hidden", Value::Null);
        client.emit(Level::Info, "test.info", "hidden", Value::Null);
        assert_eq!(client.queue_len(), 0);
        client.emit(Level::Error, "test.error", "kept", Value::Null);
        assert_eq!(client.queue_len(), 1);
        client.shutdown();
    }

    #[test]
    fn heartbeat_carries_interval_and_counters() {
        let cfg = Config {
            project: Some("algoproj".to_string()),
            ..Config::default()
        };
        let queue = BoundedQueue::new(10);
        let event = heartbeat_event(&cfg, &queue);
        assert_eq!(event.event, "heartbeat");
        assert_eq!(event.level, Level::Info);
        assert_eq!(event.msg, "heartbeat");
        let data = event.data.clone().expect("data");
        assert_eq!(data["heartbeat_s"], 60);
        assert_eq!(data["dropped"], 0);
        assert_eq!(data["queued"], 0);
        let key = event.dedupe_key.clone().expect("dedupe key");
        assert!(key.starts_with("hb:algoproj:unknown:app:"), "{key}");
    }

    #[test]
    fn heartbeat_omits_dedupe_without_project() {
        let cfg = Config::default();
        let queue = BoundedQueue::new(10);
        let event = heartbeat_event(&cfg, &queue);
        assert!(event.dedupe_key.is_none());
    }
}
