use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};

use crate::config::Level;

pub const MAX_MSG_CHARS: usize = 2000;
pub const MAX_DATA_BYTES: usize = 4096;
pub const MAX_TRACE_CHARS: usize = 120;
pub const MAX_ACTOR_CHARS: usize = 80;
pub const MAX_DEDUPE_CHARS: usize = 200;

/// One event, serialized identically to stdout and to the ingest body.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub ts: String,
    pub level: Level,
    pub event: String,
    pub msg: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub platform: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<String>,
}

impl Event {
    /// Build a normalized event. This is the single place where caps are applied.
    pub fn build(
        level: Level,
        event: &str,
        msg: &str,
        data: Value,
        project: Option<&str>,
        platform: &str,
        source: &str,
    ) -> Event {
        Event::build_at(
            SystemTime::now(),
            level,
            event,
            msg,
            data,
            project,
            platform,
            source,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_at(
        now: SystemTime,
        level: Level,
        event: &str,
        msg: &str,
        data: Value,
        project: Option<&str>,
        platform: &str,
        source: &str,
    ) -> Event {
        let mut msg = truncate_chars(msg, MAX_MSG_CHARS);
        if msg.trim().is_empty() {
            // The server rejects an empty message (1..2000). Use the event code
            // rather than inventing prose.
            msg = if event.trim().is_empty() {
                "log".to_string()
            } else {
                truncate_chars(event, MAX_MSG_CHARS)
            };
        }
        Event {
            ts: iso8601(now),
            level,
            event: if event.trim().is_empty() {
                "log".to_string()
            } else {
                event.to_string()
            },
            msg,
            project: project.map(|p| p.to_string()),
            platform: platform.to_string(),
            source: source.to_string(),
            data: normalize_data(data),
            trace_id: None,
            actor: None,
            dedupe_key: None,
        }
    }

    /// Attach a stable idempotency key (truncated to the contract cap).
    pub fn with_dedupe_key(mut self, key: impl Into<String>) -> Event {
        let key = truncate_chars(&key.into(), MAX_DEDUPE_CHARS);
        self.dedupe_key = if key.is_empty() { None } else { Some(key) };
        self
    }

    /// Attach a trace id (truncated to the contract cap).
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Event {
        let value = truncate_chars(&trace_id.into(), MAX_TRACE_CHARS);
        self.trace_id = if value.is_empty() { None } else { Some(value) };
        self
    }

    /// Attach an actor (truncated to the contract cap).
    pub fn with_actor(mut self, actor: impl Into<String>) -> Event {
        let value = truncate_chars(&actor.into(), MAX_ACTOR_CHARS);
        self.actor = if value.is_empty() { None } else { Some(value) };
        self
    }

    /// One JSON line, as written to stdout.
    pub fn to_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            // Serialization of this struct cannot fail; keep the path panic-free.
            "{\"level\":\"error\",\"event\":\"log.serialize\",\"msg\":\"event serialization failed\"}"
                .to_string()
        })
    }
}

/// Truncate to at most `max` Unicode scalar values (not bytes).
pub fn truncate_chars(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    value.chars().take(max).collect()
}

/// `data` is capped at 4096 serialized bytes; anything larger becomes `{"truncated":true}`.
/// Non-object payloads are wrapped as `{"value": <payload>}` so `data` is always
/// a JSON object on the wire.
pub fn normalize_data(data: Value) -> Option<Value> {
    let data = match data {
        Value::Null => return None,
        Value::Object(_) => data,
        other => json!({ "value": other }),
    };
    let bytes = serde_json::to_vec(&data)
        .map(|b| b.len())
        .unwrap_or(usize::MAX);
    if bytes > MAX_DATA_BYTES {
        Some(json!({ "truncated": true }))
    } else {
        Some(data)
    }
}

/// UTC ISO 8601 with milliseconds, e.g. `2026-11-19T12:00:00.123Z`.
pub fn iso8601(at: SystemTime) -> String {
    let duration = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year,
        month,
        day,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60,
        millis
    )
}

/// Epoch minute, used to make the heartbeat key stable inside one minute.
pub fn epoch_minute(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() / 60)
        .unwrap_or(0)
}

/// Howard Hinnant's civil_from_days: days since 1970-01-01 -> (year, month, day).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (year + i64::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn iso8601_matches_known_instants() {
        assert_eq!(iso8601(at(0)), "1970-01-01T00:00:00.000Z");
        // 2026-11-19T12:00:00Z
        assert_eq!(iso8601(at(1_795_089_600)), "2026-11-19T12:00:00.000Z");
        assert_eq!(
            iso8601(at(1_795_089_600) + Duration::from_millis(123)),
            "2026-11-19T12:00:00.123Z"
        );
    }

    #[test]
    fn non_object_data_is_wrapped() {
        let event = Event::build(Level::Info, "t", "m", json!([1, 2]), None, "p", "s");
        assert_eq!(event.data, Some(json!({ "value": [1, 2] })));
    }

    #[test]
    fn msg_is_truncated_to_2000_chars() {
        let long = "é".repeat(2500);
        let event = Event::build(Level::Info, "t", &long, Value::Null, None, "p", "s");
        assert_eq!(event.msg.chars().count(), MAX_MSG_CHARS);
    }

    #[test]
    fn empty_msg_falls_back_to_event_code() {
        let event = Event::build(Level::Warn, "db.timeout", "", Value::Null, None, "p", "s");
        assert_eq!(event.msg, "db.timeout");
    }

    #[test]
    fn data_over_4kb_is_replaced() {
        let big = json!({ "blob": "x".repeat(5000) });
        let event = Event::build(Level::Info, "t", "m", big, None, "p", "s");
        assert_eq!(event.data, Some(json!({ "truncated": true })));
        let small = json!({ "status": 500 });
        let event = Event::build(Level::Info, "t", "m", small.clone(), None, "p", "s");
        assert_eq!(event.data, Some(small));
    }

    #[test]
    fn null_data_is_omitted_and_json_has_no_nulls() {
        let event = Event::build(Level::Info, "t", "m", Value::Null, None, "p", "s");
        let line = event.to_line();
        assert!(!line.contains("null"), "{line}");
        assert!(!line.contains("\"data\""), "{line}");
    }

    #[test]
    fn epoch_minute_is_stable_within_a_minute() {
        assert_eq!(epoch_minute(at(3_600)), 60);
        assert_eq!(epoch_minute(at(3_659)), 60);
        assert_eq!(epoch_minute(at(3_660)), 61);
    }
}
