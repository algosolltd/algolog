use std::env;
use std::fmt;

use serde::Serialize;

/// Severity of one event.
///
/// Ordering is the log threshold order: `Debug < Info < Warn < Error < Fatal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
            Level::Fatal => "fatal",
        }
    }

    /// Parse the wire spelling. Aliases from the server contract are accepted.
    pub fn parse(value: &str) -> Option<Level> {
        match value.trim().to_ascii_lowercase().as_str() {
            "debug" => Some(Level::Debug),
            "info" => Some(Level::Info),
            "warn" | "warning" => Some(Level::Warn),
            "error" => Some(Level::Error),
            "fatal" | "critical" => Some(Level::Fatal),
            _ => None,
        }
    }

    /// True when an event at `self` is at or above the configured threshold.
    pub fn allows(self, threshold: Level) -> bool {
        self >= threshold
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Client configuration, read once from the process environment.
#[derive(Debug, Clone)]
pub struct Config {
    /// `ALGOSTATUS_LOG_URL`, base URL of algostatus. No trailing slash.
    pub url: Option<String>,
    /// `ALGOSTATUS_LOG_TOKEN`, write token.
    pub token: Option<String>,
    /// `ALGOSTATUS_PROJECT`; optional when the token is bound to a project.
    pub project: Option<String>,
    /// `ALGOSTATUS_PLATFORM`, default `unknown`.
    pub platform: String,
    /// `ALGOSTATUS_SOURCE`, default `app`.
    pub source: String,
    /// `ALGOSTATUS_LOG_LEVEL`, default `info`.
    pub level: Level,
    /// `ALGOSTATUS_LOG_DISABLED=1` turns the client off completely.
    pub disabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            url: None,
            token: None,
            project: None,
            platform: "unknown".to_string(),
            source: "app".to_string(),
            level: Level::Info,
            disabled: false,
        }
    }
}

impl Config {
    /// Read configuration from the documented `ALGOSTATUS_*` variables.
    pub fn from_env() -> Config {
        Config {
            url: env_string("ALGOSTATUS_LOG_URL").map(|u| u.trim_end_matches('/').to_string()),
            token: env_string("ALGOSTATUS_LOG_TOKEN"),
            project: env_string("ALGOSTATUS_PROJECT"),
            platform: env_string("ALGOSTATUS_PLATFORM").unwrap_or_else(|| "unknown".to_string()),
            source: env_string("ALGOSTATUS_SOURCE").unwrap_or_else(|| "app".to_string()),
            level: env_string("ALGOSTATUS_LOG_LEVEL")
                .and_then(|v| Level::parse(&v))
                .unwrap_or(Level::Info),
            disabled: env_string("ALGOSTATUS_LOG_DISABLED").as_deref() == Some("1"),
        }
    }

    /// The client can send only when it is not disabled and has both URL and token.
    /// A missing project is fine: the token may be bound to one.
    pub fn can_send(&self) -> bool {
        !self.disabled && self.url.is_some() && self.token.is_some()
    }
}

fn env_string(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parse_accepts_contract_aliases() {
        assert_eq!(Level::parse("WARN"), Some(Level::Warn));
        assert_eq!(Level::parse("warning"), Some(Level::Warn));
        assert_eq!(Level::parse("critical"), Some(Level::Fatal));
        assert_eq!(Level::parse("nope"), None);
    }

    #[test]
    fn level_threshold_is_ordered() {
        assert!(Level::Error.allows(Level::Info));
        assert!(!Level::Debug.allows(Level::Info));
        assert!(Level::Fatal.allows(Level::Fatal));
    }

    #[test]
    fn missing_transport_disables_sending() {
        let cfg = Config::default();
        assert!(!cfg.can_send());
        let cfg = Config {
            url: Some("http://127.0.0.1:1".into()),
            token: Some("t".into()),
            ..Config::default()
        };
        assert!(cfg.can_send());
    }
}
