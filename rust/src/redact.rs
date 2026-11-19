//! Best-effort secret redaction for `msg` and the string leaves of `data`.
//!
//! The client never wants a token, a password or an API key to reach stdout or
//! algostatus. Redaction is deliberately local and best effort: it recognizes
//! the common shapes (`Bearer …`, `key=value`, credentials in a URL) but it is
//! not a guarantee. Callers must still avoid logging secrets on purpose.
//!
//! No `regex` dependency: the three passes are simple `&str`/`char` scanners.

use serde_json::Value;

pub const REDACTED: &str = "[redacted]";

const MAX_DEPTH: usize = 8;
const MAX_ARRAY: usize = 100;
const KEYS: [&str; 8] = [
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
];

/// Redact the common secret shapes in `text`. Idempotent on its own output.
pub fn redact(text: &str) -> String {
    let bearer = redact_bearer(text);
    let keys = redact_keys(&bearer);
    redact_url_credentials(&keys)
}

/// Recurse into JSON values (depth 8, arrays 100) redacting every string leaf.
pub fn redact_value(value: Value, depth: usize) -> Value {
    match value {
        Value::String(text) => Value::String(redact(&text)),
        Value::Array(items) if depth < MAX_DEPTH => Value::Array(
            items
                .into_iter()
                .enumerate()
                .map(|(index, item)| {
                    if index < MAX_ARRAY {
                        redact_value(item, depth + 1)
                    } else {
                        item
                    }
                })
                .collect(),
        ),
        Value::Object(map) if depth < MAX_DEPTH => Value::Object(
            map.into_iter()
                .map(|(key, item)| (key, redact_value(item, depth + 1)))
                .collect(),
        ),
        other => other,
    }
}

fn is_token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '~' | '+' | '/' | '=' | '-')
}

fn is_scheme_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-')
}

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn is_value_char(c: char) -> bool {
    !c.is_whitespace()
        && !matches!(
            c,
            ',' | ';' | '&' | '"' | '\'' | '<' | '>' | '(' | ')' | '[' | ']' | '{' | '}'
        )
}

fn matches_ci(chars: &[char], at: usize, needle: &str) -> bool {
    let mut index = at;
    for expected in needle.chars() {
        if index >= chars.len() || !chars[index].eq_ignore_ascii_case(&expected) {
            return false;
        }
        index += 1;
    }
    true
}

fn redact_bearer(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < chars.len() {
        let at_boundary = index == 0 || !is_word_char(chars[index - 1]);
        if at_boundary && matches_ci(&chars, index, "bearer") {
            let after = index + "bearer".len();
            if after < chars.len() && (chars[after] == ' ' || chars[after] == '\t') {
                let mut start = after;
                while start < chars.len() && (chars[start] == ' ' || chars[start] == '\t') {
                    start += 1;
                }
                let mut end = start;
                while end < chars.len() && is_token_char(chars[end]) {
                    end += 1;
                }
                if end > start {
                    out.extend(chars[index..start].iter());
                    out.push_str(REDACTED);
                    index = end;
                    continue;
                }
                out.extend(chars[index..after].iter());
                index = after;
                continue;
            }
        }
        out.push(chars[index]);
        index += 1;
    }
    out
}

fn redact_keys(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < chars.len() {
        let at_boundary = index == 0 || !is_word_char(chars[index - 1]);
        if at_boundary {
            let found = KEYS
                .iter()
                .copied()
                .find(|key| matches_ci(&chars, index, key));
            if let Some(key) = found {
                let after_key = index + key.len();
                let mut separator = after_key;
                while separator < chars.len()
                    && (chars[separator] == ' ' || chars[separator] == '\t')
                {
                    separator += 1;
                }
                if separator < chars.len() && (chars[separator] == '=' || chars[separator] == ':') {
                    let mut start = separator + 1;
                    while start < chars.len() && (chars[start] == ' ' || chars[start] == '\t') {
                        start += 1;
                    }
                    let mut end = start;
                    while end < chars.len() && is_value_char(chars[end]) {
                        end += 1;
                    }
                    // `Authorization: Bearer x` is handled by the Bearer pass;
                    // leave the keyword alone so the output stays readable.
                    let starts_bearer = matches_ci(&chars, start, "bearer")
                        && (start + "bearer".len() >= chars.len()
                            || !is_token_char(chars[start + "bearer".len()]));
                    if end > start && !starts_bearer {
                        out.extend(chars[index..start].iter());
                        out.push_str(REDACTED);
                        index = end;
                        continue;
                    }
                }
            }
        }
        out.push(chars[index]);
        index += 1;
    }
    out
}

fn redact_url_credentials(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < chars.len() {
        let is_scheme_end = index + 2 < chars.len()
            && chars[index] == ':'
            && chars[index + 1] == '/'
            && chars[index + 2] == '/';
        if is_scheme_end {
            let mut scheme_start = index;
            while scheme_start > 0 && is_scheme_char(chars[scheme_start - 1]) {
                scheme_start -= 1;
            }
            let has_scheme = scheme_start < index && chars[scheme_start].is_ascii_alphabetic();
            if has_scheme {
                let mut scan = index + 3;
                let mut at = None;
                while scan < chars.len() {
                    let c = chars[scan];
                    if c == '@' {
                        at = Some(scan);
                        break;
                    }
                    if c.is_whitespace() || matches!(c, '/' | '?' | '#') {
                        break;
                    }
                    scan += 1;
                }
                if let Some(at) = at {
                    let userinfo = &chars[index + 3..at];
                    if userinfo.contains(&':') {
                        out.extend(chars[index..index + 3].iter());
                        out.push_str(REDACTED);
                        out.push('@');
                        index = at + 1;
                        continue;
                    }
                }
            }
        }
        out.push(chars[index]);
        index += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Level;
    use crate::event::Event;
    use serde_json::json;

    #[test]
    fn redacts_common_secret_patterns() {
        assert_eq!(
            redact("Authorization: Bearer abcdef"),
            "Authorization: Bearer [redacted]"
        );
        assert_eq!(redact("password=hunter2"), "password=[redacted]");
        assert_eq!(redact("api_key=xyz"), "api_key=[redacted]");
        assert_eq!(
            redact("postgres://user:pass@host:5432/db"),
            "postgres://[redacted]@host:5432/db"
        );
        assert_eq!(redact("nothing to see here"), "nothing to see here");
    }

    #[test]
    fn redaction_is_idempotent() {
        for text in [
            "Authorization: Bearer abcdef",
            "password=hunter2 api_key=xyz",
            "postgres://user:pass@host:5432/db",
            "redis://:s3cret@cache:6379/0",
        ] {
            let once = redact(text);
            assert_eq!(redact(&once), once);
        }
    }

    #[test]
    fn nested_data_strings_are_redacted() {
        let event = Event::build(
            Level::Info,
            "db.connect",
            "postgres://user:pass@host:5432/db",
            json!({
                "dsn": "postgres://u:p@h/db",
                "nested": { "headers": ["Bearer abcdef", 1] },
            }),
            None,
            "p",
            "s",
        );
        assert_eq!(event.msg, "postgres://[redacted]@host:5432/db");
        let data = event.data.expect("data");
        assert_eq!(data["dsn"], "postgres://[redacted]@h/db");
        assert_eq!(data["nested"]["headers"][0], "Bearer [redacted]");
        assert_eq!(data["nested"]["headers"][1], 1);
    }
}
