// Shared application log client for algosolltd apps.
//
// One event is one JSON line on stdout and, when url and token are configured,
// part of a batch sent to POST {url}/api/log/ingest.
//
// Zero dependencies: Node 20+ global fetch, timers and process.stdout.
// Emitting only pushes onto a bounded array; it never awaits and never throws.

import process from 'node:process';

export const CAPACITY = 5000;
export const BATCH_THRESHOLD = 100;
export const MAX_BATCH = 200;
export const FLUSH_MS = 1000;
export const HEARTBEAT_MS = 60000;
export const MAX_MSG_CHARS = 2000;
export const MAX_DATA_BYTES = 4096;
export const MAX_TRACE_CHARS = 120;
export const MAX_ACTOR_CHARS = 80;
export const MAX_DEDUPE_CHARS = 200;

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, fatal: 50 };
const LEVEL_ALIASES = { warning: 'warn', critical: 'fatal' };

function parseLevel(value, fallback = 'info') {
  if (typeof value !== 'string') return fallback;
  const key = value.trim().toLowerCase();
  const level = LEVEL_ALIASES[key] ?? key;
  return Object.hasOwn(LEVELS, level) ? level : fallback;
}

function truncateChars(value, max) {
  const text = String(value ?? '');
  if (text.length <= max) return text;
  // Spread iterates code points, not UTF-16 halves.
  return [...text].slice(0, max).join('');
}

export const REDACTED = '[redacted]';
const MAX_REDACT_DEPTH = 8;
const MAX_REDACT_ARRAY = 100;

const TOKEN_CHARS = '[A-Za-z0-9._~+/=-]';
const VALUE_CHARS = '[^\\s,;&"\'<>()\\[\\]{}]+';
const BEARER_RE = new RegExp(`\\b(Bearer[ \\t]+)${TOKEN_CHARS}+`, 'gi');
const SECRET_KEY_RE = new RegExp(
  `(^|[^A-Za-z0-9_])` +
    `(password|passwd|pwd|secret|token|api_key|apikey|authorization)` +
    `([ \\t]*[=:][ \\t]*)` +
    `(?!bearer(?![A-Za-z0-9._~+/=-]))` +
    VALUE_CHARS,
  'gi',
);
const URL_CRED_RE = /([A-Za-z][A-Za-z0-9+.-]*:\/\/)([^/\s?#@]*):([^@\s/?#]+)@/g;

// Best-effort secret redaction. Idempotent: the replacement `[redacted]` does
// not match any of the value character classes above.
export function redact(value) {
  let text = String(value ?? '');
  text = text.replace(BEARER_RE, `$1${REDACTED}`);
  text = text.replace(
    SECRET_KEY_RE,
    (_match, before, key, separator) => before + key + separator + REDACTED,
  );
  text = text.replace(URL_CRED_RE, `$1${REDACTED}@`);
  return text;
}

function redactData(value, depth = 0) {
  if (typeof value === 'string') return redact(value);
  if (value === null || typeof value !== 'object') return value;
  if (depth >= MAX_REDACT_DEPTH) return value;
  if (Array.isArray(value)) {
    return value.map((item, index) =>
      index < MAX_REDACT_ARRAY ? redactData(item, depth + 1) : item,
    );
  }
  const out = {};
  for (const [key, item] of Object.entries(value)) {
    out[key] = redactData(item, depth + 1);
  }
  return out;
}

function normalizeData(data) {
  if (data === undefined || data === null) return undefined;
  let json;
  try {
    json = JSON.stringify(data);
  } catch {
    return { truncated: true };
  }
  if (json === undefined) return undefined;
  if (Buffer.byteLength(json, 'utf8') > MAX_DATA_BYTES) return { truncated: true };
  let parsed;
  try {
    parsed = JSON.parse(json);
  } catch {
    return { truncated: true };
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { value: parsed };
  }
  return parsed;
}

function isoNow(now = new Date()) {
  return now.toISOString();
}

function epochMinute(now = new Date()) {
  return Math.floor(now.getTime() / 60000);
}

export function createLogger(options = {}) {
  const env = process.env;
  const url = trimSlash(options.url ?? env.ALGOSTATUS_LOG_URL);
  const token = options.token ?? env.ALGOSTATUS_LOG_TOKEN;
  const project = options.project ?? options.projectId ?? env.ALGOSTATUS_PROJECT;
  const platform = options.platform ?? env.ALGOSTATUS_PLATFORM ?? 'unknown';
  const source = options.source ?? env.ALGOSTATUS_SOURCE ?? 'app';
  const level = parseLevel(options.level ?? env.ALGOSTATUS_LOG_LEVEL, 'info');
  const disabled = (options.disabled ?? env.ALGOSTATUS_LOG_DISABLED) === '1';
  const canSend = !disabled && Boolean(url) && Boolean(token);
  const threshold = LEVELS[level];
  const heartbeatMs = options.heartbeatMs ?? HEARTBEAT_MS;
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const write =
    typeof options.write === 'function'
      ? options.write
      : (line) => process.stdout.write(line);

  const state = {
    q: [],
    head: 0,
    dropped: 0,
    sent: 0,
    rejected: 0,
    closed: false,
    draining: false,
    retry: 0,
    retryTimer: null,
    flushTimer: null,
    heartbeatTimer: null,
    inflight: null,
  };

  function size() {
    return state.q.length - state.head;
  }

  function compact() {
    if (state.head > 0 && (state.head > 1024 || state.head * 2 > state.q.length)) {
      state.q = state.q.slice(state.head);
      state.head = 0;
    }
  }

  function buildEvent(levelName, event, msg, data, extra = {}) {
    const at = extra.now instanceof Date ? extra.now : new Date();
    let message = truncateChars(redact(msg), MAX_MSG_CHARS);
    let code = event === undefined || event === null ? 'log' : String(event);
    if (!code.trim()) code = 'log';
    if (!message.trim()) message = code;
    const ev = {
      ts: isoNow(at),
      level: levelName,
      event: code,
      msg: message,
    };
    if (project) ev.project = String(project);
    ev.platform = String(platform);
    ev.source = String(source);
    const normalized = normalizeData(redactData(data));
    if (normalized !== undefined) ev.data = normalized;
    if (extra.traceId) ev.trace_id = truncateChars(extra.traceId, MAX_TRACE_CHARS);
    if (extra.actor) ev.actor = truncateChars(extra.actor, MAX_ACTOR_CHARS);
    if (extra.dedupeKey) ev.dedupe_key = truncateChars(extra.dedupeKey, MAX_DEDUPE_CHARS);
    return ev;
  }

  function writeLine(ev) {
    try {
      write(`${JSON.stringify(ev)}\n`);
    } catch {
      // A closed stdout must never break the caller.
    }
  }

  function scheduleFlush() {
    if (state.closed || state.flushTimer || state.draining) return;
    state.flushTimer = setTimeout(() => {
      state.flushTimer = null;
      void drain();
    }, FLUSH_MS);
    state.flushTimer.unref?.();
  }

  function enqueue(ev) {
    if (size() >= CAPACITY) {
      state.head += 1;
      state.dropped += 1;
      compact();
    }
    state.q.push(ev);
    if (size() >= BATCH_THRESHOLD) {
      setImmediate(() => void drain());
    } else {
      scheduleFlush();
    }
  }

  function record(ev) {
    writeLine(ev);
    if (canSend) enqueue(ev);
  }

  function emit(levelName, event, msg, data, extra) {
    try {
      if (disabled || LEVELS[levelName] < threshold) return;
      record(buildEvent(levelName, event, msg, data, extra));
    } catch {
      // Never throw from the logging path.
    }
  }

  async function post(events) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10000);
    timer.unref?.();
    try {
      const response = await fetchImpl(`${url}/api/log/ingest`, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${token}`,
          'content-type': 'application/json',
        },
        body: JSON.stringify({ events }),
        signal: controller.signal,
      });
      if (response.status >= 400 && response.status < 500) {
        return { kind: 'reject', status: response.status };
      }
      if (!response.ok) return { kind: 'retry' };
      return { kind: 'ok' };
    } catch {
      return { kind: 'retry' };
    } finally {
      clearTimeout(timer);
    }
  }

  // Removes the batch from the queue only after a definitive answer: a 5xx or a
  // network failure leaves it in place for the retry.
  async function drainOnce() {
    if (state.closed || state.draining || size() === 0) return;
    state.draining = true;
    try {
      while (size() > 0) {
        const count = Math.min(size(), MAX_BATCH);
        const batch = state.q.slice(state.head, state.head + count);
        const result = await post(batch);
        if (result.kind === 'retry') {
          state.retry = Math.min(Math.max(state.retry * 2, 1000), 60000);
          scheduleRetry();
          return;
        }
        state.head += count;
        compact();
        state.retry = 0;
        if (result.kind === 'reject') {
          state.rejected += 1;
          writeLine(
            buildEvent(
              'error',
              'log.rejected',
              'batch rejected by algostatus',
              { status: result.status, events: batch.length },
              {},
            ),
          );
        } else {
          state.sent += batch.length;
        }
      }
    } finally {
      state.draining = false;
    }
  }

  function scheduleRetry() {
    if (state.closed || state.retryTimer) return;
    state.retryTimer = setTimeout(() => {
      state.retryTimer = null;
      void drain();
    }, state.retry);
    state.retryTimer.unref?.();
  }

  function drain() {
    if (state.inflight) return state.inflight;
    state.inflight = drainOnce().finally(() => {
      state.inflight = null;
      if (!state.closed && size() > 0 && !state.retryTimer) scheduleFlush();
    });
    return state.inflight;
  }

  function heartbeat() {
    try {
      if (disabled) return;
      const now = new Date();
      const data = {
        heartbeat_s: Math.round(heartbeatMs / 1000),
        dropped: state.dropped,
        queued: size(),
      };
      const extra = { now };
      if (project) {
        extra.dedupeKey = `hb:${project}:${platform}:${source}:${epochMinute(now)}`;
      }
      record(buildEvent('info', 'heartbeat', 'heartbeat', data, extra));
    } catch {
      // ignore
    }
  }

  const logger = {
    level,
    debug: (event, msg, data, extra) => emit('debug', event, msg, data, extra),
    info: (event, msg, data, extra) => emit('info', event, msg, data, extra),
    warn: (event, msg, data, extra) => emit('warn', event, msg, data, extra),
    error: (event, msg, data, extra) => emit('error', event, msg, data, extra),
    fatal: (event, msg, data, extra) => emit('fatal', event, msg, data, extra),
    heartbeat,
    stats: () => ({
      queued: size(),
      dropped: state.dropped,
      sent: state.sent,
      rejected: state.rejected,
      capacity: CAPACITY,
    }),
    flush: () => drain(),
    async shutdown() {
      if (state.closed) {
        await state.inflight;
        return;
      }
      state.closed = true;
      for (const timer of [state.flushTimer, state.retryTimer, state.heartbeatTimer]) {
        if (timer) clearTimeout(timer);
      }
      state.flushTimer = null;
      state.retryTimer = null;
      state.heartbeatTimer = null;
      await state.inflight;
      // Best effort: send what is still queued, once. A failing server must not
      // hold the process open.
      let guard = 0;
      while (size() > 0 && guard < 100) {
        guard += 1;
        const count = Math.min(size(), MAX_BATCH);
        const batch = state.q.slice(state.head, state.head + count);
        const result = await post(batch);
        if (result.kind === 'retry') break;
        state.head += count;
        compact();
        if (result.kind === 'reject') {
          state.rejected += 1;
          writeLine(
            buildEvent('error', 'log.rejected', 'batch rejected by algostatus', {
              status: result.status,
              events: batch.length,
            }),
          );
        } else {
          state.sent += batch.length;
        }
      }
    },
  };

  if (canSend) {
    state.heartbeatTimer = setInterval(() => logger.heartbeat(), heartbeatMs);
    state.heartbeatTimer.unref?.();
  }

  return logger;
}

function trimSlash(value) {
  if (typeof value !== 'string') return value;
  return value.trim().replace(/\/+$/, '');
}

export default createLogger;
