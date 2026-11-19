# algolog

Shared application log client for `algosolltd` apps, in Rust, Node and Python.

One event is one JSON line on stdout **and** — when a URL and token are
configured — part of a batch sent to `algostatus`:

```
POST {ALGOSTATUS_LOG_URL}/api/log/ingest
Authorization: Bearer {ALGOSTATUS_LOG_TOKEN}
{"events": [ ... ]}
```

The canonical server contract is
[`algostatus/docs/ops-log.md`](https://github.com/algosolltd/algostatus/blob/main/docs/ops-log.md).
The schema for one event is [`spec/event.schema.json`](spec/event.schema.json).

`ai_*` projects are excluded server-side by credential.

## Never block the caller

The rule that shapes every client here:

> **Emitting never blocks, never throws and never loses the caller's work to the
> network.** An event is formatted, written to stdout, pushed onto a bounded
> in-memory queue and returned. A background thread (or timer) owns HTTP.

When the queue is full (5000 events) the **oldest** event is dropped and
counted; the newest are kept. A slow or dead server therefore costs memory, not
latency. If URL or token is missing the client degrades to stdout only.

## Environment

All three clients read the same variables.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `ALGOSTATUS_LOG_URL` | for sending | — | Base URL of algostatus, no trailing slash needed |
| `ALGOSTATUS_LOG_TOKEN` | for sending | — | Write token, sent as `Authorization: Bearer …` |
| `ALGOSTATUS_PROJECT` | optional | — | Repo name; omit when the token is bound to a project |
| `ALGOSTATUS_PLATFORM` | no | `unknown` | Where it runs: `railway`, `hft_kraken`, `laptop` |
| `ALGOSTATUS_SOURCE` | no | `app` | Internal part: `http`, `worker`, `strategy`, `deploy` |
| `ALGOSTATUS_LOG_LEVEL` | no | `info` | Minimum level: `debug`, `info`, `warn`, `error`, `fatal` |
| `ALGOSTATUS_LOG_DISABLED` | no | — | `1` disables the client completely |

URL or token missing ⇒ no HTTP, stdout only. `ALGOSTATUS_LOG_DISABLED=1` ⇒ no
stdout either.

## Behaviour (identical in all three clients)

1. Every emitted event is one JSON line on stdout, byte-for-byte the shape sent
   to the server, so `docker logs` and Railway keep working. When the token is bound to a platform, the server stores that platform even if the local line says `unknown`.
2. Bounded queue of 5000 events; when full the oldest is dropped and counted.
   Enqueue is O(1).
3. Batch flush when 100 events are queued or after 1 s, whichever comes first.
   One request per batch, at most 200 events per request (server limit).
4. Heartbeat every 60 s: `event=heartbeat`, level `info`, `msg="heartbeat"`,
   `data={heartbeat_s:60,dropped:<n>,queued:<n>}`. The heartbeat is written to
   stdout and sent, but the server stores it as liveness, not as a log row.
   `dedupe_key` is `hb:<project>:<platform>:<source>:<epoch minute>` when the
   project is known, so a duplicate tick inside a minute is idempotent.
5. A 4xx response (including 429) drops that batch: one local `log.rejected`
   line with `data.status`, no retry, no loop.
6. A 5xx or network error retries the batch with exponential backoff from 1 s to
   60 s. The queue stays bounded while retrying.
7. Shutdown flushes what is queued (`shutdown()`, `Guard::shutdown`, Python
   `atexit`). A dead server does not hold the process open: shutdown makes one
   attempt and gives up.
8. `msg` is truncated to 2000 characters (Unicode code points, not bytes).
   `data` larger than 4096 serialized bytes becomes `{"truncated":true}`.
9. Events below `ALGOSTATUS_LOG_LEVEL` are not emitted at all (not stdout, not
   queued). The heartbeat bypasses the level filter: it is liveness, not a log.

`data` must be a JSON object; the Rust `emit` takes a `serde_json::Value`, the
Node/Python helpers accept a plain object. An empty `msg` falls back to the
event code, because the server requires 1..2000 characters.

## Rust

`rust/` — crate `algolog`, edition 2021. Dependencies: `serde`, `serde_json`,
`ureq` v2 (blocking). No async runtime and no tracing subscriber: `algolib` will
wire `tracing` to `algolog::emit` later.

```toml
[dependencies]
algolog = { git = "https://github.com/algosolltd/algolog", package = "algolog" }
```

```rust
use serde_json::json;

fn main() {
    // Keep the guard alive: dropping it flushes the queue and stops the worker.
    let _guard = algolog::init(algolog::Config::from_env());

    algolog::info!("app.start", "started", json!({ "version": "1.0" }));
    algolog::warn!("queue.backlog", "depth above threshold", json!({ "depth": 900 }));
    algolog::error!("http.500", "POST /api/orders failed", json!({ "status": 500 }));

    // Untyped form for values built at runtime.
    algolog::emit(algolog::Level::Info, "tick", "one more", serde_json::Value::Null);
}
```

`algolog::submit(event)` accepts an [`algolog::Event`] with `trace_id`, `actor`
or `dedupe_key` attached.

## Node

`node/` — zero-dependency ESM package, Node 20+ (`fetch` is global).

```js
import { createLogger } from 'algolog';

const log = createLogger({ source: 'http' });

log.info('app.start', 'started', { version: '1.0' });
log.error('http.500', 'POST /api/orders failed', { status: 500 });
log.heartbeat();
await log.flush();

await log.shutdown(); // flush what is queued, stop the timers
```

Options override the environment: `url`, `token`, `project`, `platform`,
`source`, `level`, `disabled`. `stats()` returns `{queued, dropped, sent,
rejected, capacity}`.

## Python

`python/` — package `algolog`, standard library only (`json`, `threading`,
`urllib`; no third-party dependency).

```python
import algolog

logger = algolog.setup(source="http")

logger.info("app.start", "started", data={"version": "1.0"})
logger.error("http.500", "POST /api/orders failed", data={"status": 500})
logger.heartbeat()
logger.flush()

logger.shutdown()  # also registered with atexit by setup()
```

`setup()` reads the environment and returns a `Logger`; arguments (`project`,
`platform`, `source`, `url`, `token`, `level`, `disabled`) win over it.

## Layout

```
spec/event.schema.json   JSON Schema 2020-12 for one event
rust/                    crate algolog
node/                    ESM package
python/                  package algolog + unittest suite
```

## Gates

No CI and no Docker: these commands are the gate.

```bash
cd rust   && cargo check --locked && cargo test --locked && cargo fmt -- --check
cd node   && node --test
python -m unittest discover -s python/tests
```

Host note: this machine has no MSVC `link.exe`, so Rust gates run with the
installed GNU toolchain:

```powershell
$env:RUSTUP_TOOLCHAIN = '1.97.1-x86_64-pc-windows-gnu'
```
