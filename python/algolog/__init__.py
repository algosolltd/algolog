"""Shared application log client for algosolltd apps.

One event is one JSON line on stdout and, when ``ALGOSTATUS_LOG_URL`` and
``ALGOSTATUS_LOG_TOKEN`` are set, part of a batch sent to
``POST {url}/api/log/ingest``.

Standard library only. Emitting appends to a bounded deque under a short lock
and returns: it never waits for the network and never raises.

    import algolog

    logger = algolog.setup(project="algocrm", platform="railway", source="http")
    logger.info("app.start", "started", data={"version": "1.0"})
"""

from __future__ import annotations

import atexit
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

__all__ = ["setup", "Logger", "LEVELS"]

CAPACITY = 5000
BATCH_THRESHOLD = 100
MAX_BATCH = 200
FLUSH_S = 1.0
HEARTBEAT_S = 60.0

MAX_MSG_CHARS = 2000
MAX_DATA_BYTES = 4096
MAX_TRACE_CHARS = 120
MAX_ACTOR_CHARS = 80
MAX_DEDUPE_CHARS = 200

BACKOFF_MIN = 1.0
BACKOFF_MAX = 60.0
REQUEST_TIMEOUT = 10.0
# One flush attempt during shutdown: a dead server must not hold the process open.
SHUTDOWN_ATTEMPTS = 1

LEVELS: Dict[str, int] = {"debug": 10, "info": 20, "warn": 30, "error": 40, "fatal": 50}
_LEVEL_ALIASES = {"warning": "warn", "critical": "fatal"}


def _parse_level(value: Any, fallback: str = "info") -> str:
    if not isinstance(value, str):
        return fallback
    key = value.strip().lower()
    key = _LEVEL_ALIASES.get(key, key)
    return key if key in LEVELS else fallback


def _truncate(value: Any, limit: int) -> str:
    text = "" if value is None else str(value)
    return text if len(text) <= limit else text[:limit]


def _normalize_data(data: Any) -> Optional[Dict[str, Any]]:
    if data is None:
        return None
    if not isinstance(data, dict):
        data = {"value": data}
    try:
        encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError):
        return {"truncated": True}
    if len(encoded) > MAX_DATA_BYTES:
        return {"truncated": True}
    return data


def _iso_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _build_event(
    level: str,
    event: Any,
    msg: Any,
    data: Any,
    project: Optional[str],
    platform: str,
    source: str,
    dedupe_key: Optional[str] = None,
) -> Dict[str, Any]:
    code = "" if event is None else str(event)
    if not code.strip():
        code = "log"
    message = _truncate(msg, MAX_MSG_CHARS)
    if not message.strip():
        message = code
    payload: Dict[str, Any] = {
        "ts": _iso_now(),
        "level": level,
        "event": code,
        "msg": message,
    }
    if project:
        payload["project"] = str(project)
    payload["platform"] = str(platform)
    payload["source"] = str(source)
    normalized = _normalize_data(data)
    if normalized is not None:
        payload["data"] = normalized
    if dedupe_key:
        payload["dedupe_key"] = _truncate(dedupe_key, MAX_DEDUPE_CHARS)
    return payload


def _write_line(event: Dict[str, Any]) -> None:
    try:
        line = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        sys.stdout.write(line + "\n")
        sys.stdout.flush()
    except Exception:
        # A closed stdout must never break the caller.
        pass


class _Queue:
    """Bounded queue: push is O(1), full means the oldest item goes."""

    def __init__(self, capacity: int = CAPACITY) -> None:
        self._items: Deque[Any] = deque()
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._capacity = max(1, capacity)
        self._dropped = 0
        self._closed = False

    def push(self, item: Any) -> None:
        with self._cond:
            if self._closed:
                return
            if len(self._items) >= self._capacity:
                self._items.popleft()
                self._dropped += 1
            self._items.append(item)
            if len(self._items) >= BATCH_THRESHOLD:
                self._cond.notify()

    def wait_batch(
        self, timeout: float, threshold: int = BATCH_THRESHOLD, max_items: int = MAX_BATCH
    ) -> Optional[List[Any]]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if self._closed:
                    if not self._items:
                        return None
                    return self._drain(max_items)
                if self._items and len(self._items) >= threshold:
                    return self._drain(max_items)
                if time.monotonic() >= deadline:
                    # Empty or not, the flush window is over: hand back what is there.
                    return self._drain(max_items)
                remaining = max(0.0, deadline - time.monotonic())
                self._cond.wait(remaining)

    def _drain(self, max_items: int) -> List[Any]:
        count = min(len(self._items), max(1, max_items))
        return [self._items.popleft() for _ in range(count)]

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    @property
    def dropped(self) -> int:
        with self._lock:
            return self._dropped

    def queued(self) -> int:
        with self._lock:
            return len(self._items)


class Logger:
    """One process logger. Keep it for the process lifetime; close with shutdown()."""

    def __init__(
        self,
        project: Optional[str] = None,
        platform: str = "unknown",
        source: str = "app",
        url: Optional[str] = None,
        token: Optional[str] = None,
        level: str = "info",
        disabled: bool = False,
    ) -> None:
        env = os.environ
        self.project = project if project is not None else (env.get("ALGOSTATUS_PROJECT") or None)
        self.platform = platform or env.get("ALGOSTATUS_PLATFORM") or "unknown"
        self.source = source or env.get("ALGOSTATUS_SOURCE") or "app"
        self.level = _parse_level(level if level is not None else env.get("ALGOSTATUS_LOG_LEVEL"))
        self.disabled = disabled
        if url is None:
            url = env.get("ALGOSTATUS_LOG_URL")
        if token is None:
            token = env.get("ALGOSTATUS_LOG_TOKEN")
        self.url = (url or "").strip().rstrip("/")
        self.token = token or ""
        self.can_send = bool(not self.disabled and self.url and self.token)

        self._threshold = LEVELS[self.level]
        self._queue = _Queue(CAPACITY)
        self._sent = 0
        self._rejected = 0
        self._stats_lock = threading.Lock()
        self._shutdown = threading.Event()
        self._joined = False
        self._worker: Optional[threading.Thread] = None

        if self.can_send:
            self._worker = threading.Thread(
                target=self._run, name="algolog", daemon=True
            )
            self._worker.start()

    # -- public API -----------------------------------------------------

    def debug(self, event: str, msg: str, data: Any = None) -> None:
        self._emit("debug", event, msg, data)

    def info(self, event: str, msg: str, data: Any = None) -> None:
        self._emit("info", event, msg, data)

    def warn(self, event: str, msg: str, data: Any = None) -> None:
        self._emit("warn", event, msg, data)

    #: Contract spelling alias.
    warning = warn

    def error(self, event: str, msg: str, data: Any = None) -> None:
        self._emit("error", event, msg, data)

    def fatal(self, event: str, msg: str, data: Any = None) -> None:
        self._emit("fatal", event, msg, data)

    def heartbeat(self) -> None:
        """Liveness event: level info, data.heartbeat_s=60, counters, dedupe key."""
        try:
            if self.disabled:
                return
            data = {
                "heartbeat_s": int(HEARTBEAT_S),
                "dropped": self._queue.dropped,
                "queued": self._queue.queued(),
            }
            minute = int(time.time() // 60)
            dedupe = None
            if self.project:
                dedupe = "hb:%s:%s:%s:%s" % (
                    self.project,
                    self.platform,
                    self.source,
                    minute,
                )
            event = _build_event(
                "info",
                "heartbeat",
                "heartbeat",
                data,
                self.project,
                self.platform,
                self.source,
                dedupe,
            )
            self._record(event)
        except Exception:
            pass

    def flush(self, timeout: float = 5.0) -> bool:
        """Best-effort wait until the queue is empty. Returns True when drained."""
        if not self.can_send:
            return True
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._queue.queued() == 0:
                return True
            time.sleep(0.02)
        return self._queue.queued() == 0

    def stats(self) -> Dict[str, int]:
        with self._stats_lock:
            return {
                "queued": self._queue.queued(),
                "dropped": self._queue.dropped,
                "sent": self._sent,
                "rejected": self._rejected,
                "capacity": CAPACITY,
            }

    def shutdown(self) -> None:
        """Flush what is queued, stop the worker. Safe to call twice."""
        with self._stats_lock:
            if self._joined:
                return
            self._joined = True
        self._queue.close()
        self._shutdown.set()
        worker = self._worker
        if worker is not None and worker.is_alive():
            worker.join(timeout=REQUEST_TIMEOUT + 5.0)

    # -- internals ------------------------------------------------------

    def _emit(self, level: str, event: str, msg: str, data: Any) -> None:
        try:
            if self.disabled or LEVELS[level] < self._threshold:
                return
            payload = _build_event(
                level, event, msg, data, self.project, self.platform, self.source
            )
            self._record(payload)
        except Exception:
            pass

    def _record(self, event: Dict[str, Any]) -> None:
        _write_line(event)
        if self.can_send and not self._shutdown.is_set():
            self._queue.push(event)

    def _run(self) -> None:
        next_heartbeat = time.monotonic() + HEARTBEAT_S
        backoff = BACKOFF_MIN
        while True:
            if self._shutdown.is_set() and self._queue.closed and self._queue.queued() == 0:
                return
            now = time.monotonic()
            if now >= next_heartbeat:
                self.heartbeat()
                next_heartbeat = time.monotonic() + HEARTBEAT_S
            wait = min(FLUSH_S, max(0.0, next_heartbeat - time.monotonic()))
            batch = self._queue.wait_batch(wait, BATCH_THRESHOLD, MAX_BATCH)
            if batch is None:
                return
            if not batch:
                continue
            attempt = 0
            while True:
                kind, status = self._post(batch)
                if kind == "ok":
                    with self._stats_lock:
                        self._sent += len(batch)
                    backoff = BACKOFF_MIN
                    break
                if kind == "reject":
                    with self._stats_lock:
                        self._rejected += 1
                    _write_line(
                        _build_event(
                            "error",
                            "log.rejected",
                            "batch rejected by algostatus",
                            {"status": status, "events": len(batch)},
                            self.project,
                            self.platform,
                            self.source,
                        )
                    )
                    backoff = BACKOFF_MIN
                    break
                attempt += 1
                if self._shutdown.is_set() and attempt >= SHUTDOWN_ATTEMPTS:
                    return
                self._sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)

    def _sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline and not self._shutdown.is_set():
            time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))

    def _post(self, batch: List[Dict[str, Any]]):
        body = json.dumps({"events": batch}, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.url + "/api/log/ingest",
            data=body,
            method="POST",
            headers={
                "Authorization": "Bearer " + self.token,
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return ("ok", response.status)
        except urllib.error.HTTPError as exc:
            status = exc.code
            try:
                exc.close()
            except Exception:
                pass
            if 400 <= status < 500:
                return ("reject", status)
            return ("retry", status)
        except Exception:
            return ("retry", None)


def setup(
    project: Optional[str] = None,
    platform: Optional[str] = None,
    source: Optional[str] = None,
    **options: Any,
) -> Logger:
    """Create a logger from the arguments and the ``ALGOSTATUS_*`` environment.

    URL and token are read from the environment unless passed explicitly; when
    either is missing the logger writes JSON to stdout only.
    """
    logger = Logger(
        project=project,
        platform=platform if platform is not None else (os.environ.get("ALGOSTATUS_PLATFORM") or "unknown"),
        source=source if source is not None else (os.environ.get("ALGOSTATUS_SOURCE") or "app"),
        url=options.pop("url", None),
        token=options.pop("token", None),
        level=options.pop("level", os.environ.get("ALGOSTATUS_LOG_LEVEL", "info")),
        disabled=bool(options.pop("disabled", os.environ.get("ALGOSTATUS_LOG_DISABLED") == "1")),
    )
    atexit.register(logger.shutdown)
    return logger
