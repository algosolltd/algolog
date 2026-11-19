import io
import json
import sys
import threading
import time
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import algolog  # noqa: E402


class Capture:
    def __init__(self, status=202):
        self.requests = []
        self.status = status
        self.received = threading.Event()

    def wait(self, timeout=5.0):
        if not self.received.wait(timeout):
            raise AssertionError("no request reached the local server")
        return self.requests[-1]


def make_handler(capture):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            length = int(self.headers.get("content-length", 0))
            raw = self.rfile.read(length)
            self.capture.requests.append(
                {
                    "method": self.command,
                    "path": self.path,
                    "authorization": self.headers.get("authorization"),
                    "content_type": self.headers.get("content-type"),
                    "body": json.loads(raw.decode("utf-8")),
                }
            )
            self.capture.received.set()
            payload = b'{"ok":true,"accepted":1,"rejected":0,"stored":1,"mode":"memory"}'
            self.send_response(self.capture.status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):  # silence the test server
            pass

    Handler.capture = capture
    return Handler


@contextmanager
def local_server(status=202):
    capture = Capture(status)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(capture))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:%d" % server.server_address[1], capture
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class StdoutCapture:
    def __enter__(self):
        self._original = sys.stdout
        self._buffer = io.StringIO()
        sys.stdout = self._buffer
        return self

    def __exit__(self, *exc):
        sys.stdout = self._original
        return False

    def events(self):
        return [
            json.loads(line)
            for line in self._buffer.getvalue().split("\n")
            if line.strip()
        ]


class SendTests(unittest.TestCase):
    def test_sends_one_batch_with_bearer_auth_and_shape(self):
        with local_server() as (url, capture), StdoutCapture() as out:
            logger = algolog.Logger(
                project="algolog",
                platform="laptop",
                source="test",
                url=url,
                token="tok-123",
            )
            try:
                logger.info("test.event", "hello", {"a": 1})
                request = capture.wait()
            finally:
                logger.shutdown()

            self.assertEqual(request["method"], "POST")
            self.assertEqual(request["path"], "/api/log/ingest")
            self.assertEqual(request["authorization"], "Bearer tok-123")
            self.assertTrue(request["content_type"].startswith("application/json"))

            events = request["body"]["events"]
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(
                list(event),
                ["ts", "level", "event", "msg", "project", "platform", "source", "data"],
            )
            self.assertEqual(event["level"], "info")
            self.assertEqual(event["event"], "test.event")
            self.assertEqual(event["msg"], "hello")
            self.assertEqual(event["project"], "algolog")
            self.assertEqual(event["platform"], "laptop")
            self.assertEqual(event["source"], "test")
            self.assertEqual(event["data"], {"a": 1})
            self.assertTrue(event["ts"].endswith("Z"))

            local = out.events()
            self.assertEqual(local, events)

    def test_4xx_drops_the_batch_with_one_local_rejected_line(self):
        with local_server(status=422) as (url, capture), StdoutCapture() as out:
            logger = algolog.Logger(
                project="algolog", platform="laptop", source="test", url=url, token="tok"
            )
            try:
                logger.error("bad.event", "rejected batch")
                capture.wait()
                time.sleep(1.5)
                self.assertEqual(len(capture.requests), 1)
            finally:
                logger.shutdown()

            rejected = [e for e in out.events() if e["event"] == "log.rejected"]
            self.assertEqual(len(rejected), 1)
            self.assertEqual(rejected[0]["level"], "error")
            self.assertEqual(rejected[0]["data"]["status"], 422)


class NormalizationTests(unittest.TestCase):
    def test_msg_and_data_caps(self):
        with StdoutCapture() as out:
            logger = algolog.Logger(project="algolog", url="", token="")
            logger.info("long.msg", "é" * 2500, {"blob": "x" * 5000})
            logger.info("small.msg", "ok", {"n": 1})
            logger.info("no.data", "ok")
            logger.shutdown()

            first, second, third = out.events()
            self.assertEqual(len(first["msg"]), algolog.MAX_MSG_CHARS)
            self.assertEqual(first["data"], {"truncated": True})
            self.assertEqual(second["data"], {"n": 1})
            self.assertNotIn("data", third)

    def test_non_object_data_is_wrapped(self):
        with StdoutCapture() as out:
            logger = algolog.Logger(project="algolog", url="", token="")
            logger.info("array.data", "ok", [1, 2])
            logger.shutdown()
            self.assertEqual(out.events()[0]["data"], {"value": [1, 2]})

    def test_empty_msg_falls_back_to_event_code(self):
        with StdoutCapture() as out:
            logger = algolog.Logger(project="algolog", url="", token="")
            logger.warn("db.timeout", "")
            logger.shutdown()
            self.assertEqual(out.events()[0]["msg"], "db.timeout")

    def test_level_filter(self):
        with StdoutCapture() as out:
            logger = algolog.Logger(project="algolog", url="", token="", level="warn")
            logger.debug("d", "drop")
            logger.info("i", "drop")
            logger.warning("w", "keep")
            logger.shutdown()
            events = out.events()
            self.assertEqual([e["event"] for e in events], ["w"])

    def test_heartbeat_shape_and_dedupe_key(self):
        with StdoutCapture() as out:
            logger = algolog.Logger(
                project="algolog", platform="laptop", source="test", url="", token=""
            )
            logger.heartbeat()
            logger.shutdown()
            (event,) = out.events()
            self.assertEqual(event["event"], "heartbeat")
            self.assertEqual(event["level"], "info")
            self.assertEqual(event["msg"], "heartbeat")
            self.assertEqual(event["data"]["heartbeat_s"], 60)
            self.assertEqual(event["data"]["dropped"], 0)
            self.assertIn("queued", event["data"])
            self.assertRegex(event["dedupe_key"], r"^hb:algolog:laptop:test:\d+$")


class QueueTests(unittest.TestCase):
    def test_bounded_queue_drops_the_oldest(self):
        queue = algolog._Queue(algolog.CAPACITY)
        for i in range(algolog.CAPACITY + 100):
            queue.push({"event": "e%d" % i})
        self.assertEqual(queue.queued(), algolog.CAPACITY)
        self.assertEqual(queue.dropped, 100)
        batch = queue.wait_batch(0, algolog.CAPACITY, algolog.CAPACITY)
        self.assertEqual(batch[0]["event"], "e100")
        self.assertEqual(batch[-1]["event"], "e%d" % (algolog.CAPACITY + 99))


if __name__ == "__main__":
    unittest.main()
