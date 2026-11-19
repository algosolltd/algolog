import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';

import { createLogger, CAPACITY, MAX_MSG_CHARS } from '../index.mjs';

function capture() {
  const lines = [];
  return {
    write: (line) => lines.push(line),
    events() {
      return lines
        .join('')
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line));
    },
  };
}

async function withServer(handler, run) {
  const server = http.createServer(handler);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  try {
    return await run(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

test('sends one batch with bearer auth and the documented shape', async () => {
  const seen = [];
  await withServer(
    async (req, res) => {
      const body = JSON.parse(await readBody(req));
      seen.push({
        method: req.method,
        url: req.url,
        authorization: req.headers.authorization,
        contentType: req.headers['content-type'],
        body,
      });
      res.writeHead(202, { 'content-type': 'application/json' });
      res.end('{"ok":true,"accepted":1,"rejected":0,"stored":1,"mode":"memory"}');
    },
    async (url) => {
      const out = capture();
      const logger = createLogger({
        url,
        token: 'tok-123',
        project: 'algolog',
        platform: 'laptop',
        source: 'test',
        write: out.write,
      });
      try {
        logger.info('test.event', 'hello', { a: 1 });
        await logger.flush();
        const local = out.events();
        assert.equal(local.length, 1);
        assert.deepEqual(local[0].data, { a: 1 });
        assert.equal(local[0].level, 'info');
        assert.equal(local[0].event, 'test.event');
        assert.ok(local[0].ts.endsWith('Z'));
      } finally {
        await logger.shutdown();
      }
    },
  );

  assert.equal(seen.length, 1);
  assert.equal(seen[0].method, 'POST');
  assert.equal(seen[0].url, '/api/log/ingest');
  assert.equal(seen[0].authorization, 'Bearer tok-123');
  assert.match(seen[0].contentType, /^application\/json/);
  assert.equal(seen[0].body.events.length, 1);
  const event = seen[0].body.events[0];
  assert.deepEqual(Object.keys(event), [
    'ts',
    'level',
    'event',
    'msg',
    'project',
    'platform',
    'source',
    'data',
  ]);
  assert.equal(event.project, 'algolog');
  assert.equal(event.platform, 'laptop');
  assert.equal(event.source, 'test');
  assert.equal(event.msg, 'hello');
});

test('truncates msg to 2000 chars and oversized data to the marker', async () => {
  const out = capture();
  const logger = createLogger({
    url: 'http://127.0.0.1:1',
    token: 'tok',
    project: 'algolog',
    write: out.write,
    fetch: async () => ({ status: 202, ok: true }),
  });
  try {
    logger.info('long.msg', 'é'.repeat(2500), { blob: 'x'.repeat(5000) });
    logger.info('small.msg', 'ok', { n: 1 });
    logger.info('no.data', 'ok');
    logger.info('array.data', 'ok', [1, 2]);
    const [first, second, third, fourth] = out.events();
    assert.equal([...first.msg].length, MAX_MSG_CHARS);
    assert.deepEqual(first.data, { truncated: true });
    assert.deepEqual(second.data, { n: 1 });
    assert.equal(Object.hasOwn(third, 'data'), false);
    assert.equal(third.msg, 'ok');
    assert.deepEqual(fourth.data, { value: [1, 2] });
  } finally {
    await logger.shutdown();
  }
});

test('bounded queue drops the oldest and keeps the newest 5000', async () => {
  const sent = [];
  const out = capture();
  const logger = createLogger({
    url: 'http://127.0.0.1:1',
    token: 'tok',
    project: 'algolog',
    write: out.write,
    fetch: async (_url, init) => {
      const body = JSON.parse(init.body);
      sent.push(...body.events);
      return { status: 202, ok: true };
    },
  });
  try {
    for (let i = 0; i < CAPACITY + 100; i += 1) {
      logger.info(`e${i}`, 'm');
    }
    const stats = logger.stats();
    assert.equal(stats.queued, CAPACITY);
    assert.equal(stats.dropped, 100);
  } finally {
    await logger.shutdown();
  }
  assert.equal(sent.length, CAPACITY);
  assert.equal(sent[0].event, 'e100');
  assert.equal(sent[sent.length - 1].event, `e${CAPACITY + 99}`);
});

test('a 4xx batch is dropped with one local log.rejected line and no retry', async () => {
  let requests = 0;
  await withServer(
    async (req, res) => {
      requests += 1;
      await readBody(req);
      res.writeHead(422, { 'content-type': 'application/json' });
      res.end('{"ok":false}');
    },
    async (url) => {
      const out = capture();
      const logger = createLogger({
        url,
        token: 'tok',
        project: 'algolog',
        source: 'test',
        write: out.write,
      });
      try {
        logger.error('bad.event', 'rejected batch');
        await logger.flush();
        await new Promise((resolve) => setTimeout(resolve, 1500));
        const rejected = out.events().filter((e) => e.event === 'log.rejected');
        assert.equal(rejected.length, 1);
        assert.equal(rejected[0].level, 'error');
        assert.equal(rejected[0].data.status, 422);
      } finally {
        await logger.shutdown();
      }
    },
  );
  assert.equal(requests, 1);
});

test('level filter drops events below the threshold', async () => {
  const out = capture();
  const logger = createLogger({
    url: 'http://127.0.0.1:1',
    token: 'tok',
    level: 'warn',
    write: out.write,
    fetch: async () => ({ status: 202, ok: true }),
  });
  try {
    logger.debug('d', 'drop');
    logger.info('i', 'drop');
    logger.warn('w', 'keep');
    const events = out.events();
    assert.equal(events.length, 1);
    assert.equal(events[0].event, 'w');
  } finally {
    await logger.shutdown();
  }
});

test('heartbeat carries interval, counters and a per-minute dedupe key', async () => {
  const out = capture();
  const logger = createLogger({
    url: 'http://127.0.0.1:1',
    token: 'tok',
    project: 'algolog',
    platform: 'laptop',
    source: 'test',
    write: out.write,
    fetch: async () => ({ status: 202, ok: true }),
  });
  try {
    logger.heartbeat();
    const [hb] = out.events();
    assert.equal(hb.event, 'heartbeat');
    assert.equal(hb.level, 'info');
    assert.equal(hb.msg, 'heartbeat');
    assert.equal(hb.data.heartbeat_s, 60);
    assert.equal(hb.data.queued, 0);
    assert.match(hb.dedupe_key, /^hb:algolog:laptop:test:\d+$/);
  } finally {
    await logger.shutdown();
  }
});

test('missing url or token means no transport, stdout still works', async () => {
  const out = capture();
  const logger = createLogger({ token: '', project: 'algolog', write: out.write });
  try {
    logger.info('no.transport', 'still printed', { n: 1 });
    const events = out.events();
    assert.equal(events.length, 1);
    assert.equal(events[0].event, 'no.transport');
    assert.equal(logger.stats().sent, 0);
  } finally {
    await logger.shutdown();
  }
});
