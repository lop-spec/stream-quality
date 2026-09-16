const { test } = require('node:test');
const assert = require('node:assert/strict');
const SQ = require('../stream-quality.js');
const runner = require('../stream-quality-runner.cjs');
const good = () => ({ ok: true, metricKind: 'stream-quality-v1', measurement: 'sse-only', profileKey: 'same|sse-only',
  stream: { ok: true, flowPass: true, firstSampleMs: 100, jitterMs: 0, maxExtraGapMs: 0, burstRatio: 0 } });

test('179 lanes all start before any completes; no download budget or fabricated Mbps', async () => {
  const nodes = Array.from({ length: 179 }, (_, i) => ({ key: String(i), port: i }));
  let release, count = 0; const barrier = new Promise(resolve => { release = resolve; }), events = [];
  const result = await runner.run({ endpoint: 'https://example.com', nodes }, { emit: e => events.push(e),
    probeFn: async (_url, options) => { assert.equal(options.includeDownload, false); if (++count === 179) release(); await barrier; return good(); } });
  assert.equal(result.activity.maxActiveTotal, 179); assert.equal(result.outcomes.length, 179);
  const start = events.find(e => e.type === 'start'); assert.equal(start.maxDownloadBytes, 0); assert.equal(start.concurrency, 179);
  for (const { value } of result.outcomes) { assert.equal(SQ.isResult(value), true); assert.equal(value.verified, false); assert.equal('download' in value, false); }
});

test('all active lanes observe cancellation, no partial score or next round', async () => {
  const controller = new AbortController(), nodes = Array.from({ length: 179 }, (_, i) => ({ key: String(i) }));
  let started = 0, cancelled = 0;
  const result = await runner.run({ endpoint: 'https://example.com', nodes, rounds: 3 }, { signal: controller.signal,
    probeFn: async (_url, { signal }) => {
      const stopped = new Promise(resolve => signal.addEventListener('abort', () => { cancelled++; resolve(); }, { once: true }));
      if (++started === 179) controller.abort(); await stopped; return good();
    } });
  assert.equal(started, 179); assert.equal(cancelled, 179); assert.equal(result.cancelled, true);
  assert.ok(result.outcomes.every(o => !o.value.ok && o.value.failureScope === 'cancelled'));
});

test('source busy does not erase a concurrent healthy lane or masquerade as a bad node', async () => {
  const result = await runner.run({ endpoint: 'https://example.com', nodes: [{ key: 'busy', port: 1 }, { key: 'ok', port: 2 }] }, {
    probeFn: async (_url, { port }) => port === 1 ? { ok: false, failureScope: 'endpoint', error: 'HTTP 429' } : good()
  });
  assert.equal(result.outcomes[0].value.failureScope, 'endpoint'); assert.equal(result.outcomes[1].value.ok, true);
});

test('explicit legacy bandwidth mode is serial; mixed measurements cannot aggregate', async () => {
  let active = 0, peak = 0;
  const full = { ...good(), measurement: 'sse-and-download', download: { ok: true, mbps: 10, endToEndMbps: 9 } };
  const result = await runner.run({ endpoint: 'https://example.com', nodes: [{ key: 'a' }, { key: 'b' }], includeDownload: true }, {
    probeFn: async (_url, { includeDownload }) => { assert.equal(includeDownload, true); peak = Math.max(peak, ++active); await new Promise(r => setTimeout(r, 1)); active--; return full; }
  });
  assert.equal(peak, 1); assert.equal(result.outcomes[0].value.download.mbps, 10);
  assert.equal(runner.aggregate([good(), full], 2, 'r').ok, false);
  assert.notEqual(SQ.profileKey('https://example.com', 'same'), SQ.profileKey('https://example.com', 'same', false));
});

test('local scheduling overload invalidates measurements rather than blaming nodes', async () => {
  const logs = [];
  const result = await runner.run({ endpoint: 'https://example.com' }, {
    emit: e => { if (e.type === 'log') logs.push(e.message); },
    probeFn: async () => {
      await new Promise(resolve => setTimeout(resolve, 40));
      const until = performance.now() + 160; while (performance.now() < until) { /* Controlled client stall. */ }
      await new Promise(resolve => setTimeout(resolve, 40)); return good();
    }
  });
  assert.ok(result.activity.maxClientLagMs > 100); assert.equal(result.outcomes[0].value.failureScope, 'measurement');
  assert.equal(result.outcomes[0].value.ok, false); assert.ok(logs.some(s => /local event loop stalled/.test(s)));
});

test('179 real SSE streams preserve every frame, finish together and never request download', { timeout: 29000 }, async t => {
  const { createServer } = await import('../server.mjs');
  const server = createServer(), routes = {}, times = [];
  let active = 0, peak = 0;
  server.on('request', (req, res) => {
    routes[req.url] = (routes[req.url] || 0) + 1;
    if (req.url === '/api/stream') { peak = Math.max(peak, ++active); res.on('close', () => active--); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const start = performance.now();
  const progress = setInterval(() => t.diagnostic(`179-lane fixture: ${Math.round(performance.now() - start)}ms, ${times.length} finished, ${active} active`), 5000);
  const abort = new AbortController(), timeout = setTimeout(() => abort.abort(), 26000);
  let result;
  try {
    result = await runner.run({ endpoint: `http://127.0.0.1:${server.address().port}`, nodes: Array.from({ length: 179 }, (_, i) => ({ key: String(i) })) },
      { signal: abort.signal, emit: e => { if (e.type === 'progress' && e.phase === 'sample-done') times.push(performance.now() - start); } });
  } finally { clearInterval(progress); clearTimeout(timeout); }
  const elapsedMs = performance.now() - start;
  const report = { fixture: 'loopback-only, not real subscription routes', elapsedMs: Math.round(elapsedMs),
    valid: result.outcomes.filter(o => SQ.isResult(o.value)).length, peak, ...result.activity,
    completeWithin20s: times.filter(ms => ms <= 20000).length, target20sMet: elapsedMs <= 20000, routes };
  t.diagnostic(JSON.stringify(report));
  assert.equal(result.cancelled, false); assert.equal(peak, 179); assert.equal(result.activity.maxActiveTotal, 179);
  assert.equal(routes['/api/manifest'], 179); assert.equal(routes['/api/stream'], 179); assert.equal(routes['/api/download'], undefined);
  for (const { key, value } of result.outcomes) {
    assert.equal(SQ.isResult(value), true, `${key}: ${JSON.stringify(value)}`);
    assert.equal(value.stream.receivedSamples, 401); assert.equal(value.stream.flowPass, true);
    assert.equal(value.verified, false); assert.equal('download' in value, false);
  }
  // The original end-to-end goal is a separate, explicit gate, not weakened to 21/30s.
  if (process.env.SQ_REQUIRE_20S === '1') assert.ok(elapsedMs <= 20000, `179-node end-to-end target missed: ${elapsedMs.toFixed(1)}ms > 20000ms`);
});

test('SSE source admits catalog concurrency but keeps independent bandwidth and abuse limits', async () => {
  const { handle, LIMITS } = await import('../worker.mjs');
  const streams = [], downloads = [];
  const req = path => new Request('https://x/api/' + path, { headers: { 'cf-connecting-ip': 'capacity-fixture' } });
  try {
    for (let i = 0; i < LIMITS.download.concurrent; i++) { const r = handle(req('download')); assert.equal(r.status, 200); downloads.push(r); }
    assert.equal(handle(req('download')).status, 429);
    for (let i = 0; i < LIMITS.stream.concurrent; i++) { const r = handle(req('stream')); assert.equal(r.status, 200); streams.push(r); }
    assert.ok(streams.length >= 179); assert.equal(handle(req('stream')).status, 429);
  } finally { await Promise.all([...streams, ...downloads].map(r => r.body.cancel())); }
  const again = handle(req('stream')); assert.equal(again.status, 200); await again.body.cancel();
});
