const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const runner = require('../stream-quality-runner.cjs');
const SQ = require('../stream-quality.js');
test('real 20s HTTP/SSE and exact 8MiB download through isolated local HTTP proxy', { timeout: 29000 }, async t => {
  const { createServer } = await import('../server.mjs');
  const origin = createServer(); await new Promise(r => origin.listen(0, '127.0.0.1', r));
  t.after(() => { origin.closeAllConnections(); origin.close(); });
  let proxyRequests = 0;
  const proxy = http.createServer((req, res) => {
    proxyRequests++; const upstream = http.get(req.url, r => { res.writeHead(r.statusCode, r.headers); r.pipe(res); });
    upstream.on('error', () => res.destroy()); res.on('close', () => upstream.destroy());
  });
  await new Promise(r => proxy.listen(0, '127.0.0.1', r)); t.after(() => { proxy.closeAllConnections(); proxy.close(); });
  const start = Date.now(); const result = await runner.probe(`http://127.0.0.1:${origin.address().port}`, { port: proxy.address().port });
  assert.equal(result.ok, true); assert.equal(result.stream.receivedSamples, 401); assert.equal(result.download.bytes, SQ.PROFILE.downloadBytes);
  assert.equal(proxyRequests, 3); assert.ok(Date.now() - start >= 19900); assert.ok(result.stream.sourceSlipMs <= 250); assert.equal(result.stream.flowPass, true);
});
test('abort destroys an actual streaming socket within 1s', { timeout: 3000 }, async t => {
  let closed = false;
  const server = http.createServer((_req, res) => { res.writeHead(200); res.write('partial'); res.once('close', () => { closed = true; }); });
  await new Promise(r => server.listen(0, '127.0.0.1', r)); t.after(() => { server.closeAllConnections(); server.close(); });
  const controller = new AbortController(), started = Date.now(); setTimeout(() => controller.abort(), 50);
  await assert.rejects(runner.request(`http://127.0.0.1:${server.address().port}`, '/', { signal: controller.signal, maxBytes: 100 }), e => e.failureScope === 'cancelled');
  await new Promise(r => setTimeout(r, 50)); assert.ok(Date.now() - started < 1000); assert.equal(closed, true);
});
test('source rejects configurable sizes and non-GET requests, advertises CORS and identity encoding', async () => {
  const { handle } = await import('../worker.mjs');
  assert.equal(handle(new Request('https://x/api/download?bytes=99999999')).status, 400);
  assert.equal(handle(new Request('https://x/api/stream', { method: 'POST' })).status, 405);
  const r = handle(new Request('https://x/api/download')); assert.equal(r.headers.get('access-control-allow-origin'), '*'); assert.equal(r.headers.get('content-encoding'), 'identity'); await r.body.cancel();
});
