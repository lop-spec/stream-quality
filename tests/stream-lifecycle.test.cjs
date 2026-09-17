const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { Readable } = require('node:stream');
const { getEventListeners } = require('node:events');
const SQ = require('../stream-quality.js'), runner = require('../stream-quality-runner.cjs');
const { workerClock } = require('./fixtures/worker-clock.cjs');
const wait = ms => new Promise(r => setTimeout(r, ms));
async function origin(t, serve) {
  const sockets = new Set(), server = http.createServer(serve);
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  t.after(() => { server.closeAllConnections(); server.close(); });
  return { url: `http://127.0.0.1:${server.address().port}`, async drained() {
    for (let n = 0; n < 50 && sockets.size; n++) await wait(10);
    assert.equal(sockets.size, 0, 'all fixture sockets must be released');
  } };
}
function headers() { return { 'content-type': 'text/event-stream', 'x-stream-quality-profile': SQ.PROFILE.id, 'x-stream-quality-location': 'fixture' }; }
function manifest(req, res) {
  if (req.url !== '/api/manifest') return false;
  res.end(JSON.stringify({ profile: SQ.PROFILE, location: 'fixture' })); return true;
}
test('a real source encoder failure must not classify the node as bad; sockets and listeners are released', { timeout: 5000 }, async t => {
  const w = workerClock({ frameFault: true }), control = new AbortController();
  const source = await origin(t, (req, res) => {
    if (manifest(req, res)) return;
    const response = w.request(), body = Readable.fromWeb(response.body);
    res.writeHead(200, Object.fromEntries(response.headers)); res.flushHeaders();
    body.on('error', () => res.destroy()); res.on('close', () => body.destroy()); body.pipe(res);
    setTimeout(() => w.advanceTo(150), 20);
  });
  await assert.rejects(runner.probe(source.url, { includeDownload: false, reuseConnections: true, signal: control.signal }), error => {
    assert.equal(error.failureScope, 'measurement'); assert.equal(error.requestContext.httpStatus, 200);
    assert.equal(error.requestContext.manifestLocation, 'fixture'); return true;
  });
  await source.drained(); assert.equal(getEventListeners(control.signal, 'abort').length, 0);
  assert.deepEqual(w.inspect(), { stream: 0, download: 0, pendingTimers: 0 });
});
test('graceful EOF without a terminal event stays measurement failure and closes the keepalive session', { timeout: 5000 }, async t => {
  const source = await origin(t, (req, res) => { if (manifest(req, res)) return; res.writeHead(200, headers()); res.end(SQ.frame(0, 0)); });
  const result = await runner.probe(source.url, { includeDownload: false, reuseConnections: true });
  assert.equal(result.failureScope, 'measurement'); assert.match(result.error, /terminal/); await source.drained();
});
test('client cancel releases a real keepalive response and its abort listener', { timeout: 5000 }, async t => {
  const control = new AbortController();
  const source = await origin(t, (_req, res) => { res.writeHead(200, headers()); res.write(SQ.frame(0, 0)); });
  const session = runner.transportSession(source.url);
  await assert.rejects(runner.request(source.url, '/api/stream', { session, signal: control.signal, maxBytes: 200000,
    onData: () => control.abort() }), e => e.failureScope === 'cancelled');
  await source.drained(); assert.equal(getEventListeners(control.signal, 'abort').length, 0); session.destroy();
});
test('pre-header connection failures stay distinct from invalid post-header source measurements', { timeout: 5000 }, async t => {
  const source = await origin(t, (_req, res) => res.destroy());
  await assert.rejects(runner.probe(source.url, { includeDownload: false, reuseConnections: true }), e => {
    assert.notEqual(e.failureScope, 'measurement'); assert.equal(e.requestContext.httpStatus, undefined); return true;
  });
  await source.drained();
});
