const { test } = require('node:test');
const assert = require('node:assert/strict');
const SQ = require('../stream-quality.js');
const { workerClock } = require('./fixtures/worker-clock.cjs');

test('virtual-clock normal completion keeps all 401 frames and releases the slot/timer exactly once', async () => {
  const w = workerClock(), response = w.request();
  assert.equal(w.inspect().stream, 1);
  await w.advanceTo(20000);
  const text = await response.text();
  assert.equal((text.match(/event: sample\n/g) || []).length, SQ.PROFILE.samples);
  assert.equal((text.match(/event: end\n/g) || []).length, 1);
  assert.deepEqual(w.inspect(), { stream: 0, download: 0, pendingTimers: 0 });
  await w.advanceTo(60000);
  assert.equal(w.inspect().stream, 0);
});
test('client cancellation wakes the pending producer and frees full source capacity, without double release', async () => {
  const w = workerClock(), held = Array.from({ length: w.limits.stream.concurrent }, (_, i) => w.request('stream', `fixture-${i}`));
  assert.equal(w.request('stream', 'blocked').status, 429);
  await Promise.all(held.map(async r => { await r.body.cancel(); await r.body.cancel(); }));
  assert.deepEqual(w.inspect(), { stream: 0, download: 0, pendingTimers: 0 });
  await w.advanceTo(60000);
  const next = w.request('stream', 'fresh'); assert.equal(next.status, 200); await next.body.cancel();
  assert.equal(w.inspect().stream, 0);
});
test('source encoder exception errors the body, logs its cause and releases the stream slot', async () => {
  const w = workerClock({ frameFault: true }), response = w.request();
  await w.advanceTo(20000);
  await assert.rejects(response.text(), /source encoder failure/);
  assert.deepEqual(w.inspect(), { stream: 0, download: 0, pendingTimers: 0 });
  assert(w.logs.some(x => /source failed/.test(x)));
});
test('normal EOF without a terminal event is measurement failure, not evidence of an active-slot leak', async () => {
  const w = workerClock({ omitTerminal: true }), response = w.request();
  await w.advanceTo(20000);
  const text = await response.text(), parser = SQ.analyzer();
  for (const block of text.trimEnd().split('\n\n')) {
    const data = JSON.parse(block.split('\ndata: ')[1]); parser.push(Buffer.from(block + '\n\n'), data.sentMs);
  }
  assert.equal(parser.result().failureScope, 'measurement');
  assert.match(parser.result().error, /terminal/);
  assert.deepEqual(w.inspect(), { stream: 0, download: 0, pendingTimers: 0 });
});
test('download producer exception also releases its slot and logs the source failure', async () => {
  const w = workerClock({ downloadFault: true }), response = w.request('download');
  await assert.rejects(response.text(), /source buffer failure/);
  assert.deepEqual(w.inspect(), { stream: 0, download: 0, pendingTimers: 0 });
  assert(w.logs.some(x => /source failed/.test(x)));
});
test('abandoned-task fault model recovers capacity only after the fixed timeout; late cancellation cannot free a new slot', async () => {
  const w = workerClock(), held = Array.from({ length: w.limits.stream.concurrent }, (_, i) => w.request('stream', `old-${i}`));
  w.abandonRequestTasks(); await w.advanceTo(SQ.PROFILE.timeoutMs - 1);
  assert.equal(w.request('stream', 'before-timeout').status, 429);
  await w.advanceTo(SQ.PROFILE.timeoutMs);
  const replacement = w.request('stream', 'after-timeout');
  assert.equal(replacement.status, 200);
  assert.equal(w.inspect().stream, 1);
  await Promise.all(held.map(r => r.body.cancel()));
  assert.equal(w.inspect().stream, 1, 'old cancellation must not decrement a replacement lease');
  await replacement.body.cancel(); assert.equal(w.inspect().stream, 0);
  assert(w.logs.some(x => /source lease expired/.test(x)), 'expiry recovery must not be silent');
});
test('a producer resuming beyond 30s fails instead of sending a stale observation window', async () => {
  const w = workerClock(), response = w.request();
  w.delayCallbacksUntil(SQ.PROFILE.timeoutMs + 1); await w.advanceTo(SQ.PROFILE.timeoutMs + 1);
  await assert.rejects(response.text(), /source request exceeded fixed timeout/);
  assert.deepEqual(w.inspect(), { stream: 0, download: 0, pendingTimers: 0 });
  assert(w.logs.some(x => /SourceDeadlineExceeded/.test(x)));
});
test('abandoned downloads have separate bounded leases and cannot release a replacement on late cancellation', async () => {
  const w = workerClock(), old = w.request('download');
  await w.advanceTo(SQ.PROFILE.timeoutMs);
  const replacement = w.request('download'); assert.equal(replacement.status, 200);
  assert.equal(w.inspect().download, 1);
  await old.body.cancel(); assert.equal(w.inspect().download, 1);
  await replacement.body.cancel(); assert.equal(w.inspect().download, 0);
});
