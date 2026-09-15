const { test } = require('node:test');
const assert = require('node:assert/strict');
const SQ = require('../stream-quality.js'), runner = require('../stream-quality-runner.cjs');
const P = SQ.PROFILE;
function simulate(arrival = (i, sent) => sent + 100, sent = i => i * P.intervalMs, terminal = true) {
  const parser = SQ.analyzer();
  for (let i = 0; i < P.samples; i++) {
    const text = Buffer.from(SQ.frame(i, sent(i))), at = arrival(i, sent(i));
    parser.push(text.subarray(0, 17), at); parser.push(text.subarray(17), at);
  }
  if (terminal) parser.push(Buffer.from(SQ.endFrame()), 20100);
  return parser.result();
}
test('v1 exact 256-byte frames and 20 second schedule; no token metric', () => {
  for (let i = 0; i < P.samples; i++) assert.equal(Buffer.byteLength(SQ.frame(i, i * P.intervalMs + .3)), 256);
  assert.equal((P.samples - 1) * P.intervalMs, 20000);
  const r = simulate(); assert.equal(r.ok, true); assert.equal(r.flowPass, true); assert.equal(r.jitterMs, 0); assert.equal(r.firstSampleMs, 100); assert.equal(r.maxExtraGapMs, 0); assert.equal('tokPerSec' in r, false);
});
test('source slip excluded, network stall and batching detected independently', () => {
  const jitter = simulate((i, sent) => 100 + (i < 200 ? sent : Math.max(sent, 11000)));
  assert.equal(jitter.ok, true); assert.equal(jitter.flowPass, false); assert.ok(jitter.maxExtraGapMs >= 1000); assert.ok(jitter.burstRatio > 0);
  const badSource = simulate((i, sent) => sent + 100, i => i * 50 + (i >= 200 ? 500 : 0));
  assert.equal(badSource.ok, false); assert.equal(badSource.failureScope, 'endpoint');
});
test('truncation, malformed frame, extra terminal and byte overflow never succeed', () => {
  assert.equal(simulate(undefined, undefined, false).ok, false);
  for (const text of ['event: sample\ndata: {}\n\n', 'x'.repeat(120000)]) { const parser = SQ.analyzer(); parser.push(Buffer.from(text), 0); assert.equal(parser.result().ok, false); }
  const parser = SQ.analyzer(); parser.push(Buffer.from(SQ.frame(1, 50)), 100); assert.equal(parser.result().ok, false);
});
test('HTTPS endpoint validation rejects embedded credentials and mutable query profiles', () => {
  for (const s of ['http://example.com', 'https://u:p@example.com', 'https://example.com/?bytes=1']) assert.throws(() => SQ.endpoint(s));
  assert.equal(SQ.endpoint('https://example.com/'), 'https://example.com');
});
const sample = (mbps = 10) => ({ ok: true, metricKind: 'stream-quality-v1', profileKey: 'same', stream: { ...simulate(), flowPass: true }, download: { ok: true, mbps, endToEndMbps: mbps - 1, shortSample: false } });
test('three-pass interleaved scheduler is serial, tests every node and rotates order', async () => {
  const calls = []; let active = 0, max = 0;
  const result = await runner.run({ endpoint: 'https://example.com', nodes: [{ key: 'a', port: 1 }, { key: 'b', port: 2 }, { key: 'c', port: 3 }], rounds: 3 }, {
    probeFn: async (_url, { port }) => { active++; max = Math.max(max, active); calls.push(port); await new Promise(r => setTimeout(r, 1)); active--; return sample(port); }
  });
  assert.deepEqual(calls, [1, 2, 3, 2, 3, 1, 3, 1, 2]); assert.equal(max, 1); assert.equal(result.outcomes.length, 3);
  for (const { value } of result.outcomes) { assert.equal(value.verified, true); assert.equal(value.successRate, 1); }
});
test('cancel ends queue and returns no publishable partial score', async () => {
  const controller = new AbortController(); let calls = 0;
  const result = await runner.run({ endpoint: 'https://example.com', nodes: [{ key: 'a' }, { key: 'b' }], rounds: 3 }, { signal: controller.signal,
    probeFn: async () => { calls++; controller.abort(); return sample(); } });
  assert.equal(calls, 1); assert.equal(result.cancelled, true); assert.ok(result.outcomes.every(o => !o.value.ok && o.value.failureScope === 'cancelled'));
});
test('partial success cannot be verified; endpoint locations never aggregate; passing flows tie', () => {
  const partial = runner.aggregate([sample(), { ok: false }, sample(30)], 3, 'r');
  assert.equal(partial.verified, false); assert.equal(partial.stream.flowPass, false); assert.equal(partial.successRate, 2 / 3);
  assert.equal(runner.aggregate([sample(), { ...sample(), profileKey: 'other-colo' }], 3, 'r').ok, false);
  assert.equal(SQ.compare(sample(10), sample(100)), 0);
});
