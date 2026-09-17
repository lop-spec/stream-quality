'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), path = require('node:path');
const { fork } = require('node:child_process');
const { setTimeout: sleep } = require('node:timers/promises');
const { probeEcho, runLocal, summarize } = require('../scripts/local-sse-prototype.cjs');
const legacy = require('../stream-quality.js');
const target = { host: 'echo.fixture.invalid', port: 7 };
const workload = { samples: 20, intervalMs: 20, frameBytes: 256, drainMs: 1000 };
async function circuit(t, modes, w = workload) {
  const child = fork(path.join(__dirname, 'fixtures/local-echo-circuit.cjs'), [], { windowsHide: true, stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  let stderr = ''; child.stderr.on('data', b => { stderr += b; });
  const closed = new Promise(resolve => child.once('close', (code, signal) => resolve({ code, signal })));
  const message = type => new Promise((resolve, reject) => {
    const onMessage = m => { if (m.type === type) { cleanup(); resolve(m); } };
    const failed = () => { cleanup(); reject(Error(`fixture exited: ${stderr}`)); };
    const timer = setTimeout(() => { cleanup(); reject(Error(`fixture ${type} timeout`)); }, 8000);
    function cleanup() { clearTimeout(timer); child.off('message', onMessage); child.off('exit', failed); }
    child.on('message', onMessage); child.once('exit', failed);
  });
  t.after(async () => {
    const guard = setTimeout(() => child.kill(), 1500);
    if (child.connected) child.send({ type: 'stop' });
    const result = await closed; clearTimeout(guard); assert.equal(stderr, ''); assert.equal(result.code, 0); assert.equal(result.signal, null);
  });
  const ready = message('ready'); child.send({ type: 'start', modes, ...w });
  const { ports } = await ready;
  return { ports, async stats() { const got = message('stats'); child.send({ type: 'stats' }); return got; } };
}
const options = port => ({ port, workload });
function noScore(result) {
  assert.equal(result.sseDownlinkVerified, false); assert.equal(result.qualityPass, null); assert.equal(result.accuracy, 'unvalidated');
  assert.equal(legacy.isResult(result), false, 'must not enter the legacy quality score store');
}
test('prototype has no implicit source, PING path or direct fallback', async () => {
  await assert.rejects(probeEcho(target, { workload }), /dedicated loopback/);
  await assert.rejects(probeEcho({ host: 'user:secret@example.test', port: 7 }, options(1080)), /explicit echo/);
  await assert.rejects(probeEcho(target, { port: 1080, workload: { ...workload, samples: 100000 } }), /bounded/);
  await assert.rejects(runLocal({ target, workload, concurrency: 2, nodes: [{ key: 'a', port: 1 }, { key: 'b', port: 1 }] }), /own explicit proxy port/);
});
test('locally generated SSE bytes traverse only the selected CONNECT lane', { timeout: 8000 }, async t => {
  const fixture = await circuit(t, ['healthy', 'healthy']);
  const result = await probeEcho(target, options(fixture.ports[1]));
  assert.equal(result.ok, true); assert.equal(result.metrics.receivedSamples, workload.samples);
  assert.equal(result.terminalReceived, true); assert.equal(result.receivedBytes, 21 * 256); noScore(result);
  const { stats } = await fixture.stats();
  assert.equal(stats[0].connects, 0, 'unselected proxy must see no traffic');
  assert.equal(stats[1].connects, 1); assert.deepEqual(stats[1].targets, ['echo.fixture.invalid:7']);
  assert.equal(stats[1].upstreamBytes, result.receivedBytes); assert.equal(stats[1].downstreamBytes, result.receivedBytes);
});
test('held frames and batch release are measured from received bytes, not fabricated local playback', { timeout: 8000 }, async t => {
  const fixture = await circuit(t, ['healthy', 'stall', 'batch']);
  const [healthy, stalled, batched] = await Promise.all(fixture.ports.map(port => probeEcho(target, options(port))));
  for (const result of [healthy, stalled, batched]) { assert.equal(result.ok, true); noScore(result); }
  assert.ok(stalled.metrics.maxExtraGapMs > 130, JSON.stringify(stalled.metrics));
  assert.ok(stalled.metrics.compressedIntervals >= 5, JSON.stringify(stalled.metrics));
  assert.ok(batched.metrics.firstEchoMs > 300);
  assert.ok(batched.metrics.compressedIntervals >= 18, JSON.stringify(batched.metrics));
  assert.ok(batched.metrics.roundtripVariationMs > 200);
});
test('disconnect, corrupt payload and ordinary HTTP response cannot become an SSE pass', { timeout: 8000 }, async t => {
  const fixture = await circuit(t, ['cut', 'corrupt', 'http']);
  const results = await Promise.all(fixture.ports.map(port => probeEcho(target, options(port))));
  for (const result of results) { assert.equal(result.ok, false); assert.equal(result.terminalReceived, false); noScore(result); }
  assert.ok(results[0].metrics.receivedSamples < workload.samples); assert.equal(results[1].reason, 'integrity');
  assert.equal(results[2].reason, 'incomplete');
});
test('silent source times out and a hung CONNECT cancels without leaked sockets', { timeout: 8000 }, async t => {
  const fixture = await circuit(t, ['silent', 'connect-hang']);
  const controller = new AbortController();
  const cancelled = probeEcho(target, { ...options(fixture.ports[1]), signal: controller.signal });
  setTimeout(() => controller.abort(), 60);
  assert.equal((await cancelled).reason, 'cancelled');
  const silent = await probeEcho(target, { ...options(fixture.ports[0]), workload: { ...workload, samples: 3, drainMs: 60 } });
  assert.equal(silent.reason, 'incomplete'); assert.equal(silent.metrics.receivedSamples, 0); noScore(silent);
  await sleep(30); assert.equal((await fixture.stats()).openSockets, 0);
});
test('roundtrip traces cannot distinguish uplink buffering from downlink buffering', () => {
  const uplink = Array.from({ length: 10 }, (_, seq) => ({ seq, sentMs: seq * 20, receivedMs: 250 }));
  const downlink = structuredClone(uplink);
  assert.deepEqual(summarize(uplink, 20), summarize(downlink, 20));
  assert.equal(summarize(uplink, 20).directionality, 'roundtrip-only');
});
test('outer budget retains all unmeasured nodes, includes cleanup and logs the reason', { timeout: 8000 }, async t => {
  const fixture = await circuit(t, ['silent', 'healthy']), events = [];
  const result = await runLocal({ target, workload, nodes: fixture.ports.map((port, i) => ({ key: `fixture-${i}`, port })), concurrency: 1, wallBudgetMs: 200 }, { emit: e => events.push(e) });
  assert.equal(result.nodeCount, 2); assert.equal(result.outcomes.length, 2); assert.equal(result.startedNodes, 1);
  assert.equal(result.outcomes[1].reason, 'unmeasured'); assert.equal(result.budgetExpired, true);
  assert.equal(result.acceptance, false); assert.ok(events.some(e => e.type === 'log' && /deadline/.test(e.message)));
  await sleep(30); assert.equal((await fixture.stats()).openSockets, 0);
});
test('setup and owned-core cleanup are inside the unrounded outer wall clock', { timeout: 8000 }, async t => {
  const fixture = await circuit(t, ['healthy']); let cleaned = false;
  const result = await runLocal({ target, workload, config: { outbounds: [{ tag: 'fixture', type: 'http' }] }, nodes: [{ key: 'fixture', tag: 'fixture' }], concurrency: 1 }, {
    startCoreFn: async () => { await sleep(80); return { ports: fixture.ports, cleanup: async () => { await sleep(90); cleaned = true; } }; }
  });
  assert.equal(cleaned, true); assert.equal(result.completeEchoNodes, 1);
  assert.ok(result.elapsedIncludingCleanupMs >= result.outcomes[0].elapsedIncludingSocketCleanupMs + 160);
  assert.equal(result.acceptance, false);
});
test('173 local fixture lanes are all represented; fast fixture completion is never WAN acceptance', { timeout: 12000 }, async t => {
  const w = { ...workload, samples: 5, intervalMs: 20, drainMs: 2000 };
  const fixture = await circuit(t, Array(173).fill('healthy'), w);
  const result = await runLocal({ target, workload: w, nodes: fixture.ports.map((port, i) => ({ key: `fixture-${i}`, port })), concurrency: 173 });
  assert.equal(result.nodeCount, 173); assert.equal(result.startedNodes, 173); assert.equal(result.completeEchoNodes, 173);
  assert.equal(result.outcomes.length, 173); assert.equal(result.acceptance, false); assert.equal(result.sseDownlinkVerified, false);
  const { stats } = await fixture.stats();
  assert.equal(Object.keys(stats).length, 173); assert.ok(Object.values(stats).every(s => s.connects === 1 && s.upstreamBytes === 6 * 256 && s.downstreamBytes === 6 * 256));
  t.diagnostic(`FIXTURE ONLY: ${result.elapsedIncludingCleanupMs.toFixed(2)}ms including lane cleanup; not 173 real subscription nodes`);
});
test('actual isolated sing-box routes each fixture node through its selected proxy outbound', { timeout: 15000, skip: !process.env.SQ_TEST_CORE_PATH && 'set SQ_TEST_CORE_PATH to an existing authorized core; no automatic download' }, async t => {
  const fixture = await circuit(t, ['healthy', 'stall']);
  const nodes = [{ key: 'fixture-A', tag: 'fixture-A' }, { key: 'fixture-B', tag: 'fixture-B' }];
  const result = await runLocal({ target, workload, nodes, concurrency: 2, corePath: process.env.SQ_TEST_CORE_PATH,
    config: { outbounds: nodes.map((n, i) => ({ type: 'http', tag: n.tag, server: '127.0.0.1', server_port: fixture.ports[i] })) } });
  assert.equal(result.roundFailure, undefined); assert.equal(result.completeEchoNodes, 2);
  assert.ok(result.outcomes[1].metrics.maxExtraGapMs > 130); assert.notEqual(result.outcomes[0].route.proxyPort, result.outcomes[1].route.proxyPort);
  const { stats } = await fixture.stats(); assert.equal(stats[0].connects, 1); assert.equal(stats[1].connects, 1);
  assert.equal(result.acceptance, false);
  t.diagnostic(`Actual isolated core / synthetic local nodes only: ${result.elapsedIncludingCleanupMs.toFixed(2)}ms including owned-core cleanup`);
});
