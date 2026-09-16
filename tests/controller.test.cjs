const { test } = require('node:test'), assert = require('node:assert/strict');
const { createController } = require('../controller.cjs');
const { run: runRound } = require('../stream-quality-runner.cjs');
const SQ = require('../stream-quality.js');
const template = { endpoint: 'https://example.com', corePath: '/fixture/core', config: { outbounds: [{ type: 'direct', tag: 'one', password: 'PRIVATE-FIXTURE-ONLY' }] },
  nodes: [{ key: 'a', tag: 'one', label: 'same node', subscriptions: ['one', 'two'] }, { key: 'b', tag: 'two', subscriptions: ['three'] }] };
const good = { ok: true, metricKind: 'stream-quality-v1', profileKey: 'same', stream: { ok: true, flowPass: true }, download: { ok: true, mbps: 10 } };
test('paired controller covers all nodes, protects credentials, serializes jobs and truly cancels while preserving history', async t => {
  let begin, starts = 0, cancelled = false;
  const started = new Promise(r => { begin = r; });
  const app = createController({ ...template, history: { a: good } }, { log: () => {}, run: async (job, { signal }) => {
    starts++; assert.equal(job.nodes.length, 2); assert.equal(job.includeDownload, false); begin(); await new Promise(r => signal.addEventListener('abort', r, { once: true })); cancelled = true;
    return { type: 'result', ok: true, cancelled: true, outcomes: job.nodes.map(n => ({ key: n.key, value: { ok: false, failureScope: 'cancelled', error: 'cancelled' } })) };
  } });
  await new Promise(r => app.server.listen(0, '127.0.0.1', r)); t.after(async () => { await app.stop(); app.server.closeAllConnections(); app.server.close(); });
  const base = `http://127.0.0.1:${app.server.address().port}`;
  const api = (route, body, extra = {}) => fetch(base + route, { method: body ? 'POST' : 'GET', headers: { authorization: `Bearer ${app.token}`, origin: 'https://lop-spec.github.io', ...extra }, body: body ? JSON.stringify(body) : undefined });
  assert.equal((await fetch(base + '/v1/status')).status, 401);
  assert.equal((await api('/v1/status', null, { origin: 'https://attacker.invalid' })).status, 403);
  const before = await (await api('/v1/status')).json(); assert.equal(before.catalog.length, 2); assert.deepEqual(before.catalog[0].subscriptions, ['one','two']);
  assert.equal(JSON.stringify(before).includes('PRIVATE-FIXTURE'), false); assert.equal(JSON.stringify(before).includes(app.token), false);
  assert.equal((await api('/v1/run', { keys: ['unknown'] })).status, 400);
  assert.equal((await api('/v1/run', { endpoint: 'https://attacker.invalid' })).status, 400);
  const response = await api('/v1/run', { rounds: 3 }); assert.equal(response.status, 202);
  const budget = await response.json(); assert.equal(budget.maxDownloadBytes, 0); assert.equal(budget.concurrency, 2); await started;
  assert.equal((await api('/v1/run', {})).status, 409); assert.equal(starts, 1);
  assert.equal((await api('/v1/cancel', {})).status, 202); await app.stop(); assert.equal(cancelled, true);
  const after = await (await api('/v1/status')).json(); assert.equal(after.state.running, false); assert.equal(after.history.a.download.mbps, 10); assert.equal(after.history.a.lastAttempt.status, 'cancelled');
});
test('endpoint faults allow active parallel samples to finish, stop later rounds and never blame nodes', async () => {
  let calls = 0;
  const result = await runRound({ endpoint: 'https://example.com', nodes: [{ key: 'a' }, { key: 'b' }], rounds: 3 }, { probeFn: async () => { calls++; return { ok: false, failureScope: 'endpoint', error: 'busy' }; } });
  assert.equal(calls, 2); assert.equal(result.channelFailure.failureScope, 'endpoint'); assert.ok(result.outcomes.every(o => o.value.failureScope === 'endpoint'));
});
test('remote SSE-only success archives old download and preserves it on later failure', async t => {
  let calls = 0;
  const sse = { ...good, measurement: 'sse-only', profileKey: 'same|sse-only' }; delete sse.download;
  const app = createController({ ...template, includeDownload: true, history: { a: good } }, { log: () => {}, run: async job => {
    assert.equal(job.includeDownload, false); calls++;
    return { type: 'result', ok: true, outcomes: [{ key: 'a', value: calls < 3 ? sse : { ok: false, failureScope: 'endpoint', error: 'source busy' } }] };
  } });
  await new Promise(r => app.server.listen(0, '127.0.0.1', r));
  t.after(async () => { await app.stop(); app.server.closeAllConnections(); app.server.close(); });
  const base = `http://127.0.0.1:${app.server.address().port}`;
  const headers = { authorization: `Bearer ${app.token}` };
  for (let i = 0; i < 3; i++) {
    const started = await fetch(base + '/v1/run', { method: 'POST', headers, body: JSON.stringify({ keys: ['a'] }) });
    assert.equal(started.status, 202); await started.json();
    const status = await (await fetch(base + '/v1/status', { headers })).json();
    assert.equal(status.history.a.measurement, 'sse-only'); assert.equal(status.history.a.download, undefined);
    assert.equal(status.history.a.legacyDownloadResult.download.mbps, 10);
    if (i === 2) assert.equal(status.history.a.lastAttempt.failureScope, 'endpoint');
  }
});
