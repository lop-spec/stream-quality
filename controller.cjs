#!/usr/bin/env node
'use strict';
// Private execution-layer API. The public Pages site never receives subscription credentials.
const http = require('node:http'), fs = require('node:fs/promises'), path = require('node:path'), os = require('node:os');
const { randomBytes, timingSafeEqual } = require('node:crypto');
const runner = require('./stream-quality-runner.cjs'), SQ = require('./stream-quality.js');
const WEB = new Map([['/', 'web/index.html'], ['/app.js', 'web/app.js'], ['/controller.js', 'web/controller.js'], ['/style.css', 'web/style.css'], ['/stream-quality.js', 'stream-quality.js']]);
function createController(template, { run = runner.run, onEvent = () => {}, log = message => console.error(message), token = randomBytes(32).toString('hex') } = {}) {
  if (!template.config || !Array.isArray(template.nodes) || !template.nodes.length || template.nodes.length > 1000) throw Error('private job must include config and 1..1000 unique cached nodes');
  if (new Set(template.nodes.map(n => n.key)).size !== template.nodes.length) throw Error('deduplicate nodes by fingerprint before creating the private job');
  SQ.endpoint(template.endpoint, ['localhost', '127.0.0.1'].includes(new URL(template.endpoint).hostname));
  const catalog = template.nodes.map(n => ({ key: n.key, label: n.label || n.tag, subscriptions: n.subscriptions || [] }));
  let active, state = { running: false }, version = 0, lastResult;
  const history = Object.fromEntries(catalog.map(n => [n.key, template.history?.[n.key]]).filter(([, v]) => v));
  const notify = event => { version++; onEvent(event); };
  const cancel = () => { active?.controller.abort(); };
  const stop = async () => { cancel(); await active?.promise; };
  function originAllowed(req) {
    const origin = req.headers.origin, host = req.headers.host || '';
    return !origin || origin === 'https://lop-spec.github.io'
      || /^localhost:\d+$|^127\.0\.0\.1:\d+$/.test(host) && origin === `http://${host}`
      || /^[a-z0-9.-]+\.ts\.net(?::\d+)?$/i.test(host) && origin === `https://${host}`;
  }
  const server = http.createServer(async (req, res) => {
    const headers = { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff', 'referrer-policy': 'no-referrer' };
    const send = (status, data) => { if (!res.destroyed) { res.writeHead(status, headers); res.end(JSON.stringify(data)); } };
    try {
      if (!originAllowed(req)) { send(403, { error: 'origin rejected' }); return; }
      if (req.headers.origin) headers['access-control-allow-origin'] = req.headers.origin;
      headers.vary = 'Origin';
      if (req.method === 'OPTIONS') {
        headers['access-control-allow-methods'] = 'GET, POST, OPTIONS'; headers['access-control-allow-headers'] = 'Authorization, Content-Type';
        headers['access-control-allow-private-network'] = 'true'; res.writeHead(204, headers); res.end(); return;
      }
      const url = new URL(req.url, 'http://localhost');
      if (url.search) { send(400, { error: 'query parameters are not accepted; use Authorization headers' }); return; }
      if (req.method === 'GET' && WEB.has(url.pathname)) {
        const file = WEB.get(url.pathname), content = await fs.readFile(path.join(__dirname, file));
        headers['content-type'] = file.endsWith('.js') ? 'text/javascript; charset=utf-8' : file.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8';
        res.writeHead(200, headers); res.end(content); return;
      }
      const supplied = Buffer.from(String(req.headers.authorization || '').replace(/^Bearer /, '')), expected = Buffer.from(token);
      if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) { send(401, { error: 'pairing required' }); return; }
      if (req.method === 'GET' && url.pathname === '/v1/status') {
        send(200, { version, executionHost: os.hostname(), endpoint: template.endpoint, catalog, history, state, lastResult }); return;
      }
      if (req.method !== 'POST') { send(404, { error: 'not found' }); return; }
      let raw = ''; for await (const chunk of req) { raw += chunk; if (Buffer.byteLength(raw) > 65536) { send(413, { error: 'control body exceeds 64 KiB' }); return; } }
      let input; try { input = JSON.parse(raw || '{}'); } catch { send(400, { error: 'invalid JSON' }); return; }
      if (url.pathname === '/v1/cancel') { cancel(); send(202, { cancelling: !!active }); return; }
      if (url.pathname !== '/v1/run') { send(404, { error: 'not found' }); return; }
      if (active) { send(409, { error: 'one measurement at a time; stop or await the current job' }); return; }
      if (Object.keys(input).some(key => !['rounds', 'keys'].includes(key))) { log('controller rejected unknown run fields'); send(400, { error: 'only rounds and cached-node keys are accepted' }); return; }
      const rounds = Number(input.rounds || 1), keys = input.keys || catalog.map(n => n.key);
      if (![1, 3].includes(rounds) || !Array.isArray(keys) || !keys.length || new Set(keys).size !== keys.length || keys.some(k => !catalog.some(n => n.key === k))) { send(400, { error: 'invalid rounds or cached-node selection' }); return; }
      const nodes = template.nodes.filter(n => keys.includes(n.key)), controller = new AbortController(), owned = { controller, promise: null };
      active = owned; state = { running: true, completed: 0, total: nodes.length * rounds, measurement: 'sse-only',
        maxDownloadBytes: 0, maxSseBytes: nodes.length * rounds * (SQ.PROFILE.samples * SQ.PROFILE.frameBytes + 4096), concurrency: nodes.length };
      notify({ ...state, type: 'controller-state' });
      owned.promise = Promise.resolve().then(() => run({ ...template, nodes, rounds, includeDownload: false }, { signal: controller.signal, emit: event => {
        if (event.type === 'progress') state = { ...state, ...event };
        if (event.type === 'log') log(event.message); notify(event);
      } })).then(result => {
        lastResult = result;
        for (const { key, value } of result.outcomes || []) {
          const attempt = { status: value.ok ? 'done' : result.cancelled ? 'cancelled' : 'error', error: value.error, failureScope: value.failureScope, measuredAt: Date.now() };
          const previous = history[key];
          const legacy = SQ.isResult(previous) && previous.measurement !== 'sse-only' ? previous : previous?.legacyDownloadResult;
          history[key] = SQ.isResult(value) ? { ...value,
            ...(value.measurement === 'sse-only' && legacy ? { legacyDownloadResult: legacy } : {}), lastAttempt: attempt }
            : { ...(previous || {}), lastAttempt: attempt };
        }
        notify(result);
      }).catch(error => { log(`controller round failed: ${error.message}`); state.error = error.message;
        lastResult = { type: 'result', ok: false, error: error.message, outcomes: nodes.map(n => ({ key: n.key, value: { ok: false, failureScope: 'round', error: error.message } })) };
        for (const n of nodes) history[n.key] = { ...(history[n.key] || {}), lastAttempt: { status: 'error', error: error.message, measuredAt: Date.now() } };
        notify(lastResult);
      }).finally(() => { active = null; state = { ...state, running: false, cancelled: controller.signal.aborted }; notify({ ...state, type: 'controller-state' }); });
      send(202, { started: true, ...state });
    } catch (error) { log(`controller request failed: ${error.message}`); send(500, { error: 'controller request failed; see local log' }); }
  });
  server.requestTimeout = 10000; server.headersTimeout = 10000;
  return { server, token, cancel, stop };
}
async function cli() {
  const args = process.argv.slice(2), arg = flag => args[args.indexOf(flag) + 1];
  if (!args.includes('--job')) throw Error('usage: node controller.cjs --job PRIVATE_JOB.json [--port 8799]');
  const template = JSON.parse(await fs.readFile(arg('--job'), 'utf8'));
  const emit = e => process.stdout.write(JSON.stringify(e) + '\n');
  const app = createController(template, { onEvent: emit, log: message => emit({ type: 'log', message }) });
  const port = args.includes('--port') ? Number(arg('--port')) : 8799;
  await new Promise((resolve, reject) => { app.server.once('error', reject); app.server.listen(port, '127.0.0.1', resolve); });
  emit({ type: 'controller-ready', port: app.server.address().port, token: app.token, executionHost: os.hostname() });
  let stopping = false, input = '';
  const shutdown = async () => { if (stopping) return; stopping = true; await app.stop(); app.server.closeAllConnections(); await new Promise(resolve => app.server.close(resolve)); process.stdin.pause(); };
  process.on('SIGINT', shutdown); process.on('SIGTERM', shutdown);
  process.stdin.setEncoding('utf8'); process.stdin.on('end', shutdown);
  process.stdin.on('data', chunk => { input += chunk; if (input.length > 8192) { void shutdown(); return; } let i;
    while ((i = input.indexOf('\n')) >= 0) { const line = input.slice(0, i); input = input.slice(i + 1); try { const m = JSON.parse(line); if (m.action === 'cancel') app.cancel(); else if (m.action === 'shutdown') void shutdown(); } catch { emit({ type: 'log', message: 'controller ignored invalid control input' }); } }
  });
}
module.exports = { createController };
if (require.main === module) cli().catch(error => { console.error('Controller: ' + error.message); process.exitCode = 1; });
