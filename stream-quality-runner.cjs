#!/usr/bin/env node
'use strict';
const SQ = require('./stream-quality.js');
const http = require('node:http'), https = require('node:https'), tls = require('node:tls'), net = require('node:net');
const fs = require('node:fs/promises'), path = require('node:path'), os = require('node:os');
const { spawn } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const P = SQ.PROFILE;
const failure = (error, scope = 'node') => Object.assign(Error(error), { failureScope: scope });
function request(base, route, { port = 0, signal, maxBytes, onData = () => {}, onHeaders = () => {} } = {}) {
  return new Promise((resolve, reject) => {
    const target = new URL(base + route), start = performance.now();
    let agent, connect, socket, req, response, settled = false, bytes = 0, firstAt;
    const finish = error => {
      if (settled) return; settled = true; clearTimeout(timer); signal?.removeEventListener('abort', abort);
      const elapsedMs = performance.now() - start;
      response?.destroy(); req?.destroy(); connect?.destroy(); socket?.destroy(); agent?.destroy();
      if (error) reject(error); else resolve({ bytes, elapsedMs, firstByteMs: firstAt ?? elapsedMs, transferMs: Math.max(.1, elapsedMs - (firstAt ?? elapsedMs)) });
    };
    const abort = () => finish(failure('cancelled', 'cancelled'));
    const timer = setTimeout(() => finish(failure('request timed out')), P.timeoutMs);
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) { abort(); return; }
    const options = { method: 'GET', headers: { 'accept-encoding': 'identity', 'cache-control': 'no-cache', 'user-agent': 'stream-quality/1.0' } };
    if (port && target.protocol === 'https:') {
      agent = new https.Agent({ keepAlive: false });
      agent.createConnection = (_options, callback) => {
        let called = false;
        const ready = (error, sock) => { if (called) return; called = true; callback(error, sock); };
        connect = http.request({ host: '127.0.0.1', port, method: 'CONNECT', path: `${target.hostname}:${target.port || 443}`, agent: false });
        connect.on('error', error => { ready(error); finish(error); });
        connect.on('connect', (res, raw, head) => {
          socket = raw;
          if (settled) { raw.destroy(); return; }
          if (res.statusCode !== 200) { const error = failure(`proxy CONNECT HTTP ${res.statusCode}`); ready(error); finish(error); return; }
          if (head.length) raw.unshift(head);
          socket = tls.connect({ socket: raw, servername: net.isIP(target.hostname) ? undefined : target.hostname, rejectUnauthorized: true }, () => ready(null, socket));
          socket.on('error', error => { ready(error); finish(error); });
        });
        connect.end();
      };
      options.agent = agent;
    } else options.agent = false;
    try {
      const transport = target.protocol === 'https:' ? https : http;
      const address = port && target.protocol === 'http:' ? { hostname: '127.0.0.1', port, path: target.href, headers: { ...options.headers, host: target.host } } : {};
      req = transport.request(target, { ...options, ...address }, res => {
        response = res;
        try {
          if (res.statusCode !== 200) throw failure(`HTTP ${res.statusCode}`, [429, 500, 502, 503, 504].includes(res.statusCode) ? 'endpoint' : 'node');
          if (res.headers['content-encoding'] && res.headers['content-encoding'] !== 'identity') throw failure('compressed workload rejected', 'measurement');
          onHeaders(res.headers);
        } catch (error) { finish(error); return; }
        res.on('data', chunk => {
          if (settled) return;
          firstAt ??= performance.now() - start; bytes += chunk.length;
          try { if (bytes > maxBytes) throw failure('byte budget exceeded', 'measurement'); onData(chunk, performance.now() - start); }
          catch (error) { finish(error); }
        });
        res.on('end', () => finish());
        res.on('aborted', () => finish(failure('truncated response')));
        res.on('error', finish);
      });
      req.on('error', finish); req.end();
    } catch (error) { finish(error); }
  });
}
async function probe(base, { port = 0, signal, onProgress = () => {} } = {}) {
  base = SQ.endpoint(base, ['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
  let manifestText = '';
  await request(base, '/api/manifest', { port, signal, maxBytes: 8192, onData: chunk => { manifestText += chunk; } });
  let manifest;
  try { manifest = JSON.parse(manifestText); } catch { throw failure('invalid manifest', 'endpoint'); }
  if (manifest.profile?.id !== P.id || Object.keys(P).some(key => manifest.profile[key] !== P[key])) throw failure('incompatible profile', 'endpoint');
  const checkHeaders = headers => {
    if (headers['x-stream-quality-profile'] !== P.id) throw failure('profile header mismatch', 'measurement');
    if (headers['x-stream-quality-location'] !== manifest.location) throw failure('endpoint location changed during sample', 'endpoint');
  };
  onProgress('stream');
  const parser = SQ.analyzer();
  await request(base, '/api/stream', { port, signal, maxBytes: P.samples * P.frameBytes + 4096,
    onHeaders: headers => { checkHeaders(headers); if (!headers['content-type']?.startsWith('text/event-stream')) throw failure('not SSE', 'measurement'); },
    onData: (chunk, at) => { parser.push(chunk, at); if (parser.error) throw failure(parser.error, 'measurement'); } });
  const stream = parser.result();
  if (!stream.ok) return { ...stream, stream };
  onProgress('download');
  const download = await request(base, '/api/download', { port, signal, maxBytes: P.downloadBytes, onHeaders: checkHeaders });
  if (download.bytes !== P.downloadBytes) throw failure('download truncated');
  return { ok: true, metricKind: 'stream-quality-v1', endpoint: base, location: manifest.location,
    profileKey: SQ.profileKey(base, manifest.location), measuredAt: Date.now(), stream,
    download: { ...download, ok: true, mbps: download.bytes * 8 / download.transferMs / 1000,
      endToEndMbps: download.bytes * 8 / download.elapsedMs / 1000, shortSample: download.transferMs < 1000 } };
}
async function reservePorts(count) {
  const reservations = [];
  try {
    for (let i = 0; i < count; i++) {
      const server = net.createServer(); reservations.push(server);
      await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
    }
    return reservations.map(s => s.address().port);
  } finally { await Promise.all(reservations.map(s => new Promise(resolve => s.close(resolve)))); }
}
function isolatedConfig(job, ports) {
  const byTag = new Map(job.config.outbounds.map(o => [o.tag, o])), required = new Set(job.nodes.map(n => n.tag));
  for (const server of job.config.dns?.servers || []) if (server.detour) required.add(server.detour);
  for (const tag of required) {
    const outbound = byTag.get(tag);
    if (!outbound || ['selector', 'urltest'].includes(outbound.type)) throw failure('test route dependency is missing or would perform automatic selection/probing', 'round');
    if (outbound.detour) required.add(outbound.detour);
  }
  return { log: { level: 'error', timestamp: false }, dns: job.config.dns,
    outbounds: [...required].map(tag => byTag.get(tag)),
    inbounds: ports.map((port, i) => ({ type: 'mixed', tag: `sq-in-${i}`, listen: '127.0.0.1', listen_port: port })),
    route: { auto_detect_interface: true, default_domain_resolver: job.config.route?.default_domain_resolver,
      rules: job.nodes.map((node, i) => ({ inbound: [`sq-in-${i}`], outbound: node.tag })), final: job.nodes[0].tag } };
}
async function startCore(job, signal, log) {
  if (!job.corePath || !path.isAbsolute(job.corePath)) throw failure('absolute sing-box corePath required', 'round');
  const tags = new Set((job.config?.outbounds || []).map(o => o.tag));
  if (!job.nodes.length || job.nodes.some(n => !tags.has(n.tag))) throw failure('some cached nodes were not converted to sing-box; no silent filtering', 'round');
  const ports = await reservePorts(job.nodes.length), config = isolatedConfig(job, ports);
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'stream-quality-')), configPath = path.join(dir, 'probe.json');
  // No TUN, system proxy, controller, cache file, selector changes or daily-core operations.
  let child, exited = false, stderr = '', spawnError;
  const cleanup = async () => {
    if (child && !exited) {
      child.kill();
      await Promise.race([new Promise(resolve => child.once('exit', resolve)), new Promise(resolve => setTimeout(resolve, 2000))]);
      if (!exited) { log('owned test core did not stop promptly'); child.kill('SIGKILL'); }
    }
    await fs.unlink(configPath).catch(error => { if (error.code !== 'ENOENT') log(`private temporary config cleanup failed: ${error.code}`); });
    await fs.rmdir(dir).catch(error => log(`temporary directory cleanup failed: ${error.code}`));
  };
  try {
    await fs.writeFile(configPath, JSON.stringify(config), { mode: 0o600 });
    child = spawn(job.corePath, ['run', '-c', configPath], { windowsHide: true, stdio: ['ignore', 'ignore', 'pipe'] });
    child.once('error', error => { spawnError = error; }); child.once('exit', () => { exited = true; });
    child.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-2000); });
    const deadline = Date.now() + 10000;
    while (Date.now() < deadline) {
      if (signal.aborted) throw failure('cancelled', 'cancelled');
      if (spawnError || exited) throw failure(`isolated core failed: ${spawnError?.code || 'configuration rejected'}`, 'round');
      const ready = await new Promise(resolve => {
        const socket = net.connect(ports.at(-1), '127.0.0.1');
        socket.setTimeout(200); const finish = value => { socket.destroy(); resolve(value); };
        socket.once('connect', () => finish(true)); socket.once('error', () => finish(false)); socket.once('timeout', () => finish(false));
      });
      if (ready) return { ports, cleanup, pid: child.pid };
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    // Do not echo sing-box stderr: node credentials can occur in parser diagnostics.
    throw failure(`isolated core readiness timeout (${stderr ? 'diagnostics withheld to protect node credentials' : 'no diagnostics'})`, 'round');
  } catch (error) { await cleanup(); throw error; }
}
function aggregate(samples, total, roundId) {
  const good = samples.filter(SQ.isResult);
  if (!good.length) return { ...(samples.at(-1) || { ok: false, failureScope: 'cancelled', error: 'not started' }), roundId };
  if (good.length !== total) {
    const failed = samples.find(s => !SQ.isResult(s));
    return { ok: false, verified: false, failureScope: failed?.failureScope || 'measurement',
      error: `${good.length}/${total} complete samples; ${failed?.error || 'incomplete round'}`, roundId,
      successfulSamples: good.length, sampleCount: total, successRate: good.length / total };
  }
  const profiles = new Set(good.map(s => s.profileKey));
  if (profiles.size > 1) return { ok: false, failureScope: 'endpoint', error: 'endpoint location changed between repetitions', roundId };
  const median = values => SQ.percentile(values, .5), worst = Math.max;
  const stream = { ...good[0].stream, flowPass: good.length === total && good.every(s => s.stream.flowPass) };
  for (const key of ['firstSampleMs', 'jitterMs', 'p95ExtraGapMs', 'deliveryMs']) stream[key] = median(good.map(s => s.stream[key]));
  for (const key of ['maxExtraGapMs', 'longestGapMs', 'stallCount', 'burstRatio', 'sourceSlipMs', 'tailGrowthMs']) stream[key] = worst(...good.map(s => s.stream[key]));
  return { ...good.at(-1), stream, download: { ...good.at(-1).download, mbps: median(good.map(s => s.download.mbps)),
    endToEndMbps: median(good.map(s => s.download.endToEndMbps)), shortSample: good.some(s => s.download.shortSample) },
    roundId, sampleCount: total, successfulSamples: good.length, successRate: good.length / total,
    verified: total >= 3 && good.length === total, samples: samples.map(s => ({ ok: s.ok, error: s.error, failureScope: s.failureScope,
      firstSampleMs: s.stream?.firstSampleMs, jitterMs: s.stream?.jitterMs, maxExtraGapMs: s.stream?.maxExtraGapMs, mbps: s.download?.mbps })) };
}
async function run(job, { signal = new AbortController().signal, emit = () => {}, probeFn = probe } = {}) {
  const started = Date.now(), roundId = randomUUID(), nodes = job.nodes || [{ key: 'current-route', port: Number(job.port || 0) }];
  const rounds = Number(job.rounds || 1);
  if (![1, 3].includes(rounds) || nodes.length < 1 || nodes.length > 1000 || new Set(nodes.map(n => n.key)).size !== nodes.length) throw failure('invalid round count or duplicate/empty node list', 'round');
  const total = nodes.length * rounds, collected = new Map(nodes.map(n => [n.key, []]));
  let core, channelFailure, completed = 0;
  const log = message => emit({ type: 'log', message });
  const supported = job.config ? new Set((job.config.outbounds || []).filter(o => !['selector', 'urltest'].includes(o.type)).map(o => o.tag)) : null;
  const eligible = supported ? nodes.filter(n => supported.has(n.tag)) : nodes;
  const unsupported = nodes.filter(n => !eligible.includes(n));
  for (const node of unsupported) collected.get(node.key).push({ ok: false, failureScope: 'unsupported', error: 'cached node protocol was not converted by the host; not a connection failure' });
  if (unsupported.length) log(`${unsupported.length} unsupported cached node(s) explicitly reported; continuing with ${eligible.length} supported node(s)`);
  completed += unsupported.length * rounds;
  try {
    if (job.config && eligible.length) core = await startCore({ ...job, nodes: eligible }, signal, log);
    const portByKey = new Map(eligible.map((n, i) => [n.key, core ? core.ports[i] : n.port]));
    emit({ type: 'start', roundId, profile: P.id, total, nodes: nodes.length, unsupported: unsupported.length, maxDownloadBytes: eligible.length * rounds * P.downloadBytes, concurrency: 1, isolatedPid: core?.pid });
    for (let round = 0; round < rounds && !signal.aborted && !channelFailure; round++) {
      // Rotate across nodes, not three consecutive trials on the same favorite.
      for (let offset = 0; offset < nodes.length && !signal.aborted; offset++) {
        const index = (offset + round) % nodes.length, node = nodes[index];
        if (!portByKey.has(node.key)) continue;
        let value, phase = 'manifest';
        try { value = await probeFn(job.endpoint, { signal, port: portByKey.get(node.key),
          onProgress: current => { phase = current; emit({ type: 'progress', phase, key: node.key, completed, total, round: round + 1 }); } }); }
        catch (error) { value = { ok: false, phase, failureScope: signal.aborted ? 'cancelled' : error.failureScope || 'node', error: signal.aborted ? 'cancelled' : `${phase}: ${error.message}` }; }
        collected.get(node.key).push(value); completed++;
        if (!value.ok) log(`sample failed: ${value.failureScope}: ${value.error}`);
        emit({ type: 'progress', phase: 'sample-done', key: node.key, completed, total, round: round + 1 });
        if (value.failureScope === 'endpoint') { channelFailure = value; log('endpoint failure stopped the remaining queue; unmeasured nodes are not blamed'); break; }
      }
    }
    const outcomes = nodes.map(node => ({ key: node.key, port: node.port, value: signal.aborted
      ? { ok: false, failureScope: 'cancelled', error: 'round cancelled; previous score retained', roundId }
      : collected.get(node.key).length < rounds && channelFailure ? { ...channelFailure, ok: false, roundId }
      : aggregate(collected.get(node.key), rounds, roundId) }));
    return { type: 'result', ok: true, cancelled: signal.aborted, channelFailure, roundId, outcomes, elapsedMs: Date.now() - started, activity: { maxActiveTotal: 1 } };
  } finally { await core?.cleanup(); }
}
async function cli() {
  const args = process.argv.slice(2), value = flag => args.includes(flag) ? args[args.indexOf(flag) + 1] : undefined;
  const job = args.includes('--job') ? JSON.parse(await fs.readFile(value('--job'), 'utf8'))
    : { endpoint: value('--endpoint'), port: Number(value('--port') || 0), rounds: Number(value('--rounds') || 1) };
  if (!job.endpoint) throw failure('usage: node stream-quality-runner.cjs --endpoint HTTPS_URL [--port LOCAL_PROXY_PORT] [--rounds 1|3]', 'round');
  const controller = new AbortController(), emit = event => process.stdout.write(JSON.stringify(event) + '\n');
  let input = '';
  const cancel = () => controller.abort();
  process.on('SIGINT', cancel); process.on('SIGTERM', cancel);
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', text => { input += text; if (input.length > 8192) { cancel(); return; } let i;
    while ((i = input.indexOf('\n')) >= 0) { const line = input.slice(0, i); input = input.slice(i + 1); try { if (JSON.parse(line).action === 'cancel') cancel(); } catch { emit({ type: 'log', message: 'ignored malformed control input' }); } }
  });
  if (args.includes('--job')) process.stdin.on('end', cancel);
  try { emit(await run(job, { signal: controller.signal, emit })); }
  finally { process.stdin.pause(); process.stdin.removeAllListeners(); process.removeListener('SIGINT', cancel); process.removeListener('SIGTERM', cancel); }
}
module.exports = { request, probe, aggregate, run, startCore, isolatedConfig };
if (require.main === module) cli().catch(error => { process.stdout.write(JSON.stringify({ type: 'result', ok: false, failureScope: error.failureScope || 'round', error: error.message }) + '\n'); process.exitCode = 1; process.stdin.pause(); });
