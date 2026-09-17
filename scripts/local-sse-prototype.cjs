#!/usr/bin/env node
'use strict';
// Experimental, locally paced SSE payloads over a CONNECT + TCP echo circuit.
// NOT HTTP SSE, NOT a downlink-only measurement, NOT a production node score.
const http = require('node:http');
const fs = require('node:fs/promises');
const { randomUUID } = require('node:crypto');
const { performance, monitorEventLoopDelay } = require('node:perf_hooks');
const { setMaxListeners } = require('node:events');
const { startCore } = require('../stream-quality-runner.cjs');

function validate(target, workload) {
  if (!target || typeof target.host !== 'string' || !/^[a-zA-Z0-9.-]{1,253}$/.test(target.host)
    || !Number.isInteger(target.port) || target.port < 1 || target.port > 65535) throw Error('explicit echo host and TCP port required (no URLs or credentials)');
  if (!workload || !Number.isInteger(workload.samples) || workload.samples < 2 || workload.samples > 2048
    || !Number.isInteger(workload.intervalMs) || workload.intervalMs < 2 || workload.intervalMs > 1000
    || !Number.isInteger(workload.frameBytes) || workload.frameBytes < 256 || workload.frameBytes > 4096
    || !Number.isInteger(workload.drainMs) || workload.drainMs < 10 || workload.drainMs > 10000
    || (workload.samples - 1) * workload.intervalMs + workload.drainMs > 55000) throw Error('explicit bounded simulation workload required');
}
function frame(id, seq, sentMs, workload, terminal = false) {
  const data = { id, seq, sentMs, ...(terminal ? { samples: workload.samples } : {}), pad: '' };
  const render = () => `event: ${terminal ? 'end' : 'sample'}\ndata: ${JSON.stringify(data)}\n\n`;
  data.pad = 'x'.repeat(Math.max(0, workload.frameBytes - Buffer.byteLength(render())));
  const result = Buffer.from(render());
  if (result.length !== workload.frameBytes) throw Error('frame exceeds byte budget');
  return result;
}
function summarize(records, intervalMs) {
  const rtts = records.map(r => r.receivedMs - r.sentMs), gaps = [], extras = [];
  let compressedIntervals = 0;
  for (let i = 1; i < records.length; i++) {
    const receivedGap = records[i].receivedMs - records[i - 1].receivedMs;
    const sentGap = records[i].sentMs - records[i - 1].sentMs;
    gaps.push(receivedGap); extras.push(Math.max(0, receivedGap - sentGap));
    // A diagnostic counter, not a new production pass threshold.
    if (sentGap >= intervalMs / 2 && receivedGap < sentGap / 5) compressedIntervals++;
  }
  const quantile = (xs, q) => xs.length ? [...xs].sort((a, b) => a - b)[Math.max(0, Math.ceil(xs.length * q) - 1)] : null;
  return { directionality: 'roundtrip-only', receivedSamples: records.length,
    firstEchoMs: records[0]?.receivedMs ?? null, rttP50Ms: quantile(rtts, .5), rttP95Ms: quantile(rtts, .95),
    roundtripVariationMs: rtts.length ? quantile(rtts, .95) - quantile(rtts, .05) : null,
    longestArrivalGapMs: gaps.length ? Math.max(...gaps) : null,
    maxExtraGapMs: extras.length ? Math.max(...extras) : null, compressedIntervals };
}
async function probeEcho(target, { port, workload, signal, connectTimeoutMs = 3000 } = {}) {
  validate(target, workload);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw Error('dedicated loopback proxy port required; direct fallback forbidden');
  if (!Number.isInteger(connectTimeoutMs) || connectTimeoutMs < 10 || connectTimeoutMs > 10000) throw Error('invalid CONNECT timeout');
  const started = performance.now(), id = randomUUID(), expected = [], sent = [], records = [];
  let request, socket, settled = false, timer, pacer, streamStarted, buffer = Buffer.alloc(0), receivedFrames = 0, bytes = 0, maxSendSlipMs = 0;
  return new Promise(resolve => {
    const finish = (reason, detail) => {
      if (settled) return; settled = true; clearTimeout(timer); clearTimeout(pacer); signal?.removeEventListener('abort', abort);
      const resources = [...new Set([socket, request?.socket, request].filter(r => r && !r.closed))];
      const closed = Promise.all(resources.map(r => new Promise(done => r.once('close', done))));
      socket?.destroy(); request?.destroy();
      closed.then(() => resolve({ ok: reason === 'complete', metricKind: 'local-sse-echo-prototype',
        reason, ...(detail ? { error: detail } : {}), failureScope: reason === 'complete' ? null : reason === 'cancelled' ? 'cancelled' : 'measurement',
        sseDownlinkVerified: false, qualityPass: null, accuracy: 'unvalidated',
        transport: 'CONNECT/TCP-echo (not HTTP SSE)', externalPathAttested: false,
        route: { proxyHost: '127.0.0.1', proxyPort: port, directFallback: false },
        sentSamples: Math.min(sent.length, workload.samples), receivedFrames, receivedBytes: bytes,
        terminalReceived: receivedFrames === workload.samples + 1, maxLocalSendSlipMs: maxSendSlipMs,
        metrics: summarize(records, workload.intervalMs), records,
        elapsedIncludingSocketCleanupMs: performance.now() - started }));
    };
    const abort = () => finish('cancelled', 'simulation cancelled; no quality verdict');
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) { abort(); return; }
    timer = setTimeout(() => finish('connect-timeout', 'proxy CONNECT timed out'), connectTimeoutMs);
    // Only this explicit local proxy is contacted. Target resolution/connection is
    // delegated through that lane; no direct retry, global proxy or DNS fallback.
    request = http.request({ host: '127.0.0.1', port, method: 'CONNECT', path: `${target.host}:${target.port}`, agent: false });
    request.once('error', () => finish('transport-error', 'proxy tunnel error (no node/source attribution)'));
    request.once('response', () => finish('proxy-rejected', 'CONNECT did not establish a tunnel'));
    request.once('connect', (response, raw, head) => {
      if (settled) { raw.destroy(); return; }
      socket = raw; socket.setNoDelay(true);
      socket.on('error', () => finish('transport-error', 'echo transport failed (no node/source attribution)'));
      socket.on('end', () => finish('incomplete', 'echo closed without all samples and terminal frame'));
      socket.on('close', () => finish('incomplete', 'echo tunnel closed before completion'));
      if (response.statusCode !== 200) { finish('proxy-rejected', `CONNECT HTTP ${response.statusCode}`); return; }
      if (head.length) { finish('integrity', 'unsolicited bytes before locally generated workload'); return; }
      clearTimeout(timer); streamStarted = performance.now();
      timer = setTimeout(() => finish('incomplete', 'echo did not finish within the explicit sample/drain budget'),
        workload.samples * workload.intervalMs + workload.drainMs);
      socket.on('data', chunk => {
        if (settled) return;
        const at = performance.now() - streamStarted; bytes += chunk.length;
        if (bytes > (workload.samples + 1) * workload.frameBytes) { finish('integrity', 'echo byte budget exceeded'); return; }
        buffer = Buffer.concat([buffer, chunk]);
        while (buffer.length >= workload.frameBytes && !settled) {
          const next = expected[receivedFrames];
          if (!next || !buffer.subarray(0, workload.frameBytes).equals(next)) { finish('integrity', 'echo is not the exact locally generated sequence/nonce/payload'); return; }
          buffer = buffer.subarray(workload.frameBytes);
          if (receivedFrames < workload.samples) records.push({ seq: receivedFrames, sentMs: sent[receivedFrames], receivedMs: at });
          receivedFrames++;
          if (receivedFrames === workload.samples + 1) finish(buffer.length ? 'integrity' : 'complete', buffer.length ? 'bytes after terminal frame' : undefined);
        }
      });
      const send = () => {
        if (settled) return;
        const seq = expected.length, at = performance.now() - streamStarted;
        maxSendSlipMs = Math.max(maxSendSlipMs, Math.abs(at - seq * workload.intervalMs));
        const payload = frame(id, seq, at, workload, seq === workload.samples);
        expected.push(payload); sent.push(at);
        if (!socket.write(payload)) { finish('backpressure', 'local write backpressure; no quality verdict'); return; }
        // Do not manufacture a catch-up burst after a locally delayed callback.
        if (seq < workload.samples) pacer = setTimeout(send, workload.intervalMs);
      };
      send();
    });
    request.end();
  });
}
async function runLocal(job, { signal: parentSignal = new AbortController().signal, emit = () => {}, startCoreFn = startCore } = {}) {
  const started = performance.now(); validate(job.target, job.workload);
  const nodes = job.nodes, concurrency = job.concurrency, wallBudgetMs = job.wallBudgetMs ?? 60000;
  if (!Array.isArray(nodes) || !nodes.length || nodes.length > 1000 || nodes.some(n => typeof n.key !== 'string' || !n.key)
    || new Set(nodes.map(n => n.key)).size !== nodes.length) throw Error('complete unique node catalog required');
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 256
    || !Number.isInteger(wallBudgetMs) || wallBudgetMs < 100 || wallBudgetMs > 60000) throw Error('explicit bounded concurrency and <=60000ms outer budget required');
  if (nodes.length * (job.workload.samples + 1) * job.workload.frameBytes * 2 > 64 * 1024 * 1024) throw Error('simulation roundtrip payload budget exceeds 64MiB');
  if (!job.config && (nodes.some(n => !Number.isInteger(n.port) || n.port < 1 || n.port > 65535)
    || new Set(nodes.map(n => n.port)).size !== nodes.length)) throw Error('each node needs its own explicit proxy port; no shared/direct lane');
  const owned = new AbortController(), cancel = () => owned.abort(), signal = owned.signal;
  setMaxListeners(258, signal); parentSignal.addEventListener('abort', cancel, { once: true });
  if (parentSignal.aborted) cancel();
  let core, cursor = 0, budgetExpired = false, roundFailure;
  const workers = [];
  const outcomes = new Map(nodes.map(n => [n.key, { key: n.key, started: false, reason: 'unmeasured', qualityPass: null, sseDownlinkVerified: false }]));
  const timer = setTimeout(() => { budgetExpired = true; emit({ type: 'log', message: 'simulation outer deadline: cancel active lanes, retain all unmeasured nodes, reserve cleanup time' }); cancel(); }, wallBudgetMs - Math.min(2500, wallBudgetMs / 3));
  const lag = monitorEventLoopDelay({ resolution: 10 }); lag.enable();
  try {
    const supported = job.config ? new Set((job.config.outbounds || []).filter(o => !['selector', 'urltest', 'direct', 'block'].includes(o.type)).map(o => o.tag)) : null;
    const eligible = supported ? nodes.filter(n => supported.has(n.tag)) : nodes;
    for (const node of nodes.filter(n => !eligible.includes(n))) {
      outcomes.set(node.key, { ...outcomes.get(node.key), reason: 'unsupported', error: 'no explicit proxy outbound for cached node' });
      emit({ type: 'log', message: `simulation unsupported node: ${node.key}; no fallback or fabricated result` });
    }
    if (!signal.aborted && job.config && eligible.length) core = await startCoreFn({ ...job, nodes: eligible }, signal, message => emit({ type: 'log', message }));
    const worker = async () => {
      while (cursor < eligible.length && !signal.aborted) {
        const i = cursor++, node = eligible[i];
        const result = await probeEcho(job.target, { port: core ? core.ports[i] : node.port, workload: job.workload, signal });
        outcomes.set(node.key, { key: node.key, started: true, ...result });
        emit({ type: 'progress', key: node.key, reason: result.reason, received: result.metrics.receivedSamples });
        if (!result.ok) emit({ type: 'log', message: `simulation ${node.key}: ${result.reason}; no SSE/downlink quality verdict` });
      }
    };
    for (let i = 0; i < Math.min(concurrency, eligible.length); i++) {
      workers.push(worker()); if ((i + 1) % 8 === 0) await new Promise(setImmediate);
    }
    await Promise.all(workers);
  } catch (error) {
    roundFailure = error.message; emit({ type: 'log', message: `simulation setup/run failed: ${roundFailure}` }); cancel();
  } finally {
    clearTimeout(timer); parentSignal.removeEventListener('abort', cancel);
    await Promise.allSettled(workers);
    try { await core?.cleanup(); } finally { lag.disable(); }
  }
  const elapsedIncludingCleanupMs = performance.now() - started;
  const rows = [...outcomes.values()];
  return { type: 'local-sse-prototype-result', acceptance: false, sseDownlinkVerified: false, accuracy: 'unvalidated',
    ...(roundFailure ? { roundFailure } : {}), scope: 'local scheduling and bidirectional SSE-payload echo only; not HTTP SSE or a WAN acceptance test',
    nodeCount: nodes.length, startedNodes: rows.filter(r => r.started).length, completeEchoNodes: rows.filter(r => r.ok).length,
    cancelled: signal.aborted, budgetExpired, elapsedIncludingCleanupMs, withinBudget: elapsedIncludingCleanupMs <= wallBudgetMs,
    maxLocalEventLoopLagMs: Math.max(0, lag.max / 1e6 - 10), outcomes: rows };
}
async function cli() {
  const i = process.argv.indexOf('--job');
  if (i < 0 || !process.argv[i + 1]) throw Error('usage: node scripts/local-sse-prototype.cjs --job EXPLICIT_JOB.json; no default target or public test');
  const job = JSON.parse(await fs.readFile(process.argv[i + 1], 'utf8'));
  const controller = new AbortController(), cancel = () => controller.abort(), emit = value => process.stdout.write(JSON.stringify(value) + '\n');
  process.on('SIGINT', cancel); process.on('SIGTERM', cancel);
  try { emit(await runLocal(job, { signal: controller.signal, emit })); }
  finally { process.removeListener('SIGINT', cancel); process.removeListener('SIGTERM', cancel); }
}
module.exports = { validate, frame, summarize, probeEcho, runLocal };
if (require.main === module) cli().catch(error => { console.error(error.message); process.exitCode = 1; });
