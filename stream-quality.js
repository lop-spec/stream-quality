/* Stream Quality v1 — deterministic workload, no model calls. MIT. */
(function (root, factory) {
  const api = factory();
  root.StreamQuality = api;
  if (typeof module === 'object' && module.exports) module.exports = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const PROFILE = Object.freeze({ id: 'sq-v1-256b-50ms-20s-8mib', frameBytes: 256, intervalMs: 50,
    samples: 401, durationMs: 20000, downloadBytes: 8 * 1024 * 1024, timeoutMs: 30000,
    sourceMaxSlipMs: 250, jitterPassMs: 100, gapPassMs: 500, burstPassRatio: 0.1 });
  const encoder = new TextEncoder();
  const size = text => encoder.encode(text).length;
  const round = n => Math.round(n * 10) / 10;
  function percentile(values, q) {
    if (!values.length) return 0;
    const xs = [...values].sort((a, b) => a - b);
    return xs[Math.max(0, Math.ceil(q * xs.length) - 1)];
  }
  function frame(seq, sentMs, profile = PROFILE) {
    const data = { seq, sentMs: round(sentMs), scheduledMs: seq * profile.intervalMs, pad: '' };
    const render = () => `event: sample\ndata: ${JSON.stringify(data)}\n\n`;
    data.pad = 'x'.repeat(Math.max(0, profile.frameBytes - size(render())));
    const result = render();
    if (size(result) !== profile.frameBytes) throw Error('frame size exceeds profile');
    return result;
  }
  const endFrame = (profile = PROFILE) => `event: end\ndata: ${JSON.stringify({ profile: profile.id, samples: profile.samples })}\n\n`;
  function analyzer(profile = PROFILE) {
    const decoder = new TextDecoder('utf-8', { fatal: true });
    let buffer = '', ended = false, error = '', bytes = 0;
    const events = [];
    function block(text, at) {
      if (!text.trim()) return;
      const lines = text.split('\n');
      const type = lines.find(l => l.startsWith('event:'))?.slice(6).trim();
      const data = JSON.parse(lines.filter(l => l.startsWith('data:')).map(l => l.slice(5).trimStart()).join('\n'));
      if (ended) throw Error('event after terminal');
      if (type === 'sample') {
        if (events.length >= profile.samples || data.seq !== events.length || !Number.isFinite(data.sentMs)
          || data.sentMs < 0 || data.scheduledMs !== data.seq * profile.intervalMs
          || events.length && data.sentMs < events.at(-1).sentMs
          || size(text + '\n\n') !== profile.frameBytes) throw Error('invalid sequence, timing or frame size');
        events.push({ seq: data.seq, sentMs: data.sentMs, at });
      } else if (type === 'end') {
        if (data.profile !== profile.id || data.samples !== profile.samples || events.length !== profile.samples) throw Error('incomplete stream');
        ended = true;
      } else throw Error('unknown SSE event');
    }
    return {
      push(chunk, at) {
        if (error) return;
        try {
          bytes += chunk.byteLength;
          if (bytes > profile.samples * profile.frameBytes + 4096) throw Error('stream exceeds byte budget');
          buffer += decoder.decode(chunk, { stream: true });
          let i;
          while ((i = buffer.indexOf('\n\n')) >= 0) { block(buffer.slice(0, i), at); buffer = buffer.slice(i + 2); }
          if (buffer.length > 4096) throw Error('SSE frame exceeds bound');
        } catch (e) { error = e.message; }
      },
      get error() { return error; },
      result() {
        if (error || !ended || buffer.trim()) return { ok: false, failureScope: 'measurement', error: error || 'missing complete terminal event' };
        const first = events[0], last = events.at(-1), extras = [], queues = [], gaps = [], slips = [];
        let bursts = 0;
        for (let i = 0; i < events.length; i++) {
          const e = events[i];
          slips.push(Math.abs(e.sentMs - e.seq * profile.intervalMs));
          queues.push((e.at - first.at) - (e.sentMs - first.sentMs));
          if (i) {
            const received = e.at - events[i - 1].at, sent = e.sentMs - events[i - 1].sentMs;
            gaps.push(received); extras.push(Math.max(0, received - sent));
            if (received < 5 && sent >= profile.intervalMs / 2) bursts++;
          }
        }
        const sourceSlipMs = Math.max(...slips);
        if (sourceSlipMs > profile.sourceMaxSlipMs) return { ok: false, failureScope: 'endpoint',
          error: 'test source missed its schedule; not a node failure', sourceSlipMs: round(sourceSlipMs) };
        const jitterMs = round(percentile(queues, .95) - percentile(queues, .05));
        const maxExtraGapMs = round(Math.max(...extras)), burstRatio = bursts / (events.length - 1);
        return { ok: true, firstSampleMs: round(first.at), deliveryMs: round(last.at - first.at),
          jitterMs, p95ExtraGapMs: round(percentile(extras, .95)), maxExtraGapMs,
          longestGapMs: round(Math.max(...gaps)), stallCount: extras.filter(n => n > profile.gapPassMs).length,
          tailGrowthMs: round(Math.max(0, queues.at(-1))), burstRatio: round(burstRatio * 1000) / 1000,
          sourceSlipMs: round(sourceSlipMs), receivedSamples: events.length, streamBytes: bytes,
          flowPass: jitterMs <= profile.jitterPassMs && maxExtraGapMs <= profile.gapPassMs && burstRatio <= profile.burstPassRatio };
      }
    };
  }
  function endpoint(value, allowHttp = false) {
    const url = new URL(value);
    if (url.username || url.password || url.search || url.hash || (url.protocol !== 'https:' && !(allowHttp && url.protocol === 'http:'))) {
      throw Error('endpoint must be HTTPS without credentials, query or fragment');
    }
    return url.href.replace(/\/+$/, '');
  }
  function profileKey(base, location = '', includeDownload = true) { return `${PROFILE.id}|${base}|${location}${includeDownload ? '' : '|sse-only'}`; }
  function isResult(r) { return r?.metricKind === 'stream-quality-v1' && r.ok === true && r.stream?.ok === true
    && (r.measurement === 'sse-only' || r.download?.ok === true); }
  function compare(a, b) {
    // Fixed-rate flows that meet the same limits are tied. Bandwidth is separate.
    return Number(!!b?.stream?.flowPass) - Number(!!a?.stream?.flowPass)
      || (a?.stream?.flowPass && b?.stream?.flowPass ? 0 : (a?.stream?.jitterMs ?? Infinity) - (b?.stream?.jitterMs ?? Infinity));
  }
  async function browserProbe(base, { signal, onProgress = () => {}, includeDownload = false } = {}) {
    base = endpoint(base, new URL(base).hostname === '127.0.0.1' || new URL(base).hostname === 'localhost');
    const controller = new AbortController();
    const abort = () => controller.abort(); signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    let timer;
    const request = async path => {
      clearTimeout(timer); timer = setTimeout(abort, PROFILE.timeoutMs);
      return fetch(base + path, { signal: controller.signal, cache: 'no-store', credentials: 'omit', redirect: 'error' });
    };
    try {
      const mr = await request('/api/manifest');
      if (!mr.ok) throw Error(`manifest HTTP ${mr.status}`);
      const manifest = await mr.json();
      if (manifest.profile?.id !== PROFILE.id || Object.keys(PROFILE).some(key => manifest.profile[key] !== PROFILE[key])) throw Error('incompatible endpoint profile');
      const checkHeaders = response => {
        if (response.headers.get('x-stream-quality-profile') !== PROFILE.id || response.headers.get('x-stream-quality-location') !== manifest.location) throw Error('endpoint profile/location changed');
        if (response.headers.get('content-encoding') && response.headers.get('content-encoding') !== 'identity') throw Error('compressed workload rejected');
      };
      onProgress('stream');
      const start = performance.now(), response = await request('/api/stream'), parser = analyzer();
      if (!response.ok || !response.headers.get('content-type')?.startsWith('text/event-stream')) throw Error(`SSE HTTP ${response.status}`);
      checkHeaders(response);
      const reader = response.body.getReader();
      while (true) { const { done, value } = await reader.read(); if (done) break; parser.push(value, performance.now() - start); if (parser.error) { await reader.cancel(); throw Error(parser.error); } }
      const stream = parser.result();
      if (!stream.ok) return { ...stream, stream };
      const result = { ok: true, metricKind: 'stream-quality-v1', measurement: includeDownload ? 'sse-and-download' : 'sse-only',
        endpoint: base, location: manifest.location || 'unspecified', profileKey: profileKey(base, manifest.location, includeDownload), measuredAt: Date.now(), stream };
      if (!includeDownload) return result;
      onProgress('download');
      const ds = performance.now(), dr = await request('/api/download');
      if (!dr.ok || dr.headers.get('x-stream-quality-profile') !== PROFILE.id) throw Error(`download HTTP ${dr.status} / profile mismatch`);
      checkHeaders(dr);
      let bytes = 0, firstAt = null;
      const rd = dr.body.getReader();
      while (true) { const { done, value } = await rd.read(); if (done) break; firstAt ??= performance.now(); bytes += value.byteLength; if (bytes > PROFILE.downloadBytes) { await rd.cancel(); throw Error('download exceeds byte budget'); } }
      const finished = performance.now();
      if (bytes !== PROFILE.downloadBytes) throw Error('download truncated');
      const transferMs = Math.max(.1, finished - firstAt), elapsedMs = finished - ds;
      return { ...result,
        download: { ok: true, bytes, elapsedMs: round(elapsedMs), transferMs: round(transferMs),
          mbps: round(bytes * 8 / transferMs / 1000), endToEndMbps: round(bytes * 8 / elapsedMs / 1000), shortSample: transferMs < 1000 } };
    } finally { clearTimeout(timer); signal?.removeEventListener('abort', abort); controller.abort(); }
  }
  return { PROFILE, frame, endFrame, analyzer, percentile, endpoint, profileKey, isResult, compare, browserProbe };
});
