import './stream-quality.js';
const SQ = globalThis.StreamQuality;
const P = SQ.PROFILE;
const encoder = new TextEncoder();
const noise = new Uint8Array(65536);
let seed = 0x19af0731;
for (let i = 0; i < noise.length; i++) { seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5; noise[i] = seed & 255; }
const clients = new Map();
let active = 0;
function headers(type) {
  return { 'content-type': type, 'cache-control': 'no-store, no-transform', 'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, OPTIONS', 'access-control-expose-headers': 'x-stream-quality-profile, x-stream-quality-location, content-length',
    'x-content-type-options': 'nosniff', 'x-stream-quality-profile': P.id };
}
export function handle(request, location = 'unspecified') {
  const url = new URL(request.url);
  if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: headers('text/plain') });
  if (request.method !== 'GET') return new Response('GET only', { status: 405, headers: headers('text/plain') });
  if (url.search) return new Response('Fixed profile: query parameters not accepted', { status: 400, headers: headers('text/plain') });
  if (url.pathname === '/api/manifest') return Response.json({ protocol: 'stream-quality-v1', profile: P, location,
    privacy: 'No model, cookies, subscriptions or credentials required. Source workload only; not OpenAI routing.' }, { headers: headers('application/json') });
  if (!['/api/stream', '/api/download'].includes(url.pathname)) return new Response('Stream Quality endpoint. See /api/manifest', { status: 404, headers: headers('text/plain') });
  const ip = request.headers.get('cf-connecting-ip') || 'local', now = Date.now();
  for (const [key, entry] of clients) if (now - entry.at >= 60000) clients.delete(key);
  const client = clients.get(ip) || { at: now, requests: 0 };
  if (client.requests >= 40 || active >= 8 || clients.size >= 4096 && !clients.has(ip)) {
    console.warn('stream-quality rate limit: source busy; not a node failure');
    return new Response('Source rate limited; retry later', { status: 429, headers: { ...headers('text/plain'), 'retry-after': '60' } });
  }
  client.requests++; clients.set(ip, client); active++;
  let settled = false, timer, wake;
  const finish = () => { if (settled) return; settled = true; clearTimeout(timer); wake?.(); active--; };
  const h = { ...headers(url.pathname === '/api/stream' ? 'text/event-stream; charset=utf-8' : 'application/octet-stream'),
    'x-stream-quality-location': location, 'content-encoding': 'identity' };
  if (url.pathname === '/api/download') {
    h['content-length'] = String(P.downloadBytes);
    let remaining = P.downloadBytes;
    return new Response(new ReadableStream({
      pull(controller) {
        if (settled) return;
        const length = Math.min(noise.length, remaining);
        controller.enqueue(noise.slice(0, length)); remaining -= length;
        if (!remaining) { controller.close(); finish(); }
      }, cancel: finish
    }), { headers: h });
  }
  const body = new ReadableStream({
    start(controller) {
      const start = performance.now();
      const pump = async () => {
        try {
          for (let i = 0; i < P.samples && !settled; i++) {
            const wait = i * P.intervalMs - (performance.now() - start);
            if (wait > 0) await new Promise(resolve => { wake = resolve; timer = setTimeout(resolve, wait); });
            if (settled) return;
            controller.enqueue(encoder.encode(SQ.frame(i, performance.now() - start)));
          }
          if (!settled) { controller.enqueue(encoder.encode(SQ.endFrame())); controller.close(); finish(); }
        } catch (error) { console.error('stream-quality source failed:', error.name); controller.error(error); finish(); }
      };
      void pump();
    }, cancel: finish
  });
  return new Response(body, { headers: h });
}
export default { fetch(request) { return handle(request, request.cf?.colo ? `cf:${request.cf.colo}` : 'cf:unknown'); } };
