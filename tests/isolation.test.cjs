const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { isolatedConfig, run, request } = require('../stream-quality-runner.cjs');
test('all selected nodes receive dedicated loopback routes; daily TUN/controller/rules/urltest removed', () => {
  const nodes = Array.from({ length: 179 }, (_, i) => ({ key: String(i), tag: 'n' + i }));
  const source = { inbounds: [{ type: 'tun' }], experimental: { clash_api: { external_controller: '127.0.0.1:9090' } },
    outbounds: [...nodes.map(n => ({ type: 'direct', tag: n.tag })), { type: 'urltest', tag: 'daily-group', outbounds: ['n0'] }],
    dns: { servers: [{ type: 'local', tag: 'dns' }], final: 'dns' }, route: { rules: [{ outbound: 'daily-group' }], rule_set: [{ type: 'remote', tag: 'private' }] } };
  const before = JSON.stringify(source), config = isolatedConfig({ nodes, config: source }, nodes.map((_, i) => 30000 + i));
  assert.equal(JSON.stringify(source), before); assert.equal(config.inbounds.length, 179); assert.equal(config.outbounds.length, 179);
  assert.equal(config.experimental, undefined); assert.equal(config.route.rule_set, undefined); assert.equal(config.outbounds.some(o => o.type === 'urltest'), false);
  for (let i = 0; i < nodes.length; i++) { assert.equal(config.inbounds[i].listen, '127.0.0.1'); assert.equal(config.route.rules[i].outbound, nodes[i].tag); assert.equal(config.route.rules[i].inbound[0], config.inbounds[i].tag); }
});
test('missing protocol remains an explicit unsupported outcome, not silently absent', async () => {
  const events = [], result = await run({ endpoint: 'https://example.com', config: { outbounds: [] }, nodes: [{ key: 'unsupported', tag: 'none' }] }, { emit: e => events.push(e) });
  assert.equal(result.outcomes.length, 1); assert.equal(result.outcomes[0].value.failureScope, 'unsupported'); assert.ok(events.some(e => e.type === 'log' && /unsupported/.test(e.message)));
});
test('cancelling during HTTPS CONNECT/TLS negotiation closes the owned socket', { timeout: 3000 }, async t => {
  let peer, connected;
  const ready = new Promise(resolve => { connected = resolve; });
  const server = http.createServer();
  server.on('connect', (_req, socket) => { peer = socket; socket.write('HTTP/1.1 200 Connection Established\r\n\r\n'); socket.on('data', () => {}); socket.on('end', () => socket.end()); socket.on('error', error => assert.equal(error.code, 'ECONNRESET')); connected(); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve)); t.after(() => { peer?.destroy(); server.close(); });
  const controller = new AbortController();
  const pending = request('https://example.test', '/', { port: server.address().port, signal: controller.signal, maxBytes: 1024 });
  await ready;
  const closed = new Promise(resolve => peer.once('close', () => resolve(true)));
  const checked = assert.rejects(pending, e => e.failureScope === 'cancelled');
  controller.abort(); await checked;
  const result = await Promise.race([closed, new Promise(resolve => setTimeout(() => resolve(false), 700))]);
  assert.equal(result, true);
});
