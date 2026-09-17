'use strict';
// Test-only local TCP echo behind distinct HTTP CONNECT relays. No WAN or credentials.
const net = require('node:net'), http = require('node:http');
const servers = [], sockets = new Set(), timers = new Set(), stats = {};
const own = socket => { sockets.add(socket); socket.on('error', () => {}); socket.once('end', () => socket.destroy()); socket.once('close', () => sockets.delete(socket)); return socket; };
const listen = server => new Promise((resolve, reject) => { servers.push(server); server.once('error', reject); server.listen(0, '127.0.0.1', () => resolve(server.address().port)); });
const later = (fn, ms) => { const t = setTimeout(() => { timers.delete(t); fn(); }, ms); timers.add(t); return t; };
async function start(m) {
  const echoPort = await listen(net.createServer(socket => { own(socket); socket.on('data', b => socket.write(b)); }));
  const ports = [];
  for (let index = 0; index < m.modes.length; index++) {
    const mode = m.modes[index], state = stats[index] = { mode, connects: 0, upstreamBytes: 0, downstreamBytes: 0, targets: [] };
    const proxy = http.createServer((_req, res) => { res.writeHead(405); res.end(); });
    proxy.on('connection', own);
    proxy.on('connect', (req, client, head) => {
      state.connects++; state.targets.push(req.url);
      if (req.url !== 'echo.fixture.invalid:7') { client.end('HTTP/1.1 403 Forbidden\r\n\r\n'); return; }
      client.resume(); // Consume EOF even when intentionally withholding CONNECT response.
      if (mode === 'connect-hang') return;
      const upstream = own(net.connect(echoPort, '127.0.0.1'));
      let buffered = [], count = 0, held = false, stalled = false;
      const deliver = b => { if (!client.destroyed) { state.downstreamBytes += b.length; client.write(b); } };
      const release = () => { held = false; const all = Buffer.concat(buffered); buffered = []; if (all.length) deliver(all); };
      client.once('close', () => upstream.destroy()); upstream.once('close', () => client.destroy());
      upstream.once('connect', () => {
        client.write('HTTP/1.1 200 Connection Established\r\n\r\n');
        if (head.length) upstream.write(head);
        client.on('data', b => { state.upstreamBytes += b.length; upstream.write(b); });
      });
      upstream.on('data', b => {
        count += b.length;
        if (mode === 'silent') return;
        if (mode === 'http') { client.end('HTTP/1.1 204 No Content\r\n\r\n'); return; }
        if (mode === 'cut' && count >= 5 * m.frameBytes) { client.destroy(); upstream.destroy(); return; }
        if (mode === 'corrupt') { b = Buffer.from(b); b[0] ^= 1; }
        if (mode === 'batch') {
          buffered.push(b);
          if (count >= (m.samples + 1) * m.frameBytes) release();
          return;
        }
        if (mode === 'stall' && !stalled && count >= 5 * m.frameBytes) {
          stalled = held = true; later(release, 200);
        }
        if (held) buffered.push(b); else deliver(b);
      });
    });
    ports.push(await listen(proxy));
  }
  process.send({ type: 'ready', ports });
}
async function stop() {
  for (const t of timers) clearTimeout(t); timers.clear();
  for (const socket of sockets) socket.destroy();
  await Promise.all(servers.map(server => new Promise(resolve => server.close(resolve))));
  if (process.connected) process.disconnect();
}
process.on('message', message => {
  if (message.type === 'start') start(message).catch(e => { console.error(e.message); process.exitCode = 1; stop(); });
  if (message.type === 'stats') process.send({ type: 'stats', stats, openSockets: sockets.size });
  if (message.type === 'stop') stop();
});
process.on('disconnect', () => { for (const socket of sockets) socket.destroy(); for (const server of servers) server.close(); for (const timer of timers) clearTimeout(timer); });
