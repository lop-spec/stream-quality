import http from 'node:http';
import { Readable } from 'node:stream';
import fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { handle } from './worker.mjs';
const assets = new Map([['/', 'web/index.html'], ['/app.js', 'web/app.js'], ['/style.css', 'web/style.css'], ['/stream-quality.js', 'stream-quality.js']]);
export function createServer() {
  return http.createServer(async (req, res) => {
    try {
      req.socket.setNoDelay(true);
      const path = (req.url || '/').split('?')[0];
      if (assets.has(path) && req.method === 'GET') {
        const data = await fs.readFile(new URL(assets.get(path), import.meta.url));
        res.writeHead(200, { 'content-type': path.endsWith('.js') ? 'text/javascript' : path.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8' });
        res.end(data); return;
      }
      const request = new Request('http://localhost' + req.url, { method: req.method });
      const response = handle(request, process.env.STREAM_QUALITY_LOCATION || 'self-hosted');
      res.writeHead(response.status, Object.fromEntries(response.headers)); res.flushHeaders();
      if (!response.body) { res.end(); return; }
      const body = Readable.fromWeb(response.body);
      body.on('error', () => res.destroy());
      res.on('close', () => body.destroy()); body.pipe(res);
    } catch (error) { console.error('stream-quality server failure:', error.message); if (!res.headersSent) res.writeHead(500); res.end(); }
  });
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const server = createServer();
  server.listen(Number(process.env.PORT || 8788), process.env.HOST || '127.0.0.1', () => console.log('Stream Quality listening', server.address()));
}
