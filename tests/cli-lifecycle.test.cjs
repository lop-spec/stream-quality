const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs/promises'), path = require('node:path'), os = require('node:os'), http = require('node:http');
const { spawn } = require('node:child_process');

async function heldInputJob(t, job, onEvent = () => {}) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'sq-cli-lifecycle-')), jobPath = path.join(dir, 'job.json');
  await fs.writeFile(jobPath, JSON.stringify(job));
  // Neutralino retains this write end until its spawnedProcess exit event.
  const child = spawn(process.execPath, [path.join(__dirname, '../stream-quality-runner.cjs'), '--job', jobPath], {
    windowsHide: true, stdio: ['pipe', 'pipe', 'pipe']
  });
  const events = []; let buffer = '', stderr = '', deadline, terminalAt;
  const exited = new Promise(resolve => child.once('close', (code, signal) => resolve({ code, signal })));
  t.after(async () => { clearTimeout(deadline); if (child.exitCode === null) child.kill(); await exited; await fs.unlink(jobPath); await fs.rmdir(dir); });
  child.stdout.on('data', chunk => {
    buffer += chunk; let i;
    while ((i = buffer.indexOf('\n')) >= 0) {
      const event = JSON.parse(buffer.slice(0, i)); buffer = buffer.slice(i + 1); events.push(event);
      if (event.type === 'result') terminalAt = Date.now();
      onEvent(event, child);
    }
  });
  child.stderr.on('data', chunk => { stderr += chunk; });
  const result = await Promise.race([exited, new Promise((_, reject) => {
    deadline = setTimeout(() => reject(Error(`worker did not exit with parent stdin open; resultSeen=${!!terminalAt}`)), 4000);
  })]);
  clearTimeout(deadline);
  assert.equal(stderr, ''); assert.equal(result.signal, null); assert.ok(terminalAt);
  assert.ok(Date.now() - terminalAt < 1500, 'terminal output must be followed by natural exit, not a supervisor timeout');
  assert.equal(events.filter(e => e.type === 'result').length, 1);
  return { result, terminal: events.find(e => e.type === 'result') };
}

test('CLI finishes a valid job while the parent keeps its stdin pipe open', { timeout: 6000 }, async t => {
  const { result, terminal } = await heldInputJob(t, { endpoint: 'https://unused.invalid', rounds: 1,
    nodes: [{ key: 'fixture', tag: 'unsupported' }], config: { outbounds: [] } });
  assert.equal(result.code, 0); assert.equal(terminal.ok, true);
  assert.equal(terminal.outcomes[0].value.failureScope, 'unsupported');
});

test('CLI validation failure also releases the control pipe without losing error output', { timeout: 6000 }, async t => {
  const { result, terminal } = await heldInputJob(t, { endpoint: 'https://unused.invalid', rounds: 2, nodes: [{ key: 'fixture' }] });
  assert.equal(result.code, 1); assert.equal(terminal.ok, false); assert.equal(terminal.failureScope, 'round');
});

test('CLI endpoint failure clears the queue and exits with parent control pipe open', { timeout: 6000 }, async t => {
  const server = http.createServer((_req, res) => { res.writeHead(503); res.end('fixture unavailable'); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const { result, terminal } = await heldInputJob(t, { endpoint: `http://127.0.0.1:${server.address().port}`, rounds: 3,
    nodes: [{ key: 'fixture-a', port: 0 }, { key: 'fixture-b', port: 0 }] });
  assert.equal(result.code, 0); assert.equal(terminal.channelFailure.failureScope, 'endpoint');
  assert.equal(terminal.outcomes.length, 2); assert.ok(terminal.outcomes.every(o => o.value.failureScope === 'endpoint'));
});

test('CLI cancellation releases stdin and the active measurement socket', { timeout: 6000 }, async t => {
  let response, requested;
  const received = new Promise(resolve => { requested = resolve; });
  const server = http.createServer((_req, res) => { response = res; requested(); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const { result, terminal } = await heldInputJob(t, { endpoint: `http://127.0.0.1:${server.address().port}`, rounds: 1,
    nodes: [{ key: 'fixture', port: 0 }] }, (event, child) => {
    if (event.type === 'start') void received.then(() => child.stdin.write('{"action":"cancel"}\n'));
  });
  assert.equal(result.code, 0); assert.equal(terminal.cancelled, true); assert.equal(response.destroyed, true);
});
