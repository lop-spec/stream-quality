// Executes the actual Worker source with a virtual clock. Not a Cloudflare runtime emulator
// and never evidence of physical 20s timing or deployed cancellation behavior.
const fs = require('node:fs'), vm = require('node:vm');
const SQ = require('../../stream-quality.js');
function workerClock({ downloadFault = false, frameFault = false, omitTerminal = false } = {}) {
  let now = 0, next = 0;
  const timers = new Map(), logs = [];
  class ClockDate extends Date { static now() { return 1800000000000 + now; } }
  class Bytes extends Uint8Array { slice(...args) { if (downloadFault) throw Error('injected source buffer failure'); return super.slice(...args); } }
  const context = vm.createContext({ StreamQuality: { ...SQ,
    frame: (...args) => { if (frameFault && args[0] === 3) throw Error('injected source encoder failure'); return SQ.frame(...args); },
    endFrame: (...args) => omitTerminal ? '' : SQ.endFrame(...args) },
    Uint8Array: Bytes, TextEncoder, ReadableStream, Request, Response, URL, Date: ClockDate,
    performance: { now: () => now }, console: { warn: (...x) => logs.push(x.join(' ')), error: (...x) => logs.push(x.join(' ')) },
    setTimeout: (callback, delay) => { const id = ++next; timers.set(id, { at: now + delay, callback }); return id; },
    clearTimeout: id => timers.delete(id) });
  const source = fs.readFileSync(require.resolve('../../worker.mjs'), 'utf8')
    .replace("import './stream-quality.js';", '').replace('export const LIMITS', 'const LIMITS')
    .replace('export function handle', 'function handle').replace(/^export default .*$/m, '');
  vm.runInContext(source + '\nthis.fixture = {handle, LIMITS, inspect: () => JSON.stringify({stream: quotas.stream.leases.size, download: quotas.download.leases.size})};', context);
  return {
    logs, limits: context.fixture.LIMITS,
    inspect: () => ({ ...JSON.parse(context.fixture.inspect()), pendingTimers: timers.size }),
    request: (kind = 'stream', ip = 'fixture') => context.fixture.handle(new Request(`https://fixture.test/api/${kind}`, { headers: { 'cf-connecting-ip': ip } }), 'fixture'),
    async advanceTo(target) {
      for (;;) {
        await Promise.resolve(); await Promise.resolve();
        const entry = [...timers].sort((a, b) => a[1].at - b[1].at)[0];
        if (!entry || entry[1].at > target) break;
        const [id, task] = entry; timers.delete(id); now = Math.max(now, task.at); task.callback();
      }
      now = target; await Promise.resolve(); await Promise.resolve();
    },
    // Deliberately abandons request tasks WITHOUT invoking cancel/finally. This is
    // an explicit fault model, not a claim that a deployed runtime does so.
    abandonRequestTasks: () => timers.clear(),
    delayCallbacksUntil: target => { now = target; }
  };
}
module.exports = { workerClock };
