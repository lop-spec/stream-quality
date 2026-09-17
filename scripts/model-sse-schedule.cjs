// Offline interval model, not a network runner or an accuracy/60s acceptance test.
// Each task is one unchanged 401-frame, 20s observation. Manifests, TLS, jitter
// and cleanup are deliberately excluded from the workload floor, never hidden
// inside a claimed real wall time. No production settings or scheduler changes.
const SQ = require('../stream-quality.js');
function model({ nodes = 173, rounds = 3, concurrency = 173, serializeNode = true, startupMs = 0, cleanupMs = 0 } = {}) {
  if (![nodes, rounds, concurrency].every(n => Number.isInteger(n) && n > 0 && n <= 1000) || rounds !== 3 ||
      ![startupMs, cleanupMs].every(n => Number.isFinite(n) && n >= 0)) throw Error('invalid scheduling model inputs');
  const slots = Array(concurrency).fill(startupMs), ready = Array(nodes).fill(startupMs), tasks = [];
  for (let round = 1; round <= rounds; round++) for (let node = 0; node < nodes; node++) {
    let slot = 0; for (let i = 1; i < slots.length; i++) if (slots[i] < slots[slot]) slot = i;
    const start = Math.max(slots[slot], serializeNode ? ready[node] : startupMs), end = start + SQ.PROFILE.durationMs;
    tasks.push({ node, round, start, end, samples: SQ.PROFILE.samples }); slots[slot] = end; ready[node] = end;
  }
  const events = tasks.flatMap(t => [[t.start, 1], [t.end, -1]]).sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  let active = 0, peak = 0; for (const [, delta] of events) { active += delta; peak = Math.max(peak, active); }
  const exposure = ready.map((_, node) => {
    let through = -Infinity, total = 0;
    for (const t of tasks.filter(t => t.node === node).sort((a, b) => a.start - b.start)) {
      total += Math.max(0, t.end - Math.max(through, t.start)); through = Math.max(through, t.end);
    }
    return total;
  });
  return { kind: 'offline-model-not-acceptance', concurrency, serializeNode, tasks,
    totalTasks: tasks.length, totalSampleFrames: tasks.length * SQ.PROFILE.samples, peak,
    workloadFloorMs: Math.max(...slots) - startupMs, modeledWallMs: Math.max(...slots) + cleanupMs,
    exposurePerNodeMs: { min: Math.min(...exposure), max: Math.max(...exposure) } };
}
function summaries() {
  return [[173, true], [256, false], [260, false], [519, false]].map(([concurrency, serializeNode]) => {
    const { tasks, ...summary } = model({ concurrency, serializeNode });
    return { ...summary, remainingLifecycleBudgetMs: 60000 - summary.workloadFloorMs };
  });
}
module.exports = { model, summaries };
if (require.main === module) console.log(JSON.stringify({
  warning: 'Synthetic lower bounds only. Overlap is NOT enabled and can miss faults that serial windows detect.',
  runnerConcurrencyLimitUnchanged: 256, profile: SQ.PROFILE.id, plans: summaries()
}, null, 2));
