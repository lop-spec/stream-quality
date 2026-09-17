const {test}=require('node:test'),assert=require('node:assert/strict'),SQ=require('../stream-quality.js');
const analyze=(p,delay)=>{const a=SQ.analyzer(p);for(let i=0;i<p.samples;i++)a.push(Buffer.from(SQ.frame(i,i*p.intervalMs,p)),i*p.intervalMs+delay(i));a.push(Buffer.from(SQ.endFrame(p)),p.durationMs+delay(p.samples-1));return a.result()};
test('a 3s sample provably misses a late stall caught by the unchanged 20s profile',()=>{
 const delayed=i=>100+(i>=240?1000:0),full=analyze(SQ.PROFILE,delayed);
 const short=analyze({...SQ.PROFILE,id:'counterexample-only-3s',durationMs:3000,samples:61},delayed);
 assert.equal(full.ok,true);assert.equal(short.ok,true);assert.equal(full.flowPass,false);assert.equal(short.flowPass,true);
 assert.equal(full.receivedSamples,401);assert.equal(full.sourceSlipMs,0);
});
for(const durationMs of [8000,12000])test(`three ${durationMs/1000}s windows still miss a per-transfer late stall that three 20s windows detect`,()=>{
 const delayed=i=>100+(i>=280?1000:0);
 const shortProfile={...SQ.PROFILE,id:`counterexample-only-${durationMs}ms`,durationMs,samples:durationMs/SQ.PROFILE.intervalMs+1};
 const full=Array.from({length:3},()=>analyze(SQ.PROFILE,delayed));
 const short=Array.from({length:3},()=>analyze(shortProfile,delayed));
 assert.ok(full.every(r=>r.ok&&r.receivedSamples===401&&r.sourceSlipMs===0&&r.flowPass===false));
 assert.ok(short.every(r=>r.ok&&r.receivedSamples===shortProfile.samples&&r.sourceSlipMs===0&&r.flowPass===true));
 assert.equal(full.filter((r,i)=>r.flowPass!==short[i].flowPass).length,3);
 assert.ok(3*durationMs<60000); // Faster, but not equivalent accuracy. Synthetic evidence only.
});
test('slow first response alone is not a stream-quality failure under the existing contract',()=>{
 const result=analyze(SQ.PROFILE,()=>4000);assert.equal(result.ok,true);assert.equal(result.flowPass,true);assert.equal(result.firstSampleMs,4000);
});
test('three successive complete observation windows already consume the entire 60s budget',()=>{
 assert.equal((SQ.PROFILE.samples-1)*SQ.PROFILE.intervalMs,20000);
 assert.equal(3*SQ.PROFILE.durationMs,60000);assert.equal(SQ.PROFILE.samples,401);
});
const {model}=require('../scripts/model-sse-schedule.cjs');
test('all 519 modeled observations retain their full workload; 256 slots still need at least three 20s waves',()=>{
 const serial=model({startupMs:250,cleanupMs:250});
 const overlap256=model({concurrency:256,serializeNode:false,startupMs:250,cleanupMs:250});
 for(const plan of [serial,overlap256]){
  assert.equal(plan.totalTasks,519);assert.equal(plan.totalSampleFrames,208119);
  assert.equal(new Set(plan.tasks.map(t=>`${t.node}:${t.round}`)).size,519);
  assert.ok(plan.tasks.every(t=>t.end-t.start===20000&&t.samples===401));
  assert.equal(plan.workloadFloorMs,60000);assert.equal(plan.modeledWallMs,60500);
 }
 assert.equal(serial.exposurePerNodeMs.min,60000);
 assert.equal(Math.ceil(519/2),260,'at least 260 slots needed for two ideal waves, even before transport overhead');
 assert.equal(model({concurrency:260,serializeNode:false}).workloadFloorMs,40000);
});
for(const concurrency of [256,260,519])test(`overlapping full 20s trials at ${concurrency} slots can still miss a time-local fault seen by serial trials`,()=>{
 const serial=model(),overlap=model({concurrency,serializeNode:false});
 const results=plan=>plan.tasks.filter(t=>t.node===0).map(t=>analyze(SQ.PROFILE,
  i=>100+(t.start+i*SQ.PROFILE.intervalMs>=45000?1000:0)));
 const slow=results(serial),fast=results(overlap);
 assert.ok([...slow,...fast].every(r=>r.ok&&r.receivedSamples===401&&r.sourceSlipMs===0));
 assert.equal(slow.every(r=>r.flowPass),false);
 assert.equal(fast.every(r=>r.flowPass),true);
 // A deterministic counterexample, not a measured production false-negative rate.
 assert.ok(overlap.exposurePerNodeMs.min<serial.exposurePerNodeMs.min);
});
