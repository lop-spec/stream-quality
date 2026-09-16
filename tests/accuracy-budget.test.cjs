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
