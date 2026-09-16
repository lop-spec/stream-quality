const {test}=require('node:test'),assert=require('node:assert/strict'),SQ=require('../stream-quality.js');
const analyze=(p,delay)=>{const a=SQ.analyzer(p);for(let i=0;i<p.samples;i++)a.push(Buffer.from(SQ.frame(i,i*p.intervalMs,p)),i*p.intervalMs+delay(i));a.push(Buffer.from(SQ.endFrame(p)),p.durationMs+delay(p.samples-1));return a.result()};
test('a 3s sample provably misses a late stall caught by the unchanged 20s profile',()=>{
 const delayed=i=>100+(i>=240?1000:0),full=analyze(SQ.PROFILE,delayed);
 const short=analyze({...SQ.PROFILE,id:'counterexample-only-3s',durationMs:3000,samples:61},delayed);
 assert.equal(full.ok,true);assert.equal(short.ok,true);assert.equal(full.flowPass,false);assert.equal(short.flowPass,true);
 assert.equal(full.receivedSamples,401);assert.equal(full.sourceSlipMs,0);
});
test('slow first response alone is not a stream-quality failure under the existing contract',()=>{
 const result=analyze(SQ.PROFILE,()=>4000);assert.equal(result.ok,true);assert.equal(result.flowPass,true);assert.equal(result.firstSampleMs,4000);
});
test('three successive complete observation windows already consume the entire 60s budget',()=>{
 assert.equal((SQ.PROFILE.samples-1)*SQ.PROFILE.intervalMs,20000);
 assert.equal(3*SQ.PROFILE.durationMs,60000);assert.equal(SQ.PROFILE.samples,401);
});
