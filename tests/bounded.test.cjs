const { test } = require('node:test');
const assert = require('node:assert/strict');
const { getEventListeners } = require('node:events');
const SQ = require('../stream-quality.js');
const runner = require('../stream-quality-runner.cjs');
const nodes = n => Array.from({length:n},(_,i)=>({key:`n${i}`,port:i}));
const good = () => ({ok:true,metricKind:'stream-quality-v1',measurement:'sse-only',profileKey:'same|sse-only',
 stream:{ok:true,flowPass:true,firstSampleMs:100,jitterMs:0,maxExtraGapMs:0,burstRatio:0}});

test('bounded lanes cover all nodes and preserve three successive observation rounds',async()=>{
 let active=0,peak=0;const counts=new Map(),done=[];
 const result=await runner.run({endpoint:'https://example.test',nodes:nodes(19),rounds:3,includeDownload:false,concurrency:4},{
  probeFn:async(_url,{port,nodeKey,includeDownload})=>{
   assert.equal(nodeKey,`n${port}`);assert.equal(includeDownload,false);const count=(counts.get(port)||0)+1;
   if(count>1)assert.ok([...counts.values()].every(v=>v>=count-1));
   counts.set(port,count);peak=Math.max(peak,++active);await new Promise(r=>setTimeout(r,2));active--;done.push(port);return good();
  }});
 assert.equal(peak,4);assert.equal(result.activity.maxActiveTotal,4);assert.equal(done.length,57);
 assert.equal(result.outcomes.length,19);assert.ok(result.outcomes.every(o=>o.value.verified && o.value.sampleCount===3));
 assert.ok(result.outcomes.every(o=>!('download' in o.value)));
});
test('cancellation reaches active workers, prevents queued starts, removes parent listener',async()=>{
 const c=new AbortController();let started=0,stopped=0;
 const result=await runner.run({endpoint:'https://example.test',nodes:nodes(17),rounds:3,includeDownload:false,concurrency:4},{signal:c.signal,
  probeFn:async(_url,{signal})=>{const ended=new Promise(r=>signal.addEventListener('abort',()=>{stopped++;r()},{once:true}));if(++started===4)c.abort();await ended;return good();}});
 assert.equal(started,4);assert.equal(stopped,4);assert.equal(result.cancelled,true);
 assert.equal(getEventListeners(c.signal,'abort').length,0);assert.equal(result.outcomes.length,17);
 assert.ok(result.outcomes.every(o=>!o.value.ok && o.value.failureScope==='cancelled'));
});
test('endpoint failure preserves completed active lanes and explicitly classifies every unmeasured lane',async()=>{
 let calls=0;const result=await runner.run({endpoint:'https://example.test',nodes:nodes(7),includeDownload:false,concurrency:2},{
  probeFn:async(_url,{port})=>{calls++;return port===0?{ok:false,failureScope:'endpoint',error:'HTTP 429'}:good()}});
 assert.equal(calls,2);assert.equal(result.outcomes.length,7);assert.equal(result.outcomes[1].value.ok,true);
 assert.equal(result.outcomes.filter(o=>o.value.failureScope==='endpoint').length,6);
});
test('defaults retain serial downloads; mixed profiles never aggregate; invalid concurrency is rejected',async()=>{
 let active=0,peak=0;await runner.run({endpoint:'https://example.test',nodes:nodes(3)},{probeFn:async(_url,{includeDownload})=>{
 assert.equal(includeDownload,true);peak=Math.max(peak,++active);await new Promise(r=>setTimeout(r,1));active--;return {...good(),measurement:undefined,download:{ok:true,mbps:10,endToEndMbps:10}};}});
 assert.equal(peak,1);
 assert.equal(runner.aggregate([good(),{...good(),measurement:'sse-and-download',download:{ok:true,mbps:10}}],2,'round').ok,false);
 for(const concurrency of [0,-1,257,1.5,'4'])await assert.rejects(runner.run({endpoint:'https://example.test',concurrency}),/concurrency/);
 assert.notEqual(SQ.profileKey('https://example.test','x'),SQ.profileKey('https://example.test','x',false));
});
test('client stalls invalidate results without blaming nodes',async()=>{
 const events=[];const result=await runner.run({endpoint:'https://example.test',includeDownload:false},{emit:e=>events.push(e),probeFn:async()=>{
  await new Promise(r=>setTimeout(r,40));const until=performance.now()+160;while(performance.now()<until){}await new Promise(r=>setTimeout(r,40));return good();}});
 assert.equal(result.outcomes[0].value.failureScope,'measurement');assert.ok(events.some(e=>e.type==='log'&&/local event loop stalled/.test(e.message)));
});
test('173 real streams retain all 401 frames and thresholds, single-round wall clock <=60s', {timeout:70000},async t=>{
 const {fork}=require('node:child_process'),path=require('node:path');
 const source=fork(path.join(__dirname,'fixtures/source-process.cjs'),[],{windowsHide:true,stdio:['ignore','ignore','inherit','ipc']});
 t.after(async()=>{source.send('stop');await new Promise((resolve,reject)=>{const timeout=setTimeout(()=>{source.kill();reject(Error('owned source did not exit'))},2500);source.once('exit',()=>{clearTimeout(timeout);resolve()})})});
 const {port}=await new Promise((resolve,reject)=>{const timeout=setTimeout(()=>reject(Error('source startup timeout')),5000);source.once('message',m=>{clearTimeout(timeout);resolve(m)})});
 const concurrency=Number(process.env.SQ_ACCEPTANCE_CONCURRENCY||173);
 const c=new AbortController(),timer=setTimeout(()=>c.abort(),65000),start=performance.now();let completed=0;
 const progress=setInterval(()=>console.log(`controlled independent-source SSE: ${Math.round(performance.now()-start)}ms, completed=${completed}/173, concurrency=${concurrency}`),5000);
 let result;try{result=await runner.run({endpoint:`http://127.0.0.1:${port}`,nodes:nodes(173).map(({key})=>({key})),includeDownload:false,concurrency},{signal:c.signal,emit:e=>{if(e.phase==='sample-done')completed=e.completed}})}
 finally{clearTimeout(timer);clearInterval(progress)}
 const wallMs=performance.now()-start;
 const stats=await new Promise((resolve,reject)=>{const timeout=setTimeout(()=>reject(Error('source stats timeout')),2000);source.once('message',m=>{clearTimeout(timeout);resolve(m)});source.send('stats')});
 const {peak,routes}=stats;
 console.log(JSON.stringify({fixture:'independent-source loopback, not subscription evidence',wallMs,peak,valid:result.outcomes.filter(o=>SQ.isResult(o.value)).length,activity:result.activity,routes}));
 assert.equal(result.cancelled,false);assert.equal(peak,concurrency);assert.equal(routes['/api/download'],undefined);
 assert.equal(routes['/api/manifest'],173);assert.equal(routes['/api/stream'],173);assert.equal(result.outcomes.length,173);
 for(const {value} of result.outcomes){assert.equal(SQ.isResult(value),true,JSON.stringify(value));assert.equal(value.stream.receivedSamples,401);assert.equal(value.stream.flowPass,true);assert.ok(value.stream.sourceSlipMs<=250);assert.equal(value.verified,false)}
 assert.ok(wallMs>=19900);assert.ok(wallMs<=60000,`actual wall clock ${wallMs} exceeds 60s`);
});
test('source has bounded independent SSE and download capacity and releases cancellation slots',async()=>{
 const {handle,LIMITS}=await import('../worker.mjs');const streams=[],downloads=[];const req=kind=>new Request(`https://x/api/${kind}`,{headers:{'cf-connecting-ip':'capacity-test'}});
 try{
  for(let i=0;i<LIMITS.download.concurrent;i++){const r=handle(req('download'));assert.equal(r.status,200);downloads.push(r)}
  assert.equal(handle(req('download')).status,429);
  for(let i=0;i<LIMITS.stream.concurrent;i++){const r=handle(req('stream'));assert.equal(r.status,200);streams.push(r)}
  assert.ok(streams.length>=173);assert.equal(handle(req('stream')).status,429);
 }finally{await Promise.all([...streams,...downloads].map(r=>r.body.cancel()))}
 const r=handle(req('stream'));assert.equal(r.status,200);await r.body.cancel();
});
