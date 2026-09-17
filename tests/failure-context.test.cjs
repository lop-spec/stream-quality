const {test}=require('node:test'),assert=require('node:assert/strict'),http=require('node:http');
const SQ=require('../stream-quality.js'),{request,probe,run}=require('../stream-quality-runner.cjs');
async function source(t,handler){const s=http.createServer(handler);await new Promise(r=>s.listen(0,'127.0.0.1',r));t.after(()=>{s.closeAllConnections();s.close()});return `http://127.0.0.1:${s.address().port}`}
function manifest(res){res.setHeader('content-type','application/json');res.end(JSON.stringify({profile:SQ.PROFILE,location:'cf:TPE'}))}

test('HTTP failure preserves allowlisted response evidence, not cookies, auth or body',async t=>{
 const base=await source(t,(_req,res)=>{res.writeHead(429,{'x-stream-quality-location':'cf:NRT','retry-after':'60','cf-ray':'fixture-NRT','set-cookie':'PRIVATE_COOKIE','authorization':'PRIVATE_AUTH'});res.end('PRIVATE_BODY')});
 await assert.rejects(request(base,'/api/stream',{maxBytes:4096}),error=>{
  assert.equal(error.failureScope,'endpoint');assert.deepEqual(error.requestContext,{route:'/api/stream',httpStatus:429,responseLocation:'cf:NRT',retryAfter:'60',cfRay:'fixture-NRT'});
  assert.doesNotMatch(JSON.stringify(error),/PRIVATE_/);return true;
 });
});
test('probe keeps manifest and failed response locations separate',async t=>{
 const base=await source(t,(req,res)=>{if(req.url==='/api/manifest')return manifest(res);res.writeHead(429,{'x-stream-quality-location':'cf:NRT','retry-after':'60','cf-ray':'fixture-NRT'});res.end()});
 await assert.rejects(probe(base,{includeDownload:false,reuseConnections:true}),error=>{
  assert.equal(error.failureScope,'endpoint');assert.equal(error.requestContext.manifestLocation,'cf:TPE');assert.equal(error.requestContext.responseLocation,'cf:NRT');assert.equal(error.requestContext.httpStatus,429);return true;
 });
});
test('missing error-response location is not filled from the manifest; header evidence is bounded',async t=>{
 const base=await source(t,(req,res)=>{if(req.url==='/api/manifest')return manifest(res);res.writeHead(429,{'cf-ray':'x'.repeat(600)});res.end()});
 await assert.rejects(probe(base,{includeDownload:false}),error=>{assert.equal(error.requestContext.manifestLocation,'cf:TPE');assert.equal(error.requestContext.responseLocation,undefined);assert.equal(error.requestContext.cfRay.length,256);return true});
});
test('incomplete SSE retains the actual accepted response location without changing its measurement scope',async t=>{
 const base=await source(t,(req,res)=>{if(req.url==='/api/manifest')return manifest(res);res.writeHead(200,{'content-type':'text/event-stream','x-stream-quality-profile':SQ.PROFILE.id,'x-stream-quality-location':'cf:TPE','cf-ray':'fixture-TPE'});res.end(SQ.frame(0,0))});
 const value=await probe(base,{includeDownload:false});assert.equal(value.ok,false);assert.equal(value.failureScope,'measurement');assert.match(value.error,/terminal/);
 assert.deepEqual(value.requestContext,{route:'/api/stream',httpStatus:200,responseLocation:'cf:TPE',cfRay:'fixture-TPE',manifestLocation:'cf:TPE'});
});
test('missing manifest location does not turn an incomplete stream into a diagnostic exception',async t=>{
 const base=await source(t,(req,res)=>{if(req.url==='/api/manifest')return res.end(JSON.stringify({profile:SQ.PROFILE}));res.writeHead(200,{'content-type':'text/event-stream','x-stream-quality-profile':SQ.PROFILE.id});res.end(SQ.frame(0,0))});
 const value=await probe(base,{includeDownload:false});assert.equal(value.failureScope,'measurement');assert.match(value.error,/terminal/);assert.equal(value.requestContext.manifestLocation,undefined);assert.equal(value.requestContext.responseLocation,undefined);
});
test('round endpoint failure and unmeasured outcomes retain the same diagnostic context',async()=>{
 const context={route:'/api/stream',httpStatus:429,manifestLocation:'cf:TPE',cfRay:'fixture-TPE'};let calls=0;
 const result=await run({endpoint:'https://example.test',nodes:[{key:'a'},{key:'b'}],rounds:3,includeDownload:false,concurrency:1},{probeFn:async()=>{calls++;throw Object.assign(Error('HTTP 429'),{failureScope:'endpoint',requestContext:context})}});
 const attributed={...context,nodeKey:'a'};assert.equal(calls,1);assert.deepEqual(result.channelFailure.requestContext,attributed);assert.equal(result.outcomes.length,2);for(const {value}of result.outcomes){assert.equal(value.ok,false);assert.equal(value.failureScope,'endpoint');assert.deepEqual(value.requestContext,attributed)}
});
