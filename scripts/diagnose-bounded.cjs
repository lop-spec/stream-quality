'use strict';
const fs=require('node:fs'),path=require('node:path'),cp=require('node:child_process');
if(process.argv[2]==='idle'){
 const timer=setTimeout(()=>{console.error('Idle observer safety deadline');process.exitCode=1;process.disconnect()},110000);
 process.on('message',m=>{if(m==='stop'){clearTimeout(timer);process.disconnect()}});
}else{
 const root=path.resolve(__dirname,'..'),out=path.join(root,'.diagnostics'),observer=path.join(root,'tests/fixtures/loopback-observer.cjs');
 fs.mkdirSync(out); // Refuse to overwrite or mix an earlier diagnostic run.
 const env={...process.env,SQ_TRACE_DIR:out,SQ_TRACE_SYSTEM:'0'};
 const idle=cp.fork(__filename,['idle'],{cwd:root,env:{...env,SQ_TRACE_SYSTEM:'1'},execArgv:['--require',observer],windowsHide:true,stdio:['ignore','ignore','inherit','ipc']});
 const idleExit=new Promise(resolve=>{idle.once('exit',(code,signal)=>resolve({code,signal}));idle.once('error',e=>resolve({error:e.message}))});
 const started=Date.now();let timer,probe;
 console.log('Explicit diagnostic only: original failed CI remains failed; no retry acceptance, threshold changes or deployment.');
 (async()=>{
  probe=cp.spawn(process.execPath,['--cpu-prof',`--cpu-prof-dir=${out}`,'--require',observer,'--test','--test-name-pattern=173 real streams','tests/bounded.test.cjs'],{cwd:root,env,windowsHide:true,stdio:'inherit'});
  timer=setTimeout(()=>{console.error('Diagnostic safety deadline');probe.kill()},95000);
  const result=await new Promise(resolve=>{probe.once('exit',(code,signal)=>resolve({code,signal}));probe.once('error',e=>resolve({error:e.message}))});clearTimeout(timer);
  if(idle.connected)idle.send('stop');
  timer=setTimeout(()=>{console.error('Idle observer did not stop');idle.kill()},5000);
  const idleResult=await idleExit;clearTimeout(timer);
  fs.writeFileSync(path.join(out,'result.json'),JSON.stringify({diagnosticOnly:true,accepted:false,sha:process.env.GITHUB_SHA||null,wallMs:Date.now()-started,probePid:probe.pid,idlePid:idle.pid,result,idleResult},null,2),{flag:'wx'});
  if(result.code!==0||idleResult.code!==0)process.exitCode=1;
 })().catch(e=>{console.error(e.message);process.exitCode=1}).finally(()=>{clearTimeout(timer);if(idle.connected)idle.send('stop');if(probe&&probe.exitCode===null&&probe.signalCode===null)probe.kill()});
}
