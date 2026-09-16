'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path');
const {spawn}=require('node:child_process');
async function lifecycle(t,mode){
 const child=spawn(process.execPath,[path.join(__dirname,'fixtures/core-lifecycle.cjs'),mode],{windowsHide:true,stdio:['ignore','pipe','pipe']});
 let stdout='',stderr='',terminalAt;
 child.stdout.on('data',b=>{stdout+=b;if(stdout.includes('\n'))terminalAt??=performance.now()});child.stderr.on('data',b=>stderr+=b);
 const exit=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal,at:performance.now()}))});
 t.after(async()=>{if(child.exitCode===null&&child.signalCode===null)child.kill();await exit});
 const result=await exit;
 assert.equal(result.code,0,stderr);assert.equal(stderr,'');assert.equal(result.signal,null);
 const data=JSON.parse(stdout);assert.equal(data.ownedExited,true);assert.equal(data.configRemoved,true);assert.equal(data.directoryRemoved,true);
 assert.equal(data.remainingTimeouts,0,'a completed owned-core shutdown must not leave the losing 2s timeout referenced');
 assert.ok(result.at-terminalAt<1500,`natural exit delayed ${Math.round(result.at-terminalAt)}ms after cleanup`);
 return data;
}
test('normal owned-core cleanup clears its losing timeout and exits naturally',{timeout:9000},async t=>{
 const data=await lifecycle(t,'normal');assert.deepEqual(data.calls,['SIGTERM']);assert.ok(data.cleanupMs<1500);
});
test('cancellation during core readiness also clears the shutdown timeout',{timeout:9000},async t=>{
 const data=await lifecycle(t,'cancel');assert.equal(data.caught,'cancelled');assert.deepEqual(data.calls,['SIGTERM']);
});
test('the 2s shutdown fallback still force-stops an unresponsive owned core',{timeout:9000},async t=>{
 const data=await lifecycle(t,'force');assert.deepEqual(data.calls,['SIGTERM','SIGKILL']);assert.ok(data.cleanupMs>=1800);
});
