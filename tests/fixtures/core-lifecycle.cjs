'use strict';
// Synthetic core only: no subscription, daily config, proxy forwarding or remote endpoint.
const fs=require('node:fs'),net=require('node:net'),path=require('node:path'),cp=require('node:child_process');
if(process.argv[2]==='synthetic-core'){
 const config=JSON.parse(fs.readFileSync(process.argv[3]));
 for(const inbound of config.inbounds)net.createServer(socket=>socket.end()).listen(inbound.listen_port,'127.0.0.1');
}else{
 const mode=process.argv[2],originalSpawn=cp.spawn,controller=new AbortController(),calls=[];let owned,configPath,core,cleanupMs,caught;
 const guard=setTimeout(()=>{console.error('Synthetic fixture safety deadline');owned?.kill('SIGKILL');controller.abort()},6000);guard.unref();
 cp.spawn=(command,args,options)=>{
  if(command!==process.execPath||args[0]!=='run'||args[1]!=='-c')throw Error('Unexpected synthetic core launch');
  configPath=args[2];owned=originalSpawn(process.execPath,[__filename,'synthetic-core',configPath],options);
  const kill=owned.kill.bind(owned);owned.kill=signal=>{calls.push(signal||'SIGTERM');if(mode==='force'&&!signal)return true;return kill(signal)};
  if(mode==='cancel')controller.abort();
  return owned;
 };
 const runner=require('../../stream-quality-runner.cjs');
 (async()=>{
  try{core=await runner.startCore({corePath:process.execPath,nodes:[{key:'fixture',tag:'fixture'}],config:{outbounds:[{type:'direct',tag:'fixture'}]}},controller.signal,()=>{});
   const before=performance.now();await core.cleanup();cleanupMs=performance.now()-before;core=undefined;
  }catch(e){caught=e.message;if(mode!=='cancel')throw e}
  // Force-kill remains asynchronous in the existing API; verify the owned fixture really stopped.
  if(owned&&owned.exitCode===null&&owned.signalCode===null)await new Promise(resolve=>owned.once('exit',resolve));
  await new Promise(setImmediate);
  console.log(JSON.stringify({mode,caught,cleanupMs,calls,ownedExited:owned.exitCode!==null||owned.signalCode!==null,
   remainingTimeouts:process.getActiveResourcesInfo().filter(x=>x==='Timeout').length,
   configRemoved:!fs.existsSync(configPath),directoryRemoved:!fs.existsSync(path.dirname(configPath))}));
 })().catch(e=>{console.error(e.stack);process.exitCode=1}).finally(async()=>{clearTimeout(guard);await core?.cleanup();if(owned&&owned.exitCode===null&&owned.signalCode===null)owned.kill('SIGKILL')});
}
